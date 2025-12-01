"""Google Pub/Sub producer for publishing messages to the queue.

Provides a clean interface for publishing extraction jobs.
"""

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from google.cloud import pubsub_v1
from pydantic import BaseModel

from .base import BaseProducer, PublishResult
from .pubsub_config import PubSubConfig, PubSubTopicSettings

logger = logging.getLogger(__name__)


class PubSubProducer(BaseProducer):
    """
    Google Pub/Sub producer for publishing messages.

    Usage:
        producer = PubSubProducer(pubsub_config)
        await producer.connect()
        result = await producer.publish_extraction("extraction-123")
    """

    # Default configuration
    DEFAULT_PUBLISH_TIMEOUT = 30.0  # seconds
    DEFAULT_MAX_CONCURRENT_PUBLISHES = 10
    DEFAULT_SHUTDOWN_TIMEOUT = 10.0  # seconds

    def __init__(
        self,
        pubsub_config: PubSubConfig,
        topic_settings: PubSubTopicSettings,
        publisher: pubsub_v1.PublisherClient | None = None,
        publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT,
        max_concurrent_publishes: int = DEFAULT_MAX_CONCURRENT_PUBLISHES,
        shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
        embedding_topic_name: str | None = None,
    ):
        self.pubsub_config = pubsub_config
        self.topic_settings = topic_settings
        self._publisher = publisher
        self._owns_publisher = publisher is None
        self._publish_timeout = publish_timeout
        self._shutdown_timeout = shutdown_timeout
        self._semaphore: asyncio.Semaphore | None = None
        self._max_concurrent_publishes = max_concurrent_publishes
        self._executor: ThreadPoolExecutor | None = None
        # Embedding topic: use explicit name or default to {main_topic}-embedding
        self._embedding_topic_name = (
            embedding_topic_name or f"{topic_settings.name}-embedding"
        )

    @property
    def is_connected(self) -> bool:
        """Check if Pub/Sub client is connected."""
        return self._publisher is not None

    @property
    def topic_path(self) -> str:
        """Get full topic path."""
        project = self.pubsub_config.project_id
        return f"projects/{project}/topics/{self.topic_settings.name}"

    async def connect(self) -> None:
        """Connect to Pub/Sub and initialize resources."""
        if self._publisher is None:
            self._publisher = pubsub_v1.PublisherClient()
            logger.info("Producer connected to Pub/Sub")
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrent_publishes)
        if self._executor is None and self._owns_publisher:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_concurrent_publishes,
                thread_name_prefix="pubsub_publish",
            )

    async def close(self) -> None:
        """Close Pub/Sub connection gracefully, flushing pending messages."""
        if self._publisher and self._owns_publisher:
            try:
                # stop() flushes pending messages and shuts down the batch scheduler
                # Run in executor since stop() is blocking
                loop = asyncio.get_running_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(None, self._publisher.stop),
                    timeout=self._shutdown_timeout,
                )
                logger.info("Producer flushed pending messages and stopped")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Pub/Sub publisher stop timed out after "
                    f"{self._shutdown_timeout}s, some messages may be lost"
                )
            except Exception as e:
                logger.error(f"Error during Pub/Sub publisher shutdown: {e}")
            finally:
                self._publisher = None

        # Shutdown the executor
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None

        self._semaphore = None
        logger.info("Producer disconnected from Pub/Sub")

    async def publish(
        self,
        subject: str,
        payload: dict[str, Any] | BaseModel,
        headers: dict[str, str] | None = None,
        msg_id: str | None = None,
    ) -> PublishResult:
        """
        Publish a message to the specified topic.

        Args:
            subject: Topic name (will be used to construct topic path)
            payload: Message payload (dict or Pydantic model)
            headers: Optional message attributes
            msg_id: Optional message ID for deduplication (stored as attribute)

        Returns:
            PublishResult with publish outcome
        """
        if not self.is_connected:
            return PublishResult(success=False, error="Pub/Sub client not connected")

        if self._semaphore is None:
            return PublishResult(
                success=False, error="Producer not initialized. Call connect() first."
            )

        # Acquire semaphore to limit concurrent publishes
        async with self._semaphore:
            try:
                # Serialize payload
                if isinstance(payload, BaseModel):
                    data = payload.model_dump_json().encode()
                else:
                    data = json.dumps(payload).encode()

                # Construct topic path
                topic_path = (
                    f"projects/{self.pubsub_config.project_id}/topics/{subject}"
                )

                # Prepare attributes
                attributes = headers or {}
                if msg_id:
                    attributes["dedup_id"] = msg_id

                # Publish message (non-blocking, returns a future)
                future = self._publisher.publish(
                    topic_path,
                    data,
                    **attributes,
                )

                # Wait for result with timeout using dedicated executor
                loop = asyncio.get_running_loop()
                executor = self._executor  # Use dedicated executor if available
                message_id = await asyncio.wait_for(
                    loop.run_in_executor(executor, future.result),
                    timeout=self._publish_timeout,
                )

                return PublishResult(
                    success=True,
                    stream=topic_path,
                    message_id=message_id,
                )

            except asyncio.TimeoutError:
                logger.error(
                    f"Publish to {subject} timed out after {self._publish_timeout}s"
                )
                return PublishResult(
                    success=False,
                    error=f"Publish timed out after {self._publish_timeout}s",
                )
            except Exception as e:
                logger.error(f"Failed to publish to {subject}: {e}")
                return PublishResult(success=False, error=str(e))

    async def publish_extraction(
        self,
        extraction_id: str,
        priority: int = 0,
    ) -> PublishResult:
        """
        Publish an extraction job to the queue.

        Uses extraction_id as message attribute for deduplication tracking.

        Args:
            extraction_id: ID of the extraction to process
            priority: Optional priority (higher = more important)

        Returns:
            PublishResult with publish outcome
        """
        payload = {
            "extraction_id": extraction_id,
            "priority": priority,
        }

        # Use extraction_id as dedup_id attribute
        headers = {"extraction_id": extraction_id}

        result = await self.publish(
            subject=self.topic_settings.name,
            payload=payload,
            headers=headers,
            msg_id=extraction_id,
        )

        if result.success:
            logger.debug(
                f"Published extraction {extraction_id} "
                f"to topic={self.topic_settings.name}, "
                f"msg_id={result.message_id}"
            )
        else:
            logger.error(
                f"Failed to publish extraction {extraction_id}: {result.error}"
            )

        return result

    async def publish_embedding(
        self,
        extraction_id: str,
        force: bool = False,
    ) -> PublishResult:
        """
        Publish an embedding generation job to the queue.

        Uses extraction_id as message attribute for deduplication tracking.

        Args:
            extraction_id: ID of the extraction to generate embeddings for
            force: Force regeneration even if embedding already exists

        Returns:
            PublishResult with publish outcome
        """
        payload = {
            "extraction_id": extraction_id,
            "force": force,
        }

        # Use extraction_id with suffix as dedup_id attribute
        msg_id = f"emb-{extraction_id}-{force}"
        headers = {
            "extraction_id": extraction_id,
            "job_type": "embedding",
        }

        # Use configured embedding topic
        embedding_topic = self._embedding_topic_name

        result = await self.publish(
            subject=embedding_topic,
            payload=payload,
            headers=headers,
            msg_id=msg_id,
        )

        if result.success:
            logger.debug(
                f"Published embedding {extraction_id} "
                f"to topic={embedding_topic}, "
                f"msg_id={result.message_id}"
            )
        else:
            logger.error(
                f"Failed to publish embedding {extraction_id}: {result.error}"
            )

        return result


class PubSubProducerFactory:
    """Factory for creating PubSubProducer instances."""

    DEFAULT_SHUTDOWN_TIMEOUT = 10.0  # seconds

    def __init__(
        self,
        pubsub_config: PubSubConfig,
        topic_settings: PubSubTopicSettings,
        shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
    ):
        self.pubsub_config = pubsub_config
        self.topic_settings = topic_settings
        self._publisher: pubsub_v1.PublisherClient | None = None
        self._shutdown_timeout = shutdown_timeout

    async def connect(self) -> None:
        """Connect to Pub/Sub."""
        self._publisher = pubsub_v1.PublisherClient()
        logger.info("Producer factory connected to Pub/Sub")

    async def close(self) -> None:
        """Close Pub/Sub connection gracefully, flushing pending messages."""
        if self._publisher:
            try:
                # stop() flushes pending messages and shuts down the batch scheduler
                loop = asyncio.get_running_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(None, self._publisher.stop),
                    timeout=self._shutdown_timeout,
                )
                logger.info("Producer factory flushed pending messages and stopped")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Pub/Sub publisher stop timed out after "
                    f"{self._shutdown_timeout}s, some messages may be lost"
                )
            except Exception as e:
                logger.error(f"Error during Pub/Sub publisher shutdown: {e}")
            finally:
                self._publisher = None
            logger.info("Producer factory disconnected from Pub/Sub")

    def create_producer(self) -> PubSubProducer:
        """Create a new producer instance."""
        if not self._publisher:
            raise RuntimeError("Not connected. Call connect() first.")
        return PubSubProducer(
            self.pubsub_config,
            self.topic_settings,
            self._publisher,
        )

    async def __aenter__(self) -> "PubSubProducerFactory":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
