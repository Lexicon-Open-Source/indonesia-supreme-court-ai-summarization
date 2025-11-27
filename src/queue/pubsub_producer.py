"""Google Pub/Sub producer for publishing messages to the queue.

Provides a clean interface for publishing extraction jobs.
"""

import json
import logging
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

    def __init__(
        self,
        pubsub_config: PubSubConfig,
        topic_settings: PubSubTopicSettings,
        publisher: pubsub_v1.PublisherClient | None = None,
    ):
        self.pubsub_config = pubsub_config
        self.topic_settings = topic_settings
        self._publisher = publisher
        self._owns_publisher = publisher is None

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
        """Connect to Pub/Sub."""
        if self._publisher is None:
            self._publisher = pubsub_v1.PublisherClient()
            logger.info("Producer connected to Pub/Sub")

    async def close(self) -> None:
        """Close Pub/Sub connection."""
        if self._publisher and self._owns_publisher:
            self._publisher.transport.close()
            self._publisher = None
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

        try:
            # Serialize payload
            if isinstance(payload, BaseModel):
                data = payload.model_dump_json().encode()
            else:
                data = json.dumps(payload).encode()

            # Construct topic path
            topic_path = f"projects/{self.pubsub_config.project_id}/topics/{subject}"

            # Prepare attributes
            attributes = headers or {}
            if msg_id:
                attributes["dedup_id"] = msg_id

            # Publish message
            import asyncio

            future = self._publisher.publish(
                topic_path,
                data,
                **attributes,
            )

            # Wait for result
            message_id = await asyncio.get_event_loop().run_in_executor(
                None, future.result
            )

            return PublishResult(
                success=True,
                stream=topic_path,
                message_id=message_id,
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


class PubSubProducerFactory:
    """Factory for creating PubSubProducer instances."""

    def __init__(
        self,
        pubsub_config: PubSubConfig,
        topic_settings: PubSubTopicSettings,
    ):
        self.pubsub_config = pubsub_config
        self.topic_settings = topic_settings
        self._publisher: pubsub_v1.PublisherClient | None = None

    async def connect(self) -> None:
        """Connect to Pub/Sub."""
        self._publisher = pubsub_v1.PublisherClient()
        logger.info("Producer factory connected to Pub/Sub")

    async def close(self) -> None:
        """Close Pub/Sub connection."""
        if self._publisher:
            self._publisher.transport.close()
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
