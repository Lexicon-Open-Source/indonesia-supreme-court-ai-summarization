"""Google Pub/Sub consumer implementation.

Provides reliable message consumption with:
- Proper ACK/NACK handling based on error types
- Graceful shutdown with in-flight message completion
- Automatic ack deadline extension for long-running tasks
- Dead letter queue routing for failed messages
- Health monitoring and metrics
"""

import asyncio
import functools
import logging
from datetime import datetime
from typing import Any

from google.cloud import pubsub_v1
from google.pubsub_v1 import DeadLetterPolicy, Subscription

from .base import BaseConsumer, ConsumerMetrics, WorkerMetrics
from .errors import PermanentError, RetriableError, SkipMessageError
from .handler import MessageContext, MessageHandler
from .pubsub_config import (
    PubSubConfig,
    PubSubDeadLetterSettings,
    PubSubSubscriptionSettings,
    PubSubTopicSettings,
    PubSubWorkerSettings,
)

logger = logging.getLogger(__name__)


class PubSubConsumer(BaseConsumer):
    """
    Google Pub/Sub consumer with robust error handling and graceful shutdown.

    Usage:
        async with PubSubConsumer(pubsub_config, handler) as consumer:
            await consumer.start()
            # Consumer runs until shutdown signal
    """

    def __init__(
        self,
        pubsub_config: PubSubConfig,
        handler: MessageHandler,
        topic_settings: PubSubTopicSettings,
        subscription_settings: PubSubSubscriptionSettings,
        dead_letter_settings: PubSubDeadLetterSettings | None = None,
        worker_settings: PubSubWorkerSettings | None = None,
    ):
        self.pubsub_config = pubsub_config
        self.handler = handler
        self.topic_settings = topic_settings
        self.subscription_settings = subscription_settings
        self.dead_letter_settings = dead_letter_settings
        self.worker_settings = worker_settings or PubSubWorkerSettings()

        self._publisher: pubsub_v1.PublisherClient | None = None
        self._subscriber: pubsub_v1.SubscriberClient | None = None
        self._streaming_pull_future: asyncio.Future | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._metrics = ConsumerMetrics()
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Check if Pub/Sub client is connected."""
        return self._connected and self._subscriber is not None

    @property
    def metrics(self) -> ConsumerMetrics:
        """Get consumer metrics."""
        return self._metrics

    @property
    def topic_path(self) -> str:
        """Get full topic path."""
        project = self.pubsub_config.project_id
        return f"projects/{project}/topics/{self.topic_settings.name}"

    @property
    def subscription_path(self) -> str:
        """Get full subscription path."""
        project = self.pubsub_config.project_id
        sub_name = self.subscription_settings.name
        return f"projects/{project}/subscriptions/{sub_name}"

    @property
    def dlq_topic_path(self) -> str | None:
        """Get dead letter topic path."""
        if not self.dead_letter_settings:
            return None
        project = self.pubsub_config.project_id
        dlq_name = self.dead_letter_settings.topic_name
        return f"projects/{project}/topics/{dlq_name}"

    async def connect(self) -> None:
        """
        Connect to Pub/Sub and initialize resources.

        Sets up:
        1. Publisher and Subscriber clients
        2. Topic (creates if not exists)
        3. Subscription (creates if not exists)
        4. Dead letter topic and subscription (if configured)
        """
        logger.info(f"Connecting to Pub/Sub project {self.pubsub_config.project_id}")

        # Initialize clients
        self._publisher = pubsub_v1.PublisherClient()
        self._subscriber = pubsub_v1.SubscriberClient()

        # Ensure topics exist
        await self._ensure_topic(self.topic_path)
        if self.dlq_topic_path:
            await self._ensure_topic(self.dlq_topic_path)

        # Ensure subscriptions exist
        await self._ensure_subscription()
        if self.dead_letter_settings:
            await self._ensure_dlq_subscription()

        self._connected = True
        logger.info("Pub/Sub consumer initialized successfully")

    async def _ensure_topic(self, topic_path: str) -> None:
        """Ensure the Pub/Sub topic exists."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._publisher.get_topic, request={"topic": topic_path}
                ),
            )
            logger.debug(f"Topic {topic_path} exists")
        except Exception as e:
            if "NOT_FOUND" in str(e):
                await loop.run_in_executor(
                    None,
                    functools.partial(
                        self._publisher.create_topic, request={"name": topic_path}
                    ),
                )
                logger.info(f"Created topic {topic_path}")
            else:
                raise

    async def _ensure_subscription(self) -> None:
        """Ensure the main subscription exists with correct config."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._subscriber.get_subscription,
                    request={"subscription": self.subscription_path},
                ),
            )
            logger.debug(f"Subscription {self.subscription_path} exists")
        except Exception as e:
            if "NOT_FOUND" in str(e):
                # Build subscription request
                request_kwargs = {
                    "name": self.subscription_path,
                    "topic": self.topic_path,
                    "ack_deadline_seconds": (
                        self.subscription_settings.ack_deadline_seconds
                    ),
                    "retry_policy": pubsub_v1.types.RetryPolicy(
                        minimum_backoff={"seconds": 10},
                        maximum_backoff={"seconds": 600},
                    ),
                }

                # Add dead letter policy if configured
                if self.dead_letter_settings and self.dlq_topic_path:
                    dead_letter_policy = DeadLetterPolicy(
                        dead_letter_topic=self.dlq_topic_path,
                        max_delivery_attempts=(
                            self.subscription_settings.max_delivery_attempts
                        ),
                    )
                    request_kwargs["dead_letter_policy"] = dead_letter_policy

                request = Subscription(**request_kwargs)
                await loop.run_in_executor(
                    None,
                    functools.partial(
                        self._subscriber.create_subscription, request=request
                    ),
                )
                logger.info(f"Created subscription {self.subscription_path}")
            else:
                raise

    async def _ensure_dlq_subscription(self) -> None:
        """Ensure the dead letter subscription exists."""
        loop = asyncio.get_running_loop()
        dlq_subscription_path = (
            f"projects/{self.pubsub_config.project_id}/subscriptions/"
            f"{self.dead_letter_settings.subscription_name}"
        )
        try:
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._subscriber.get_subscription,
                    request={"subscription": dlq_subscription_path},
                ),
            )
            logger.debug(f"DLQ subscription {dlq_subscription_path} exists")
        except Exception as e:
            if "NOT_FOUND" in str(e):
                request = Subscription(
                    name=dlq_subscription_path,
                    topic=self.dlq_topic_path,
                    ack_deadline_seconds=(
                        self.dead_letter_settings.ack_deadline_seconds
                    ),
                )
                await loop.run_in_executor(
                    None,
                    functools.partial(
                        self._subscriber.create_subscription, request=request
                    ),
                )
                logger.info(f"Created DLQ subscription {dlq_subscription_path}")
            else:
                raise

    async def start(self) -> None:
        """
        Start consumer workers.

        Creates worker tasks that will process messages until shutdown.
        """
        if not self.is_connected:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        logger.info(f"Starting {self.worker_settings.num_workers} consumer workers")

        # Start the streaming pull in a separate task
        pull_task = asyncio.create_task(
            self._streaming_pull(),
            name="pubsub-streaming-pull",
        )
        self._worker_tasks.append(pull_task)

        # Start worker tasks
        for i in range(self.worker_settings.num_workers):
            worker_id = f"worker-{i + 1}"
            self._metrics.workers[worker_id] = WorkerMetrics()
            task = asyncio.create_task(
                self._run_worker(worker_id),
                name=f"pubsub-{worker_id}",
            )
            self._worker_tasks.append(task)

        logger.info("Consumer workers started")

    async def _streaming_pull(self) -> None:
        """Pull messages from Pub/Sub and queue them for processing."""
        logger.info("Starting streaming pull")

        while not self._shutdown_event.is_set():
            try:
                # Use synchronous pull in async context
                response = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._subscriber.pull(
                        request={
                            "subscription": self.subscription_path,
                            "max_messages": self.subscription_settings.max_messages,
                        },
                        timeout=30.0,
                    ),
                )

                for received_message in response.received_messages:
                    if self._shutdown_event.is_set():
                        # NACK messages received during shutdown
                        # Capture ack_id by value to avoid closure race condition
                        ack_id = received_message.ack_id
                        await asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda aid=ack_id: self._subscriber.modify_ack_deadline(
                                request={
                                    "subscription": self.subscription_path,
                                    "ack_ids": [aid],
                                    "ack_deadline_seconds": 0,  # Immediate redelivery
                                }
                            ),
                        )
                        continue

                    await self._message_queue.put(received_message)

            except Exception as e:
                if "DEADLINE_EXCEEDED" in str(e):
                    # Normal timeout when no messages
                    continue
                logger.error(f"Error in streaming pull: {e}")
                await asyncio.sleep(2)

        logger.info("Streaming pull stopped")

    async def _run_worker(self, worker_id: str) -> None:
        """
        Main worker loop for processing messages.

        Args:
            worker_id: Unique identifier for this worker
        """
        logger.info(f"{worker_id}: Starting")
        metrics = self._metrics.workers[worker_id]

        while not self._shutdown_event.is_set():
            try:
                # Get message from queue with timeout
                try:
                    received_message = await asyncio.wait_for(
                        self._message_queue.get(),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    continue

                await self._process_message(worker_id, received_message, metrics)

            except asyncio.CancelledError:
                logger.info(f"{worker_id}: Cancelled")
                raise
            except Exception as e:
                logger.exception(f"{worker_id}: Error in worker loop: {e}")
                await asyncio.sleep(2)

        logger.info(f"{worker_id}: Stopped")

    async def _process_message(
        self,
        worker_id: str,
        received_message: Any,
        metrics: WorkerMetrics,
    ) -> None:
        """
        Process a single message with proper ACK/NACK handling.

        Args:
            worker_id: Worker identifier
            received_message: Pub/Sub received message
            metrics: Worker metrics to update
        """
        message = received_message.message
        ack_id = received_message.ack_id

        # Extract message context
        context = self._extract_context(message, received_message)

        logger.info(
            f"{worker_id}: Processing message {context.message_id} "
            f"(delivery {context.delivery_count})"
        )

        metrics.messages_processed += 1
        metrics.last_message_at = datetime.utcnow()

        # Start ack deadline extension task
        extension_task = asyncio.create_task(
            self._extend_ack_deadline(worker_id, ack_id, context.message_id)
        )

        try:
            result = await self.handler.handle(message.data, context)

            if result.metadata and result.metadata.get("skipped"):
                metrics.messages_skipped += 1
            else:
                metrics.messages_succeeded += 1

            metrics.total_processing_time += result.duration_seconds

            # ACK the message
            await self._ack_message(ack_id)

            logger.info(
                f"{worker_id}: Completed message {context.message_id} "
                f"in {result.duration_seconds:.2f}s"
            )

        except SkipMessageError as e:
            metrics.messages_skipped += 1
            await self._ack_message(ack_id)
            logger.info(f"{worker_id}: Skipped message {context.message_id}: {e}")

        except RetriableError as e:
            logger.warning(
                f"{worker_id}: Retriable error for {context.message_id}: {e}"
            )
            max_deliver = self.subscription_settings.max_delivery_attempts
            if context.delivery_count >= max_deliver:
                logger.error(
                    f"{worker_id}: Message {context.message_id} exceeded "
                    f"max deliveries ({max_deliver}), will be sent to DLQ by Pub/Sub"
                )
                # NACK to let Pub/Sub handle dead letter
                await self._nack_message(ack_id)
                metrics.messages_failed += 1
            else:
                # NACK for redelivery
                await self._nack_message(ack_id)
                metrics.messages_retried += 1
                logger.info(
                    f"{worker_id}: Message {context.message_id} will be redelivered "
                    f"(attempt {context.delivery_count}/{max_deliver})"
                )

        except PermanentError as e:
            logger.error(f"{worker_id}: Permanent error for {context.message_id}: {e}")
            # For permanent errors, we could either:
            # 1. ACK and manually send to DLQ
            # 2. NACK and let Pub/Sub exhaust retries
            # We'll NACK to let Pub/Sub handle it consistently
            await self._nack_message(ack_id)
            metrics.messages_failed += 1

        except Exception as e:
            logger.exception(
                f"{worker_id}: Unexpected error for {context.message_id}: {e}"
            )
            await self._nack_message(ack_id)
            max_attempts = self.subscription_settings.max_delivery_attempts
            if context.delivery_count >= max_attempts:
                metrics.messages_failed += 1
            else:
                metrics.messages_retried += 1

        finally:
            # Always stop the extension task
            extension_task.cancel()
            try:
                await extension_task
            except asyncio.CancelledError:
                pass

    async def _extend_ack_deadline(
        self, worker_id: str, ack_id: str, message_id: str
    ) -> None:
        """
        Periodically extend the ack deadline to keep message alive.

        Similar to NATS heartbeat - prevents message redelivery during
        legitimate processing.
        """
        interval = self.worker_settings.ack_extension_interval
        extension = self.worker_settings.ack_extension_seconds

        logger.debug(
            f"{worker_id}: Starting ack deadline extension for {message_id} "
            f"(interval={interval}s, extension={extension}s)"
        )

        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self._subscriber.modify_ack_deadline(
                            request={
                                "subscription": self.subscription_path,
                                "ack_ids": [ack_id],
                                "ack_deadline_seconds": extension,
                            }
                        ),
                    )
                    logger.debug(f"{worker_id}: Extended ack deadline for {message_id}")
                except Exception as e:
                    logger.warning(
                        f"{worker_id}: Failed to extend ack deadline "
                        f"for {message_id}: {e}"
                    )
        except asyncio.CancelledError:
            logger.debug(
                f"{worker_id}: Ack deadline extension stopped for {message_id}"
            )
            raise

    def _extract_context(self, message: Any, received_message: Any) -> MessageContext:
        """Extract message context from Pub/Sub message."""
        # Get delivery count from attributes
        delivery_count = 1
        if message.attributes:
            # Pub/Sub tracks delivery attempts internally
            # We can get it from the received message's delivery_attempt
            try:
                delivery_count = received_message.delivery_attempt or 1
            except AttributeError:
                delivery_count = 1

        # Get timestamp
        try:
            timestamp = message.publish_time
        except Exception:
            timestamp = datetime.utcnow()

        # Extract attributes as headers
        headers = {}
        if message.attributes:
            for key, value in message.attributes.items():
                headers[key] = value

        return MessageContext(
            message_id=message.message_id,
            subject=self.topic_settings.name,
            stream_seq=0,  # Pub/Sub doesn't have stream sequences
            delivery_count=delivery_count,
            timestamp=timestamp,
            headers=headers,
        )

    async def _ack_message(self, ack_id: str) -> None:
        """Acknowledge a message."""
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._subscriber.acknowledge(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [ack_id],
                }
            ),
        )

    async def _nack_message(self, ack_id: str) -> None:
        """Negative acknowledge a message (trigger redelivery)."""
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._subscriber.modify_ack_deadline(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [ack_id],
                    "ack_deadline_seconds": 0,  # Immediate redelivery
                }
            ),
        )

    async def shutdown(self) -> None:
        """
        Gracefully shutdown the consumer.

        1. Signal workers to stop accepting new messages
        2. Wait for in-flight messages to complete (with timeout)
        3. Cancel remaining tasks
        4. Close Pub/Sub clients
        """
        logger.info("Initiating consumer shutdown")
        self._shutdown_event.set()

        if self._worker_tasks:
            # Wait for workers to finish with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._worker_tasks, return_exceptions=True),
                    timeout=self.worker_settings.shutdown_timeout,
                )
                logger.info("All workers completed gracefully")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Shutdown timeout ({self.worker_settings.shutdown_timeout}s) "
                    "exceeded, cancelling workers"
                )
                for task in self._worker_tasks:
                    task.cancel()
                await asyncio.gather(*self._worker_tasks, return_exceptions=True)

        # Close clients
        if self._publisher:
            self._publisher.transport.close()
        if self._subscriber:
            self._subscriber.close()

        self._connected = False
        logger.info("Consumer shutdown complete")

    async def get_queue_stats(self) -> dict[str, Any]:
        """Get current queue statistics."""
        if not self._subscriber:
            return {"error": "Not connected"}

        try:
            # Get subscription details (run in executor to avoid blocking)
            loop = asyncio.get_running_loop()
            subscription = await loop.run_in_executor(
                None,
                functools.partial(
                    self._subscriber.get_subscription,
                    request={"subscription": self.subscription_path},
                ),
            )

            return {
                "subscription": {
                    "name": subscription.name,
                    "topic": subscription.topic,
                    "ack_deadline_seconds": subscription.ack_deadline_seconds,
                },
                "workers": self._metrics.to_dict(),
            }
        except Exception as e:
            return {"error": str(e)}

    async def reset_consumer(self) -> dict[str, Any]:
        """
        Reset the consumer by seeking to end of subscription.

        Use when messages are stuck or need to skip old messages.
        """
        if not self._subscriber:
            return {"error": "Not connected"}

        try:
            # Seek to the end to skip all unacked messages
            # Note: Requires "enable_message_ordering" = false

            from google.protobuf import timestamp_pb2

            now = timestamp_pb2.Timestamp()
            now.FromDatetime(datetime.utcnow())

            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._subscriber.seek(
                    request={
                        "subscription": self.subscription_path,
                        "time": now,
                    }
                ),
            )

            logger.info(f"Reset subscription {self.subscription_path}")
            return {
                "success": True,
                "message": "Subscription seeked to current time, old messages skipped",
            }
        except Exception as e:
            logger.error(f"Failed to reset consumer: {e}")
            return {"error": str(e)}
