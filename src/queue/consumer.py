"""NATS JetStream consumer implementation.

Provides reliable message consumption with:
- Proper ACK/NACK handling based on error types
- Graceful shutdown with in-flight message completion
- Automatic reconnection with exponential backoff
- Dead letter queue routing for failed messages
- Health monitoring and metrics
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig
from nats.js.errors import NotFoundError

from .config import (
    ConsumerSettings,
    DeadLetterSettings,
    NatsConfig,
    QueueSubject,
    StreamSettings,
    WorkerSettings,
)
from .errors import PermanentError, RetriableError, SkipMessageError
from .handler import MessageContext, MessageHandler

logger = logging.getLogger(__name__)


class WorkerState(str, Enum):
    """Worker lifecycle states."""

    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"  # Completing in-flight work before shutdown
    STOPPED = "stopped"


@dataclass
class WorkerMetrics:
    """Metrics for a single worker instance."""

    messages_processed: int = 0
    messages_succeeded: int = 0
    messages_failed: int = 0
    messages_skipped: int = 0
    messages_retried: int = 0
    total_processing_time: float = 0.0
    last_message_at: datetime | None = None


@dataclass
class ConsumerMetrics:
    """Aggregate metrics across all workers."""

    workers: dict[str, WorkerMetrics] = field(default_factory=dict)

    @property
    def total_processed(self) -> int:
        return sum(w.messages_processed for w in self.workers.values())

    @property
    def total_succeeded(self) -> int:
        return sum(w.messages_succeeded for w in self.workers.values())

    @property
    def total_failed(self) -> int:
        return sum(w.messages_failed for w in self.workers.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_processed": self.total_processed,
            "total_succeeded": self.total_succeeded,
            "total_failed": self.total_failed,
            "workers": {
                name: {
                    "processed": m.messages_processed,
                    "succeeded": m.messages_succeeded,
                    "failed": m.messages_failed,
                    "skipped": m.messages_skipped,
                    "retried": m.messages_retried,
                    "avg_processing_time": (
                        m.total_processing_time / m.messages_processed
                        if m.messages_processed > 0
                        else 0
                    ),
                    "last_message_at": (
                        m.last_message_at.isoformat() if m.last_message_at else None
                    ),
                }
                for name, m in self.workers.items()
            },
        }


class NatsConsumer:
    """
    NATS JetStream consumer with robust error handling and graceful shutdown.

    Usage:
        async with NatsConsumer(nats_config, handler) as consumer:
            await consumer.start()
            # Consumer runs until shutdown signal
    """

    def __init__(
        self,
        nats_config: NatsConfig,
        handler: MessageHandler,
        stream_settings: StreamSettings | None = None,
        consumer_settings: ConsumerSettings | None = None,
        dead_letter_settings: DeadLetterSettings | None = None,
        worker_settings: WorkerSettings | None = None,
    ):
        self.nats_config = nats_config
        self.handler = handler
        self.stream_settings = stream_settings or StreamSettings()
        self.consumer_settings = consumer_settings or ConsumerSettings()
        self.dead_letter_settings = dead_letter_settings or DeadLetterSettings()
        self.worker_settings = worker_settings or WorkerSettings()

        self._nats_client: NATS | None = None
        self._jetstream: JetStreamContext | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._metrics = ConsumerMetrics()

    @property
    def is_connected(self) -> bool:
        """Check if NATS client is connected."""
        return self._nats_client is not None and self._nats_client.is_connected

    @property
    def metrics(self) -> ConsumerMetrics:
        """Get consumer metrics."""
        return self._metrics

    async def __aenter__(self) -> "NatsConsumer":
        """Async context manager entry - connect to NATS."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit - graceful shutdown."""
        await self.shutdown()

    async def connect(self) -> None:
        """
        Connect to NATS and initialize JetStream.

        Sets up:
        1. NATS connection with reconnection handling
        2. JetStream context
        3. Stream (creates if not exists)
        4. Consumer (creates if not exists)
        """
        logger.info(f"Connecting to NATS at {self.nats_config.url}")

        self._nats_client = await self._connect_with_retry()
        self._jetstream = self._nats_client.jetstream()

        await self._ensure_stream()
        await self._ensure_consumer()

        logger.info("NATS consumer initialized successfully")

    async def _connect_with_retry(self, max_retries: int = 5) -> NATS:
        """Connect to NATS with exponential backoff retry."""
        retry_delay = 2

        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"NATS connection attempt {attempt}/{max_retries}")
                client = NATS()
                await client.connect(
                    self.nats_config.url,
                    error_cb=self._error_callback,
                    disconnected_cb=self._disconnected_callback,
                    reconnected_cb=self._reconnected_callback,
                    connect_timeout=self.nats_config.connect_timeout,
                    reconnect_time_wait=self.nats_config.reconnect_time_wait,
                    max_reconnect_attempts=self.nats_config.max_reconnect_attempts,
                    ping_interval=self.nats_config.ping_interval,
                    max_outstanding_pings=self.nats_config.max_outstanding_pings,
                )
                logger.info("Connected to NATS successfully")
                return client
            except Exception as e:
                logger.error(f"NATS connection failed (attempt {attempt}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)
                else:
                    raise

    async def _error_callback(self, error: Exception) -> None:
        """Handle NATS client errors."""
        if isinstance(error, nats.errors.SlowConsumerError):
            logger.warning(f"NATS slow consumer warning: {error}")
        else:
            logger.error(f"NATS client error: {error}")

    async def _disconnected_callback(self) -> None:
        """Handle NATS disconnection."""
        logger.warning("NATS client disconnected")

    async def _reconnected_callback(self) -> None:
        """Handle NATS reconnection."""
        logger.info("NATS client reconnected")

    async def _ensure_stream(self) -> None:
        """Ensure the JetStream stream exists."""
        config = self.stream_settings.to_stream_config()
        try:
            # Check if stream exists
            existing = await self._jetstream.stream_info(config.name)
            logger.debug(
                f"Stream {config.name} exists with {existing.state.messages} messages"
            )
            # Don't try to update - some settings like retention are immutable
        except NotFoundError:
            await self._jetstream.add_stream(config=config)
            logger.info(f"Created stream {config.name}")

    async def _ensure_consumer(self) -> None:
        """Ensure the JetStream consumer exists with correct config."""
        config = self.consumer_settings.to_consumer_config()
        stream_name = self.stream_settings.name

        try:
            info = await self._jetstream.consumer_info(stream_name, config.durable_name)
            # Check if config needs update
            if self._consumer_config_changed(info.config, config):
                logger.info(
                    f"Consumer config changed, recreating {config.durable_name}"
                )
                await self._jetstream.delete_consumer(stream_name, config.durable_name)
                await self._jetstream.add_consumer(stream=stream_name, config=config)
            else:
                logger.debug(
                    f"Consumer {config.durable_name} exists with correct config"
                )
        except NotFoundError:
            await self._jetstream.add_consumer(stream=stream_name, config=config)
            logger.info(f"Created consumer {config.durable_name}")

    def _consumer_config_changed(
        self, current: ConsumerConfig, desired: ConsumerConfig
    ) -> bool:
        """Check if consumer config has changed."""
        return (
            current.ack_wait != desired.ack_wait
            or current.max_deliver != desired.max_deliver
            or current.max_ack_pending != desired.max_ack_pending
            or current.filter_subject != desired.filter_subject
        )

    async def start(self) -> None:
        """
        Start consumer workers.

        Creates worker tasks that will process messages until shutdown.
        """
        if not self.is_connected:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        logger.info(f"Starting {self.worker_settings.num_workers} consumer workers")

        for i in range(self.worker_settings.num_workers):
            worker_id = f"worker-{i + 1}"
            self._metrics.workers[worker_id] = WorkerMetrics()
            task = asyncio.create_task(
                self._run_worker(worker_id),
                name=f"nats-{worker_id}",
            )
            self._worker_tasks.append(task)

        logger.info("Consumer workers started")

    async def shutdown(self) -> None:
        """
        Gracefully shutdown the consumer.

        1. Signal workers to stop accepting new messages
        2. Wait for in-flight messages to complete (with timeout)
        3. Cancel remaining tasks
        4. Close NATS connection
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

        if self._nats_client:
            try:
                await self._nats_client.drain()
                await self._nats_client.close()
            except Exception as e:
                logger.error(f"Error closing NATS connection: {e}")

        logger.info("Consumer shutdown complete")

    async def _run_worker(self, worker_id: str) -> None:
        """
        Main worker loop for processing messages.

        Args:
            worker_id: Unique identifier for this worker
        """
        logger.info(f"{worker_id}: Starting")
        metrics = self._metrics.workers[worker_id]
        subscription = None
        reconnect_delay = 2

        while not self._shutdown_event.is_set():
            try:
                # Ensure connection
                if not self.is_connected:
                    logger.warning(
                        f"{worker_id}: NATS disconnected, waiting for reconnect"
                    )
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 30)
                    continue

                reconnect_delay = 2  # Reset on successful connection

                # Bind to consumer if needed
                if subscription is None:
                    subscription = await self._jetstream.pull_subscribe_bind(
                        consumer=self.consumer_settings.durable_name,
                        stream=self.stream_settings.name,
                    )
                    logger.info(f"{worker_id}: Bound to consumer")

                # Check queue depth for adaptive timeout
                try:
                    info = await self._jetstream.consumer_info(
                        self.stream_settings.name,
                        self.consumer_settings.durable_name,
                    )
                    has_pending = info.num_pending > 0
                except Exception:
                    has_pending = False

                fetch_timeout = (
                    self.consumer_settings.fetch_timeout_busy
                    if has_pending
                    else self.consumer_settings.fetch_timeout_idle
                )

                # Fetch messages
                try:
                    messages = await subscription.fetch(
                        batch=self.consumer_settings.fetch_batch_size,
                        timeout=fetch_timeout,
                    )
                except nats.errors.TimeoutError:
                    continue  # Normal when queue is empty

                # Process messages
                for msg in messages:
                    if self._shutdown_event.is_set():
                        logger.info(
                            f"{worker_id}: Shutdown requested, finishing message"
                        )

                    await self._process_message(worker_id, msg, metrics)

            except asyncio.CancelledError:
                logger.info(f"{worker_id}: Cancelled")
                raise
            except Exception as e:
                logger.exception(f"{worker_id}: Error in worker loop: {e}")
                subscription = None  # Reset subscription on error
                await asyncio.sleep(2)

        logger.info(f"{worker_id}: Stopped")

    async def _process_message(
        self, worker_id: str, msg: Msg, metrics: WorkerMetrics
    ) -> None:
        """
        Process a single message with proper ACK/NACK handling.

        Args:
            worker_id: Worker identifier
            msg: NATS message
            metrics: Worker metrics to update
        """
        # Extract message context
        context = self._extract_context(msg)

        # Log raw message data for debugging
        try:
            msg_data = msg.data.decode('utf-8')
            logger.info(
                f"{worker_id}: Processing message {context.message_id} "
                f"(delivery {context.delivery_count}) - data: {msg_data}"
            )
        except Exception:
            logger.info(
                f"{worker_id}: Processing message {context.message_id} "
                f"(delivery {context.delivery_count})"
            )

        metrics.messages_processed += 1
        metrics.last_message_at = datetime.utcnow()

        try:
            result = await self.handler.handle(msg.data, context)

            if result.metadata and result.metadata.get("skipped"):
                metrics.messages_skipped += 1
            else:
                metrics.messages_succeeded += 1

            metrics.total_processing_time += result.duration_seconds
            await msg.ack()

            logger.info(
                f"{worker_id}: Completed message {context.message_id} "
                f"in {result.duration_seconds:.2f}s"
            )

        except SkipMessageError as e:
            metrics.messages_skipped += 1
            await msg.ack()
            logger.info(f"{worker_id}: Skipped message {context.message_id}: {e}")

        except RetriableError as e:
            logger.warning(
                f"{worker_id}: Retriable error for {context.message_id}: {e}"
            )
            # Check if max deliveries reached
            max_deliver = self.consumer_settings.max_deliver
            if context.delivery_count >= max_deliver:
                logger.error(
                    f"{worker_id}: Message {context.message_id} exceeded "
                    f"max deliveries ({max_deliver}), sending to DLQ"
                )
                await self._send_to_dlq(msg, context, str(e))
                await msg.ack()
                metrics.messages_failed += 1
            else:
                # NACK to trigger redelivery
                await msg.nak()
                metrics.messages_retried += 1
                logger.info(
                    f"{worker_id}: Message {context.message_id} will be redelivered "
                    f"(attempt {context.delivery_count}/{max_deliver})"
                )

        except PermanentError as e:
            logger.error(f"{worker_id}: Permanent error for {context.message_id}: {e}")
            await self._send_to_dlq(msg, context, str(e))
            await msg.ack()
            metrics.messages_failed += 1

        except Exception as e:
            logger.exception(
                f"{worker_id}: Unexpected error for {context.message_id}: {e}"
            )
            # Treat unexpected errors as retriable
            if context.delivery_count >= self.consumer_settings.max_deliver:
                await self._send_to_dlq(msg, context, f"Unexpected error: {e}")
                await msg.ack()
                metrics.messages_failed += 1
            else:
                await msg.nak()
                metrics.messages_retried += 1

    def _extract_context(self, msg: Msg) -> MessageContext:
        """Extract message context from NATS message."""
        # Get sequence number
        try:
            if hasattr(msg.metadata, "sequence"):
                seq = msg.metadata.sequence.stream
            else:
                seq = getattr(msg.metadata, "stream_seq", 0)
        except Exception:
            seq = 0

        # Get delivery count
        try:
            delivery_count = msg.metadata.num_delivered
        except Exception:
            delivery_count = 1

        # Get timestamp
        try:
            timestamp = msg.metadata.timestamp
        except Exception:
            timestamp = datetime.utcnow()

        # Extract headers
        headers = {}
        if msg.headers:
            for key, value in msg.headers.items():
                headers[key] = value if isinstance(value, str) else str(value)

        return MessageContext(
            message_id=f"{msg.subject}:{seq}",
            subject=msg.subject,
            stream_seq=seq,
            delivery_count=delivery_count,
            timestamp=timestamp,
            headers=headers,
        )

    async def _send_to_dlq(self, msg: Msg, context: MessageContext, error: str) -> None:
        """
        Send failed message to dead letter queue.

        Args:
            msg: Original NATS message
            context: Message context
            error: Error description
        """
        if not self._jetstream:
            logger.error("Cannot send to DLQ: JetStream not initialized")
            return

        dlq_payload = {
            "original_subject": context.subject,
            "original_message_id": context.message_id,
            "delivery_count": context.delivery_count,
            "error": error,
            "failed_at": datetime.utcnow().isoformat(),
            "payload": msg.data.decode("utf-8", errors="replace"),
        }

        try:
            await self._jetstream.publish(
                QueueSubject.DEAD_LETTER.value,
                json.dumps(dlq_payload).encode(),
            )
            logger.info(f"Sent message {context.message_id} to DLQ")
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")

    # Public methods for external access

    async def get_queue_stats(self) -> dict[str, Any]:
        """Get current queue statistics."""
        if not self._jetstream:
            return {"error": "Not connected"}

        try:
            stream_info = await self._jetstream.stream_info(self.stream_settings.name)
            consumer_info = await self._jetstream.consumer_info(
                self.stream_settings.name,
                self.consumer_settings.durable_name,
            )

            return {
                "stream": {
                    "name": stream_info.config.name,
                    "messages": stream_info.state.messages,
                    "bytes": stream_info.state.bytes,
                },
                "consumer": {
                    "name": consumer_info.name,
                    "pending": consumer_info.num_pending,
                    "ack_pending": consumer_info.num_ack_pending,
                    "redelivered": consumer_info.num_redelivered,
                    "waiting": consumer_info.num_waiting,
                },
                "workers": self._metrics.to_dict(),
            }
        except Exception as e:
            return {"error": str(e)}

    async def reset_consumer(self) -> dict[str, Any]:
        """
        Reset the consumer by deleting and recreating it.

        Use when messages are stuck in ack_pending from crashed workers.
        """
        if not self._jetstream:
            return {"error": "Not connected"}

        stream_name = self.stream_settings.name
        config = self.consumer_settings.to_consumer_config()

        try:
            # Get old state
            try:
                old_info = await self._jetstream.consumer_info(
                    stream_name, config.durable_name
                )
                old_state = {
                    "pending": old_info.num_pending,
                    "ack_pending": old_info.num_ack_pending,
                }
            except NotFoundError:
                old_state = {"error": "Consumer not found"}

            # Delete and recreate
            try:
                await self._jetstream.delete_consumer(stream_name, config.durable_name)
            except NotFoundError:
                pass

            await self._jetstream.add_consumer(stream=stream_name, config=config)

            # Get new state
            new_info = await self._jetstream.consumer_info(
                stream_name, config.durable_name
            )
            new_state = {
                "pending": new_info.num_pending,
                "ack_pending": new_info.num_ack_pending,
            }

            logger.info(f"Reset consumer {config.durable_name}")
            return {
                "success": True,
                "old_state": old_state,
                "new_state": new_state,
            }

        except Exception as e:
            logger.error(f"Failed to reset consumer: {e}")
            return {"error": str(e)}
