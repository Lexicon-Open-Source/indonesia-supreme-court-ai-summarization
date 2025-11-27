"""NATS Queue module for extraction job processing.

This module provides a robust NATS JetStream implementation with:
- Proper ACK/NACK handling based on error types
- Graceful shutdown with in-flight message completion
- Dead letter queue for failed messages
- Comprehensive metrics and monitoring

Usage:
    from src.queue import NatsConsumer, NatsProducer, ExtractionHandler

    # Consumer
    async with NatsConsumer(nats_config, handler) as consumer:
        await consumer.start()

    # Producer
    producer = NatsProducer(nats_client, jetstream)
    await producer.publish_extraction("extraction-123")
"""

from .config import (
    DEFAULT_CONSUMER_SETTINGS,
    DEFAULT_DEAD_LETTER_SETTINGS,
    DEFAULT_STREAM_SETTINGS,
    DEFAULT_WORKER_SETTINGS,
    ConsumerSettings,
    DeadLetterSettings,
    NatsConfig,
    QueueSubject,
    StreamSettings,
    WorkerSettings,
)
from .consumer import (
    ConsumerMetrics,
    NatsConsumer,
    WorkerMetrics,
    WorkerState,
)
from .errors import (
    ConnectionError,
    ConsumerError,
    PermanentError,
    QueueError,
    RetriableError,
    SkipMessageError,
    StreamError,
)
from .extraction_handler import (
    ExtractionHandler,
    ExtractionPayload,
)
from .handler import (
    MessageContext,
    MessageHandler,
    ProcessingResult,
)
from .producer import (
    NatsProducer,
    ProducerFactory,
    PublishResult,
)

__all__ = [
    # Config
    "NatsConfig",
    "StreamSettings",
    "ConsumerSettings",
    "DeadLetterSettings",
    "WorkerSettings",
    "QueueSubject",
    "DEFAULT_STREAM_SETTINGS",
    "DEFAULT_CONSUMER_SETTINGS",
    "DEFAULT_DEAD_LETTER_SETTINGS",
    "DEFAULT_WORKER_SETTINGS",
    # Consumer
    "NatsConsumer",
    "ConsumerMetrics",
    "WorkerMetrics",
    "WorkerState",
    # Producer
    "NatsProducer",
    "ProducerFactory",
    "PublishResult",
    # Handler
    "MessageHandler",
    "MessageContext",
    "ProcessingResult",
    # Errors
    "QueueError",
    "RetriableError",
    "PermanentError",
    "SkipMessageError",
    "ConsumerError",
    "ConnectionError",
    "StreamError",
    # Extraction Handler
    "ExtractionHandler",
    "ExtractionPayload",
]
