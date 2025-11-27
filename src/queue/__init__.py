"""Queue module for extraction job processing.

This module provides message queue implementations with:
- Proper ACK/NACK handling based on error types
- Graceful shutdown with in-flight message completion
- Dead letter queue for failed messages
- Comprehensive metrics and monitoring

Supports multiple backends:
- NATS JetStream
- Google Pub/Sub

Usage:
    from src.queue import create_consumer, create_producer_from_consumer, QueueConfig
    from src.queue.base import QueueBackend

    # Create consumer using factory
    config = QueueConfig(
        backend=QueueBackend.NATS,  # or QueueBackend.PUBSUB
        nats_url="nats://localhost:4222",
        # or pubsub_project_id="my-project",
    )
    consumer = create_consumer(config, handler)
    await consumer.connect()
    await consumer.start()

    # Create producer from consumer
    producer = create_producer_from_consumer(config, consumer)
    await producer.publish_extraction("extraction-123")
"""

# Base classes and types
from .base import (
    BaseConsumer,
    BaseProducer,
    ConsumerMetrics,
    PublishResult,
    QueueBackend,
    WorkerMetrics,
    WorkerState,
)

# NATS configuration
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

# NATS implementation
from .consumer import NatsConsumer

# Errors
from .errors import (
    ConnectionError,
    ConsumerError,
    PermanentError,
    QueueError,
    RetriableError,
    SkipMessageError,
    StreamError,
)

# Extraction Handler
from .extraction_handler import (
    ExtractionHandler,
    ExtractionPayload,
)

# Factory functions
from .factory import (
    QueueConfig,
    create_consumer,
    create_producer_from_consumer,
    create_standalone_producer,
)

# Handler
from .handler import (
    MessageContext,
    MessageHandler,
    ProcessingResult,
)
from .producer import NatsProducer, ProducerFactory

# Note: PublishResult is now in base.py, not producer.py
# Pub/Sub configuration
from .pubsub_config import (
    PubSubConfig,
    PubSubDeadLetterSettings,
    PubSubSubscriptionSettings,
    PubSubTopicSettings,
    PubSubWorkerSettings,
)

# Pub/Sub implementation
from .pubsub_consumer import PubSubConsumer
from .pubsub_producer import PubSubProducer, PubSubProducerFactory

__all__ = [
    # Base classes
    "BaseConsumer",
    "BaseProducer",
    "QueueBackend",
    "ConsumerMetrics",
    "WorkerMetrics",
    "WorkerState",
    "PublishResult",
    # Factory
    "QueueConfig",
    "create_consumer",
    "create_producer_from_consumer",
    "create_standalone_producer",
    # NATS Config
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
    # NATS Implementation
    "NatsConsumer",
    "NatsProducer",
    "ProducerFactory",
    # Pub/Sub Config
    "PubSubConfig",
    "PubSubTopicSettings",
    "PubSubSubscriptionSettings",
    "PubSubDeadLetterSettings",
    "PubSubWorkerSettings",
    # Pub/Sub Implementation
    "PubSubConsumer",
    "PubSubProducer",
    "PubSubProducerFactory",
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
