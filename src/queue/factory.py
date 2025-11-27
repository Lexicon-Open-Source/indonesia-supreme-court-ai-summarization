"""Queue factory for creating consumers and producers.

Provides a unified interface for creating queue components based on
the configured backend.
"""

import logging
from dataclasses import dataclass
from typing import Any

from .base import BaseConsumer, BaseProducer, QueueBackend
from .handler import MessageHandler

logger = logging.getLogger(__name__)


@dataclass
class QueueConfig:
    """Unified queue configuration."""

    backend: QueueBackend

    # NATS configuration
    nats_url: str | None = None

    # Pub/Sub configuration
    pubsub_project_id: str | None = None
    pubsub_credentials_path: str | None = None

    # Worker settings
    num_workers: int = 3
    shutdown_timeout: float = 30.0


def create_consumer(
    config: QueueConfig,
    handler: MessageHandler,
    **kwargs: Any,
) -> BaseConsumer:
    """
    Create a consumer based on the configured backend.

    Args:
        config: Queue configuration
        handler: Message handler for processing messages
        **kwargs: Additional backend-specific arguments

    Returns:
        Consumer instance (NatsConsumer or PubSubConsumer)

    Raises:
        ValueError: If backend is invalid or required config is missing
    """
    if config.backend == QueueBackend.NATS:
        if not config.nats_url:
            raise ValueError("NATS URL is required for NATS backend")

        from .config import (
            ConsumerSettings,
            NatsConfig,
            StreamSettings,
            WorkerSettings,
        )
        from .consumer import NatsConsumer

        nats_config = NatsConfig(url=config.nats_url)
        stream_settings = kwargs.get("stream_settings", StreamSettings())
        consumer_settings = kwargs.get("consumer_settings", ConsumerSettings())
        worker_settings = kwargs.get(
            "worker_settings",
            WorkerSettings(
                num_workers=config.num_workers,
                shutdown_timeout=config.shutdown_timeout,
            ),
        )

        return NatsConsumer(
            nats_config=nats_config,
            handler=handler,
            stream_settings=stream_settings,
            consumer_settings=consumer_settings,
            worker_settings=worker_settings,
        )

    elif config.backend == QueueBackend.PUBSUB:
        if not config.pubsub_project_id:
            raise ValueError("Project ID is required for Pub/Sub backend")

        from .pubsub_config import (
            PubSubConfig,
            PubSubWorkerSettings,
        )
        from .pubsub_consumer import PubSubConsumer

        pubsub_config = PubSubConfig(
            project_id=config.pubsub_project_id,
            credentials_path=config.pubsub_credentials_path,
        )

        # Topic and subscription settings MUST be provided via kwargs
        # (from environment variables via settings)
        topic_settings = kwargs.get("topic_settings")
        subscription_settings = kwargs.get("subscription_settings")
        dead_letter_settings = kwargs.get("dead_letter_settings")

        if not topic_settings:
            raise ValueError("topic_settings is required for Pub/Sub backend")
        if not subscription_settings:
            raise ValueError("subscription_settings is required for Pub/Sub backend")

        worker_settings = kwargs.get(
            "worker_settings",
            PubSubWorkerSettings(
                num_workers=config.num_workers,
                shutdown_timeout=config.shutdown_timeout,
            ),
        )

        return PubSubConsumer(
            pubsub_config=pubsub_config,
            handler=handler,
            topic_settings=topic_settings,
            subscription_settings=subscription_settings,
            dead_letter_settings=dead_letter_settings,
            worker_settings=worker_settings,
        )

    else:
        raise ValueError(f"Unsupported queue backend: {config.backend}")


def create_producer_from_consumer(
    config: QueueConfig,
    consumer: BaseConsumer,
    **kwargs: Any,
) -> BaseProducer:
    """
    Create a producer using an existing consumer's connection.

    This is the preferred method as it reuses the existing connection.

    Args:
        config: Queue configuration
        consumer: Existing consumer to share connection with
        **kwargs: Additional backend-specific arguments

    Returns:
        Producer instance (NatsProducer or PubSubProducer)
    """
    if config.backend == QueueBackend.NATS:
        from .config import StreamSettings
        from .consumer import NatsConsumer
        from .producer import NatsProducer

        if not isinstance(consumer, NatsConsumer):
            raise TypeError("Consumer must be NatsConsumer for NATS backend")

        stream_settings = kwargs.get("stream_settings", StreamSettings())

        return NatsProducer(
            nats_client=consumer._nats_client,
            jetstream=consumer._jetstream,
            stream_settings=stream_settings,
        )

    elif config.backend == QueueBackend.PUBSUB:
        from .pubsub_config import PubSubConfig
        from .pubsub_consumer import PubSubConsumer
        from .pubsub_producer import PubSubProducer

        if not isinstance(consumer, PubSubConsumer):
            raise TypeError("Consumer must be PubSubConsumer for Pub/Sub backend")

        pubsub_config = PubSubConfig(
            project_id=config.pubsub_project_id,
            credentials_path=config.pubsub_credentials_path,
        )

        # Topic settings MUST be provided via kwargs
        topic_settings = kwargs.get("topic_settings")
        if not topic_settings:
            raise ValueError("topic_settings is required for Pub/Sub backend")

        return PubSubProducer(
            pubsub_config=pubsub_config,
            topic_settings=topic_settings,
            publisher=consumer._publisher,
        )

    else:
        raise ValueError(f"Unsupported queue backend: {config.backend}")


async def create_standalone_producer(
    config: QueueConfig,
    **kwargs: Any,
) -> BaseProducer:
    """
    Create a standalone producer with its own connection.

    Use this when you don't have an existing consumer.

    Args:
        config: Queue configuration
        **kwargs: Additional backend-specific arguments

    Returns:
        Producer instance (NatsProducer or PubSubProducer)
    """
    if config.backend == QueueBackend.NATS:
        from .config import NatsConfig, StreamSettings
        from .producer import ProducerFactory

        nats_config = NatsConfig(url=config.nats_url)
        stream_settings = kwargs.get("stream_settings", StreamSettings())

        factory = ProducerFactory(nats_config, stream_settings)
        await factory.connect()
        return factory.create_producer()

    elif config.backend == QueueBackend.PUBSUB:
        from .pubsub_config import PubSubConfig
        from .pubsub_producer import PubSubProducer

        pubsub_config = PubSubConfig(
            project_id=config.pubsub_project_id,
            credentials_path=config.pubsub_credentials_path,
        )

        # Topic settings MUST be provided via kwargs
        topic_settings = kwargs.get("topic_settings")
        if not topic_settings:
            raise ValueError("topic_settings is required for Pub/Sub backend")

        producer = PubSubProducer(
            pubsub_config=pubsub_config,
            topic_settings=topic_settings,
        )
        await producer.connect()
        return producer

    else:
        raise ValueError(f"Unsupported queue backend: {config.backend}")
