"""Google Pub/Sub configuration for the extraction queue.

Configuration is centralized here and can be tuned based on workload characteristics.
All topic/subscription names should be passed from settings (environment variables).
"""

from dataclasses import dataclass


@dataclass
class PubSubConfig:
    """Google Pub/Sub connection configuration."""

    project_id: str
    # Optional: Path to service account credentials JSON file
    # If not provided, will use GOOGLE_APPLICATION_CREDENTIALS env var
    credentials_path: str | None = None


@dataclass
class PubSubTopicSettings:
    """Pub/Sub topic settings."""

    # Topic name - MUST be provided from settings/environment
    name: str

    # Message retention duration in seconds (default: 7 days)
    message_retention_duration: int = 604800
    # Whether to enable message ordering (requires ordering key in messages)
    enable_message_ordering: bool = False


@dataclass
class PubSubSubscriptionSettings:
    """Pub/Sub subscription settings optimized for long-running extraction tasks."""

    # Subscription name - MUST be provided from settings/environment
    name: str
    # Topic name - MUST be provided from settings/environment
    topic_name: str
    # Dead letter topic - MUST be provided from settings/environment
    dead_letter_topic: str

    # Acknowledgment settings
    # How long Pub/Sub waits for ack before redelivering (max 600s = 10 min)
    # With ack deadline extension, we can handle longer processing
    ack_deadline_seconds: int = 300  # 5 minutes base deadline

    # Redelivery settings
    # Max delivery attempts before sending to dead letter topic
    max_delivery_attempts: int = 3

    # Flow control - max messages in flight per worker
    max_messages: int = 1  # Process one at a time for long-running tasks

    # Message retention (how long undelivered messages are kept)
    message_retention_duration: int = 604800  # 7 days

    # Exactly-once delivery (requires additional setup)
    enable_exactly_once_delivery: bool = False

    def __post_init__(self) -> None:
        """Validate configuration constraints."""
        # Pub/Sub ack_deadline_seconds must be between 10 and 600 seconds
        if not (10 <= self.ack_deadline_seconds <= 600):
            raise ValueError(
                f"ack_deadline_seconds must be between 10 and 600, "
                f"got {self.ack_deadline_seconds}"
            )

        if self.max_delivery_attempts < 1:
            raise ValueError(
                f"max_delivery_attempts must be at least 1, "
                f"got {self.max_delivery_attempts}"
            )

        if self.max_messages < 1:
            raise ValueError(
                f"max_messages must be at least 1, got {self.max_messages}"
            )

        if self.message_retention_duration < 0:
            raise ValueError(
                f"message_retention_duration must be non-negative, "
                f"got {self.message_retention_duration}"
            )


@dataclass
class PubSubDeadLetterSettings:
    """Dead letter topic and subscription settings."""

    # DLQ topic name - MUST be provided from settings/environment
    topic_name: str
    # DLQ subscription name - MUST be provided from settings/environment
    subscription_name: str

    ack_deadline_seconds: int = 60  # DLQ processing should be fast
    max_messages: int = 5


@dataclass
class PubSubWorkerSettings:
    """Settings for consumer worker instances."""

    num_workers: int = 3
    shutdown_timeout: float = 30.0  # Max time to wait for graceful shutdown
    health_check_interval: float = 60.0  # Interval for health reporting

    # Ack deadline extension settings
    # How often to extend the ack deadline (should be < ack_deadline_seconds)
    ack_extension_interval: int = 60  # Extend every 60 seconds
    # How much to extend the deadline by
    ack_extension_seconds: int = 300  # Extend by 5 minutes
