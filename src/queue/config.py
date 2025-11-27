"""NATS JetStream configuration for the extraction queue.

Configuration is centralized here and can be tuned based on workload characteristics.
"""

from dataclasses import dataclass, field
from enum import Enum

from nats.js.api import ConsumerConfig, RetentionPolicy, StreamConfig


class QueueSubject(str, Enum):
    """NATS subjects for different message types."""

    # Use .summarize to match existing stream subject
    EXTRACTION = "SUPREME_COURT_SUMMARIZATION_EVENT.summarize"
    DEAD_LETTER = "SUPREME_COURT_SUMMARIZATION_EVENT.dead_letter"


@dataclass
class NatsConfig:
    """NATS connection configuration."""

    url: str
    connect_timeout: int = 10
    reconnect_time_wait: int = 2
    max_reconnect_attempts: int = 10
    ping_interval: int = 60
    max_outstanding_pings: int = 3


@dataclass
class StreamSettings:
    """JetStream stream settings."""

    name: str = "SUPREME_COURT_SUMMARIZATION_EVENT"
    subjects: list[str] = field(
        default_factory=lambda: ["SUPREME_COURT_SUMMARIZATION_EVENT.>"]
    )
    # Use LIMITS (default) - WORK_QUEUE can't be changed on existing streams
    retention: RetentionPolicy = RetentionPolicy.LIMITS
    max_msgs: int = 100_000
    max_bytes: int = 1_073_741_824  # 1GB
    max_age: int = 604800  # 7 days in seconds
    duplicate_window: int = 120  # 2 minutes for deduplication

    def to_stream_config(self) -> StreamConfig:
        """Convert to NATS StreamConfig."""
        return StreamConfig(
            name=self.name,
            subjects=self.subjects,
            retention=self.retention,
            max_msgs=self.max_msgs,
            max_bytes=self.max_bytes,
            max_age=self.max_age,
            duplicate_window=self.duplicate_window,
        )


@dataclass
class ConsumerSettings:
    """JetStream consumer settings optimized for long-running extraction tasks."""

    # Use existing consumer name for compatibility
    durable_name: str = "SUPREME_COURT_SUMMARIZATION"
    filter_subject: str = QueueSubject.EXTRACTION.value

    # Acknowledgment settings
    # How long before unacked message is redelivered (must exceed max processing time)
    ack_wait: int = 7200  # 2 hours (increased from 1 hour for large documents)

    # Redelivery settings
    max_deliver: int = 3  # Max attempts before message goes to DLQ

    # Flow control
    # Max messages being processed simultaneously across all workers
    max_ack_pending: int = 10  # Increased from 3 to allow better parallelism

    # Batch settings for pull subscriptions
    fetch_batch_size: int = 1  # Process one at a time for long-running tasks
    fetch_timeout_idle: float = 30.0  # Timeout when queue appears empty
    fetch_timeout_busy: float = 1.0  # Quick timeout when messages pending

    def to_consumer_config(self) -> ConsumerConfig:
        """Convert to NATS ConsumerConfig."""
        return ConsumerConfig(
            durable_name=self.durable_name,
            filter_subject=self.filter_subject,
            ack_wait=self.ack_wait,
            max_deliver=self.max_deliver,
            max_ack_pending=self.max_ack_pending,
        )


@dataclass
class DeadLetterSettings:
    """Dead letter queue consumer settings."""

    durable_name: str = "DEAD_LETTER_CONSUMER"
    filter_subject: str = QueueSubject.DEAD_LETTER.value
    ack_wait: int = 300  # 5 minutes (DLQ processing should be fast)
    max_deliver: int = 1  # Don't retry DLQ messages
    max_ack_pending: int = 5

    def to_consumer_config(self) -> ConsumerConfig:
        """Convert to NATS ConsumerConfig."""
        return ConsumerConfig(
            durable_name=self.durable_name,
            filter_subject=self.filter_subject,
            ack_wait=self.ack_wait,
            max_deliver=self.max_deliver,
            max_ack_pending=self.max_ack_pending,
        )


@dataclass
class WorkerSettings:
    """Settings for consumer worker instances."""

    num_workers: int = 3
    shutdown_timeout: float = 30.0  # Max time to wait for graceful shutdown
    health_check_interval: float = 60.0  # Interval for health reporting


# Default configurations
DEFAULT_STREAM_SETTINGS = StreamSettings()
DEFAULT_CONSUMER_SETTINGS = ConsumerSettings()
DEFAULT_DEAD_LETTER_SETTINGS = DeadLetterSettings()
DEFAULT_WORKER_SETTINGS = WorkerSettings()
