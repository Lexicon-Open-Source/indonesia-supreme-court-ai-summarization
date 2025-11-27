"""Abstract base classes for queue consumers and producers.

Defines the interface that both NATS and Google Pub/Sub implementations must follow.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class QueueBackend(str, Enum):
    """Supported queue backends."""

    NATS = "nats"
    PUBSUB = "pubsub"


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


@dataclass
class PublishResult:
    """Result of a publish operation."""

    success: bool
    stream: str | None = None
    sequence: int | None = None
    message_id: str | None = None
    duplicate: bool = False
    error: str | None = None


class BaseConsumer(ABC):
    """
    Abstract base class for message consumers.

    Implementations must handle:
    - Connection management
    - Message acknowledgment/rejection
    - Graceful shutdown
    - Metrics collection
    """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if consumer is connected to the queue backend."""
        pass

    @property
    @abstractmethod
    def metrics(self) -> ConsumerMetrics:
        """Get consumer metrics."""
        pass

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the queue backend and initialize resources."""
        pass

    @abstractmethod
    async def start(self) -> None:
        """Start consuming messages."""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Gracefully shutdown the consumer."""
        pass

    @abstractmethod
    async def get_queue_stats(self) -> dict[str, Any]:
        """Get current queue statistics."""
        pass

    @abstractmethod
    async def reset_consumer(self) -> dict[str, Any]:
        """Reset the consumer (for stuck messages recovery)."""
        pass

    async def __aenter__(self) -> "BaseConsumer":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.shutdown()


class BaseProducer(ABC):
    """
    Abstract base class for message producers.

    Implementations must handle:
    - Message publishing
    - Deduplication
    - Error handling
    """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if producer is connected to the queue backend."""
        pass

    @abstractmethod
    async def publish(
        self,
        subject: str,
        payload: dict[str, Any] | BaseModel,
        headers: dict[str, str] | None = None,
        msg_id: str | None = None,
    ) -> PublishResult:
        """
        Publish a message to the specified subject/topic.

        Args:
            subject: Subject/topic to publish to
            payload: Message payload (dict or Pydantic model)
            headers: Optional message headers/attributes
            msg_id: Optional message ID for deduplication

        Returns:
            PublishResult with publish outcome
        """
        pass

    @abstractmethod
    async def publish_extraction(
        self,
        extraction_id: str,
        priority: int = 0,
    ) -> PublishResult:
        """
        Publish an extraction job to the queue.

        Args:
            extraction_id: ID of the extraction to process
            priority: Optional priority (higher = more important)

        Returns:
            PublishResult with publish outcome
        """
        pass

    async def publish_batch(
        self,
        extraction_ids: list[str],
        max_concurrent: int = 10,
    ) -> dict[str, PublishResult]:
        """
        Publish multiple extractions with controlled concurrency.

        Args:
            extraction_ids: List of extraction IDs to publish
            max_concurrent: Maximum concurrent publish operations

        Returns:
            Dict mapping extraction_id to PublishResult
        """
        import asyncio
        import logging

        logger = logging.getLogger(__name__)
        results: dict[str, PublishResult] = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def publish_one(extraction_id: str) -> None:
            async with semaphore:
                try:
                    results[extraction_id] = await self.publish_extraction(
                        extraction_id
                    )
                except Exception as e:
                    logger.error(f"Failed to publish {extraction_id}: {e}")
                    results[extraction_id] = PublishResult(
                        success=False,
                        error=str(e),
                    )

        await asyncio.gather(*[publish_one(eid) for eid in extraction_ids])

        success_count = sum(1 for r in results.values() if r.success)
        logger.info(f"Published {success_count}/{len(extraction_ids)} extractions")

        return results
