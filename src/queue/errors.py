"""Custom error types for NATS queue processing.

Error categorization determines message handling:
- RetriableError: Message will be NACK'd for redelivery (transient failures)
- PermanentError: Message will be ACK'd and logged (unrecoverable failures)
- SkipMessageError: Message will be ACK'd silently (intentional skip)
"""

from dataclasses import dataclass


class QueueError(Exception):
    """Base exception for queue-related errors."""

    pass


@dataclass
class RetriableError(QueueError):
    """
    Error that should trigger message redelivery.

    Use for transient failures that may succeed on retry:
    - Network timeouts
    - Database connection failures
    - Rate limiting / throttling
    - External service unavailable
    """

    message: str
    original_error: Exception | None = None

    def __str__(self) -> str:
        if self.original_error:
            return f"{self.message}: {self.original_error}"
        return self.message


@dataclass
class PermanentError(QueueError):
    """
    Error that should NOT trigger redelivery.

    Use for failures that will never succeed on retry:
    - Invalid/malformed message data
    - Missing required resources (document deleted)
    - Business logic validation failures
    - LLM extraction failures after exhausting retries
    """

    message: str
    original_error: Exception | None = None
    should_update_status: bool = True  # Whether to update DB status to FAILED

    def __str__(self) -> str:
        if self.original_error:
            return f"{self.message}: {self.original_error}"
        return self.message


@dataclass
class SkipMessageError(QueueError):
    """
    Message should be acknowledged without processing.

    Use when:
    - Message was already processed (idempotency)
    - Message doesn't meet processing criteria
    - Resource is in an expected skip state
    """

    message: str
    reason: str = "skipped"

    def __str__(self) -> str:
        return f"{self.message} (reason: {self.reason})"


class ConsumerError(QueueError):
    """Error in consumer infrastructure (not message processing)."""

    pass


class ConnectionError(ConsumerError):
    """NATS connection error."""

    pass


class StreamError(ConsumerError):
    """JetStream stream error."""

    pass
