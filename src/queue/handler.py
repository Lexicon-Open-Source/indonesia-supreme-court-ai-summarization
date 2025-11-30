"""Message handler protocol and base implementation.

Defines the interface for processing messages from the queue.
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError

from .errors import PermanentError, RetriableError, SkipMessageError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


@dataclass
class MessageContext:
    """Context information about the message being processed."""

    message_id: str
    subject: str
    stream_seq: int
    delivery_count: int
    timestamp: datetime
    headers: dict[str, str]


@dataclass
class ProcessingResult:
    """Result of message processing."""

    success: bool
    message_id: str
    duration_seconds: float
    error: str | None = None
    metadata: dict[str, Any] | None = None


class MessageHandler(ABC, Generic[T]):
    """
    Abstract base class for message handlers.

    Subclasses implement domain-specific message processing logic.
    The handler is responsible for:
    1. Deserializing the message payload
    2. Validating the message
    3. Processing the message (business logic)
    4. Reporting success/failure via exceptions

    Error handling convention:
    - Raise RetriableError for transient failures (will be redelivered)
    - Raise PermanentError for permanent failures (will be acked and logged)
    - Raise SkipMessageError to skip without error (will be acked silently)
    - Return normally on success
    """

    def __init__(self, payload_model: type[T]):
        """
        Initialize handler with the expected payload model.

        Args:
            payload_model: Pydantic model class for deserializing message payloads
        """
        self.payload_model = payload_model

    def deserialize(self, data: bytes) -> T:
        """
        Deserialize message data into the payload model.

        Args:
            data: Raw message bytes

        Returns:
            Deserialized payload object

        Raises:
            PermanentError: If deserialization fails (malformed message)
        """
        try:
            json_data = json.loads(data.decode("utf-8"))
            return self.payload_model.model_validate(json_data)
        except json.JSONDecodeError as e:
            raise PermanentError(
                message="Failed to decode JSON payload",
                original_error=e,
                should_update_status=False,
            )
        except ValidationError as e:
            raise PermanentError(
                message="Failed to validate message payload",
                original_error=e,
                should_update_status=False,
            )

    @abstractmethod
    async def validate(self, payload: T, context: MessageContext) -> None:
        """
        Validate the message before processing.

        Called after deserialization but before process().
        Use this for:
        - Idempotency checks (already processed?)
        - Prerequisites (resources exist?)
        - Business rules (valid state?)

        Args:
            payload: Deserialized message payload
            context: Message context information

        Raises:
            SkipMessageError: If message should be skipped
            PermanentError: If validation fails permanently
            RetriableError: If validation fails transiently
        """
        pass

    @abstractmethod
    async def process(self, payload: T, context: MessageContext) -> dict[str, Any]:
        """
        Process the message.

        This is where the main business logic goes.

        Args:
            payload: Validated message payload
            context: Message context information

        Returns:
            Dict with processing results/metadata

        Raises:
            RetriableError: For transient failures
            PermanentError: For permanent failures
        """
        pass

    async def on_success(
        self, payload: T, context: MessageContext, result: dict[str, Any]
    ) -> None:
        """
        Called after successful processing.

        Override to add post-processing logic like:
        - Updating status to COMPLETED
        - Sending notifications
        - Recording metrics

        Args:
            payload: Message payload
            context: Message context
            result: Processing result from process()
        """
        pass

    async def on_failure(
        self, payload: T | None, context: MessageContext, error: Exception
    ) -> None:
        """
        Called after processing failure.

        Override to add error handling logic like:
        - Updating status to FAILED
        - Recording error details
        - Alerting

        Args:
            payload: Message payload (may be None if deserialization failed)
            context: Message context
            error: The exception that caused the failure
        """
        pass

    async def on_skip(self, payload: T, context: MessageContext, reason: str) -> None:
        """
        Called when message is skipped.

        Override to add skip logging/metrics.

        Args:
            payload: Message payload
            context: Message context
            reason: Reason for skipping
        """
        logger.info(f"Skipped message {context.message_id}: {reason}")

    async def handle(self, data: bytes, context: MessageContext) -> ProcessingResult:
        """
        Main entry point for message handling.

        Orchestrates the full message processing lifecycle:
        1. Deserialize
        2. Validate
        3. Process
        4. Callbacks (success/failure/skip)

        Args:
            data: Raw message bytes
            context: Message context

        Returns:
            ProcessingResult indicating outcome

        Note: This method catches and re-raises errors for the consumer
        to handle ACK/NACK decisions.
        """
        import asyncio

        start_time = asyncio.get_event_loop().time()
        payload = None

        logger.info(f"Handling message {context.message_id}")

        try:
            # Deserialize
            logger.debug(f"Deserializing message {context.message_id}")
            payload = self.deserialize(data)
            logger.info(f"Deserialized message {context.message_id}: {payload}")

            # Validate
            logger.debug(f"Validating message {context.message_id}")
            await self.validate(payload, context)

            # Process
            logger.info(f"Processing message {context.message_id}")
            result = await self.process(payload, context)

            # Success callback
            await self.on_success(payload, context, result)

            duration = asyncio.get_event_loop().time() - start_time
            return ProcessingResult(
                success=True,
                message_id=context.message_id,
                duration_seconds=duration,
                metadata=result,
            )

        except SkipMessageError as e:
            await self.on_skip(payload, context, e.reason)
            duration = asyncio.get_event_loop().time() - start_time
            return ProcessingResult(
                success=True,  # Skip is a "success" (message handled)
                message_id=context.message_id,
                duration_seconds=duration,
                metadata={"skipped": True, "reason": e.reason},
            )

        except (RetriableError, PermanentError) as e:
            await self.on_failure(payload, context, e)
            duration = asyncio.get_event_loop().time() - start_time
            # Re-raise for consumer to handle ACK/NACK
            raise

        except Exception as e:
            # Unexpected error - treat as retriable by default
            logger.exception(
                f"Unexpected error processing message {context.message_id}"
            )
            await self.on_failure(payload, context, e)
            raise RetriableError(
                message="Unexpected error during processing",
                original_error=e,
            )
