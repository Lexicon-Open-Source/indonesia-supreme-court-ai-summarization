"""NATS JetStream producer for publishing messages to the queue.

Provides a clean interface for publishing extraction jobs.
"""

import json
import logging
from typing import Any

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import PubAck
from pydantic import BaseModel

from .base import BaseProducer, PublishResult
from .config import NatsConfig, QueueSubject, StreamSettings

logger = logging.getLogger(__name__)


class NatsProducer(BaseProducer):
    """
    NATS JetStream producer for publishing messages.

    Usage:
        producer = NatsProducer(nats_client, jetstream)
        result = await producer.publish_extraction("extraction-123")
    """

    def __init__(
        self,
        nats_client: NATS,
        jetstream: JetStreamContext,
        stream_settings: StreamSettings | None = None,
    ):
        self._nats_client = nats_client
        self._jetstream = jetstream
        self.stream_settings = stream_settings or StreamSettings()

    @property
    def is_connected(self) -> bool:
        """Check if NATS client is connected."""
        return self._nats_client is not None and self._nats_client.is_connected

    async def publish(
        self,
        subject: str,
        payload: dict[str, Any] | BaseModel,
        headers: dict[str, str] | None = None,
        msg_id: str | None = None,
    ) -> PublishResult:
        """
        Publish a message to the specified subject.

        Args:
            subject: NATS subject to publish to
            payload: Message payload (dict or Pydantic model)
            headers: Optional message headers
            msg_id: Optional message ID for deduplication

        Returns:
            PublishResult with publish outcome
        """
        if not self.is_connected:
            return PublishResult(success=False, error="NATS client not connected")

        try:
            # Serialize payload
            if isinstance(payload, BaseModel):
                data = payload.model_dump_json().encode()
            else:
                data = json.dumps(payload).encode()

            # Publish with optional deduplication
            ack: PubAck = await self._jetstream.publish(
                subject,
                data,
                headers=headers,
            )

            return PublishResult(
                success=True,
                stream=ack.stream,
                sequence=ack.seq,
                duplicate=getattr(ack, "duplicate", False),
            )

        except Exception as e:
            logger.error(f"Failed to publish to {subject}: {e}")
            return PublishResult(success=False, error=str(e))

    async def publish_extraction(
        self,
        extraction_id: str,
        priority: int = 0,
    ) -> PublishResult:
        """
        Publish an extraction job to the queue.

        Uses extraction_id as message ID for deduplication - duplicate
        messages within the stream's duplicate_window will be rejected.

        Args:
            extraction_id: ID of the extraction to process
            priority: Optional priority (higher = more important)

        Returns:
            PublishResult with publish outcome
        """
        payload = {
            "extraction_id": extraction_id,
            "priority": priority,
        }

        # Use extraction_id as Nats-Msg-Id for deduplication
        headers = {"Nats-Msg-Id": extraction_id}

        result = await self.publish(
            subject=QueueSubject.EXTRACTION.value,
            payload=payload,
            headers=headers,
        )

        if result.success:
            logger.debug(
                f"Published extraction {extraction_id} to stream={result.stream}, "
                f"seq={result.sequence}"
            )
        else:
            logger.error(
                f"Failed to publish extraction {extraction_id}: {result.error}"
            )

        return result

    # publish_batch is inherited from BaseProducer


class ProducerFactory:
    """Factory for creating NatsProducer instances."""

    def __init__(
        self,
        nats_config: NatsConfig,
        stream_settings: StreamSettings | None = None,
    ):
        self.nats_config = nats_config
        self.stream_settings = stream_settings or StreamSettings()
        self._nats_client: NATS | None = None
        self._jetstream: JetStreamContext | None = None

    async def connect(self) -> None:
        """Connect to NATS."""
        import nats

        self._nats_client = await nats.connect(
            self.nats_config.url,
            connect_timeout=self.nats_config.connect_timeout,
            reconnect_time_wait=self.nats_config.reconnect_time_wait,
            max_reconnect_attempts=self.nats_config.max_reconnect_attempts,
        )
        self._jetstream = self._nats_client.jetstream()
        logger.info("Producer connected to NATS")

    async def close(self) -> None:
        """Close NATS connection."""
        if self._nats_client:
            await self._nats_client.close()
            logger.info("Producer disconnected from NATS")

    def create_producer(self) -> NatsProducer:
        """Create a new producer instance."""
        if not self._nats_client or not self._jetstream:
            raise RuntimeError("Not connected. Call connect() first.")
        return NatsProducer(
            self._nats_client,
            self._jetstream,
            self.stream_settings,
        )

    async def __aenter__(self) -> "ProducerFactory":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
