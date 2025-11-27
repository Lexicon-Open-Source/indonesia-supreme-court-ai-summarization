"""Extraction message handler implementation.

Handles extraction job messages from the NATS queue.
"""

import asyncio
import logging
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.extraction import ExtractionStatus, LLMExtraction
from src.io import Extraction
from src.pipeline import run_extraction_pipeline

from .errors import PermanentError, RetriableError, SkipMessageError
from .handler import MessageContext, MessageHandler

logger = logging.getLogger(__name__)


class ExtractionPayload(BaseModel):
    """Message payload for extraction jobs."""

    extraction_id: str = Field(..., description="ID of the extraction to process")
    priority: int = Field(default=0, description="Job priority (higher = important)")


class ExtractionHandler(MessageHandler[ExtractionPayload]):
    """
    Handler for extraction job messages.

    Processes PDF documents through the extraction pipeline:
    1. Validates extraction exists and is ready for processing
    2. Updates status to PROCESSING
    3. Runs extraction pipeline
    4. Updates status to COMPLETED/FAILED
    """

    def __init__(
        self,
        crawler_db_engine: AsyncEngine,
    ):
        super().__init__(ExtractionPayload)
        self.crawler_db_engine = crawler_db_engine

    async def validate(
        self, payload: ExtractionPayload, context: MessageContext
    ) -> None:
        """
        Validate the extraction before processing.

        Checks:
        1. Extraction record exists in source table
        2. Extraction has artifact_link (PDF available)
        3. Extraction has valid raw_page_link
        4. Extraction hasn't already been completed (idempotency)
        """
        extraction_id = payload.extraction_id
        logger.info(f"Validating extraction {extraction_id}")
        async_session = async_sessionmaker(
            bind=self.crawler_db_engine, class_=AsyncSession
        )

        async with async_session() as session:
            # Check if extraction exists
            result = await session.execute(
                select(Extraction).where(Extraction.id == extraction_id)
            )
            extraction_record = result.scalar_one_or_none()

            if not extraction_record:
                logger.warning(f"Extraction {extraction_id} not found in source table")
                raise SkipMessageError(
                    message=f"Extraction {extraction_id} not found",
                    reason="source_not_found",
                )

            logger.debug(
                f"Found extraction {extraction_id}: "
                f"artifact_link={extraction_record.artifact_link}, "
                f"raw_page_link={extraction_record.raw_page_link}"
            )

            if not extraction_record.artifact_link:
                logger.warning(f"Extraction {extraction_id} has no artifact_link")
                raise SkipMessageError(
                    message=f"Extraction {extraction_id} has no artifact_link",
                    reason="no_pdf_available",
                )

            if not extraction_record.raw_page_link.startswith("https://putusan3"):
                logger.warning(
                    f"Extraction {extraction_id} has invalid raw_page_link: "
                    f"{extraction_record.raw_page_link}"
                )
                raise SkipMessageError(
                    message=f"Extraction {extraction_id} has invalid raw_page_link",
                    reason="invalid_source",
                )

        # Atomically claim the job by updating status to PROCESSING
        # Only succeeds if current status is PENDING or FAILED
        async with async_session() as session:
            # First check current status
            result = await session.execute(
                select(LLMExtraction).where(
                    LLMExtraction.extraction_id == extraction_id
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                if existing.status == ExtractionStatus.COMPLETED.value:
                    raise SkipMessageError(
                        message=f"Extraction {extraction_id} already completed",
                        reason="already_completed",
                    )
                elif existing.status == ExtractionStatus.PROCESSING.value:
                    raise SkipMessageError(
                        message=f"Extraction {extraction_id} already being processed",
                        reason="already_processing",
                    )

                # Atomically claim: update only if status is still PENDING/FAILED
                stmt = (
                    update(LLMExtraction)
                    .where(LLMExtraction.extraction_id == extraction_id)
                    .where(
                        LLMExtraction.status.in_([
                            ExtractionStatus.PENDING.value,
                            ExtractionStatus.FAILED.value,
                        ])
                    )
                    .values(status=ExtractionStatus.PROCESSING.value)
                )
                result = await session.execute(stmt)
                await session.commit()

                # If no rows updated, another worker claimed it
                if result.rowcount == 0:
                    raise SkipMessageError(
                        message=f"Extraction {extraction_id} claimed by another worker",
                        reason="claimed_by_other",
                    )

                logger.info(f"Claimed extraction {extraction_id} for processing")
            else:
                # No existing record - create one with PROCESSING status
                new_record = LLMExtraction(
                    extraction_id=extraction_id,
                    status=ExtractionStatus.PROCESSING.value,
                )
                session.add(new_record)
                await session.commit()
                logger.info(f"Created PROCESSING record for {extraction_id}")

        logger.info(f"Validation passed for {extraction_id}")

    async def process(
        self, payload: ExtractionPayload, context: MessageContext
    ) -> dict[str, Any]:
        """
        Run the extraction pipeline.

        Status is already set to PROCESSING in validate() via atomic claim.
        """
        extraction_id = payload.extraction_id
        logger.info(f"Starting extraction pipeline for {extraction_id}")

        try:
            start_time = asyncio.get_event_loop().time()
            logger.info(f"Calling run_extraction_pipeline for {extraction_id}")

            (
                extraction_result,
                summary_id,
                summary_en,
                decision_number,
            ) = await run_extraction_pipeline(
                extraction_id=extraction_id,
                crawler_db_engine=self.crawler_db_engine,
            )

            duration = asyncio.get_event_loop().time() - start_time

            return {
                "extraction_id": extraction_id,
                "decision_number": decision_number,
                "fields_extracted": len(
                    extraction_result.model_dump(exclude_none=True)
                ),
                "has_summary_id": summary_id is not None,
                "has_summary_en": summary_en is not None,
                "processing_time": duration,
            }

        except Exception as e:
            logger.exception(f"Pipeline failed for {extraction_id}: {e}")

            # Categorize the error
            error_str = str(e).lower()

            # Retriable errors (transient failures)
            if any(
                term in error_str
                for term in [
                    "timeout",
                    "connection",
                    "rate limit",
                    "quota",
                    "503",
                    "502",
                    "504",
                    "temporarily unavailable",
                ]
            ):
                raise RetriableError(
                    message=f"Transient error during extraction: {e}",
                    original_error=e,
                )

            # Permanent errors (won't succeed on retry)
            raise PermanentError(
                message=f"Extraction failed: {e}",
                original_error=e,
                should_update_status=True,
            )

    async def on_success(
        self,
        payload: ExtractionPayload,
        context: MessageContext,
        result: dict[str, Any],
    ) -> None:
        """Update status to COMPLETED on success."""
        # Status is already updated by the pipeline, but we can log success
        logger.info(
            f"Extraction {payload.extraction_id} completed successfully: "
            f"decision={result.get('decision_number')}, "
            f"fields={result.get('fields_extracted')}, "
            f"time={result.get('processing_time', 0):.2f}s"
        )

    async def on_failure(
        self,
        payload: ExtractionPayload | None,
        context: MessageContext,
        error: Exception,
    ) -> None:
        """Update status to FAILED on permanent failure."""
        if payload is None:
            return

        if isinstance(error, PermanentError) and error.should_update_status:
            await self._update_status(
                payload.extraction_id,
                ExtractionStatus.FAILED,
                error_message=str(error),
            )
        elif isinstance(error, RetriableError):
            # Keep status as PROCESSING for retriable errors
            # so we know it's being worked on
            logger.info(
                f"Extraction {payload.extraction_id} will be retried "
                f"(delivery {context.delivery_count})"
            )

    async def _update_status(
        self,
        extraction_id: str,
        status: ExtractionStatus,
        error_message: str | None = None,
    ) -> None:
        """Update the extraction status in the database."""
        async_session = async_sessionmaker(
            bind=self.crawler_db_engine, class_=AsyncSession
        )

        async with async_session() as session:
            result = await session.execute(
                select(LLMExtraction).where(
                    LLMExtraction.extraction_id == extraction_id
                )
            )
            record = result.scalar_one_or_none()

            if record:
                record.status = status.value
                if error_message:
                    # Store error in extraction_result if failed
                    record.extraction_result = record.extraction_result or {}
                    record.extraction_result["_error"] = error_message
                session.add(record)
                await session.commit()
                logger.debug(f"Updated {extraction_id} status to {status.value}")
            else:
                # Create new record if doesn't exist
                new_record = LLMExtraction(
                    extraction_id=extraction_id,
                    status=status.value,
                )
                if error_message:
                    new_record.extraction_result = {"_error": error_message}
                session.add(new_record)
                await session.commit()
                logger.debug(f"Created {extraction_id} with status {status.value}")
