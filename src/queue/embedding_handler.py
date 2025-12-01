"""Embedding message handler implementation.

Handles embedding generation jobs from the queue for AI Agent council.

Embeddings are stored directly in llm_extractions table:
- content_embedding: Structured extraction data (768 dims)
- summary_embedding_id: Indonesian summary (768 dims)
- summary_embedding_en: English summary (768 dims)
- embedding_generated: Boolean flag indicating completion
"""

import asyncio
import logging
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.embedding import (
    generate_all_embeddings,
    save_extraction_embeddings,
)
from src.extraction import ExtractionStatus, LLMExtraction

from .errors import PermanentError, RetriableError, SkipMessageError
from .handler import MessageContext, MessageHandler

logger = logging.getLogger(__name__)


class EmbeddingPayload(BaseModel):
    """Message payload for embedding jobs."""

    extraction_id: str = Field(..., description="ID of the extraction to embed")
    force: bool = Field(
        default=False, description="Force re-generation even if exists"
    )


class EmbeddingHandler(MessageHandler[EmbeddingPayload]):
    """
    Handler for embedding generation jobs.

    Processes completed extractions to generate vector embeddings:
    1. Validates extraction exists and is completed
    2. Checks if embedding already exists (skip if not forcing)
    3. Generates content + summary embeddings (ID and EN)
    4. Saves embeddings to llm_extractions table
    """

    def __init__(self, crawler_db_engine: AsyncEngine):
        super().__init__(EmbeddingPayload)
        self.crawler_db_engine = crawler_db_engine

    async def validate(
        self, payload: EmbeddingPayload, context: MessageContext
    ) -> None:
        """
        Validate the extraction before generating embeddings.

        Checks:
        1. LLM extraction exists and is completed
        2. Extraction has result data
        3. Embedding doesn't already exist (unless force=True)
        """
        extraction_id = payload.extraction_id
        logger.info(f"Validating embedding job for {extraction_id}")

        async_session = async_sessionmaker(
            bind=self.crawler_db_engine, class_=AsyncSession
        )

        async with async_session() as session:
            # Check if extraction exists and is completed
            result = await session.execute(
                select(LLMExtraction).where(
                    LLMExtraction.extraction_id == extraction_id
                )
            )
            extraction = result.scalar_one_or_none()

            if not extraction:
                logger.warning(f"Extraction {extraction_id} not found")
                raise SkipMessageError(
                    message=f"Extraction {extraction_id} not found",
                    reason="extraction_not_found",
                )

            if extraction.status != ExtractionStatus.COMPLETED.value:
                logger.warning(
                    f"Extraction {extraction_id} not completed "
                    f"(status: {extraction.status})"
                )
                raise SkipMessageError(
                    message=f"Extraction {extraction_id} not completed",
                    reason="extraction_not_completed",
                )

            if not extraction.extraction_result:
                logger.warning(f"Extraction {extraction_id} has no result data")
                raise SkipMessageError(
                    message=f"Extraction {extraction_id} has no result data",
                    reason="no_extraction_result",
                )

            # Check if embedding already exists (using flag in llm_extractions)
            if not payload.force and extraction.embedding_generated:
                logger.info(
                    f"Embedding already exists for {extraction_id}, skipping"
                )
                raise SkipMessageError(
                    message=f"Embedding already exists for {extraction_id}",
                    reason="already_embedded",
                )

        logger.info(f"Validation passed for embedding {extraction_id}")

    async def process(
        self, payload: EmbeddingPayload, context: MessageContext
    ) -> dict[str, Any]:
        """
        Generate all embeddings for the extraction.
        """
        extraction_id = payload.extraction_id
        logger.info(f"Generating embeddings for {extraction_id}")

        async_session = async_sessionmaker(
            bind=self.crawler_db_engine, class_=AsyncSession
        )

        try:
            start_time = asyncio.get_event_loop().time()

            # Fetch extraction data
            async with async_session() as session:
                result = await session.execute(
                    select(LLMExtraction).where(
                        LLMExtraction.extraction_id == extraction_id
                    )
                )
                extraction = result.scalar_one()

            # Generate all embeddings (content + summary_id + summary_en)
            content_emb, summary_emb_id, summary_emb_en = await generate_all_embeddings(
                extraction_result=extraction.extraction_result,
                summary_id=extraction.summary_id,
                summary_en=extraction.summary_en,
            )

            # Save to database (updates llm_extractions directly)
            await save_extraction_embeddings(
                db_engine=self.crawler_db_engine,
                extraction_id=extraction_id,
                content_embedding=content_emb,
                summary_embedding_id=summary_emb_id,
                summary_embedding_en=summary_emb_en,
            )

            duration = asyncio.get_event_loop().time() - start_time

            return {
                "extraction_id": extraction_id,
                "content_embedding_dims": len(content_emb),
                "summary_id_embedding_dims": len(summary_emb_id),
                "summary_en_embedding_dims": len(summary_emb_en),
                "processing_time": duration,
            }

        except Exception as e:
            logger.exception(f"Embedding generation failed for {extraction_id}: {e}")

            error_str = str(e).lower()

            # Retriable errors (API rate limits, timeouts)
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
                    "resource exhausted",
                ]
            ):
                raise RetriableError(
                    message=f"Transient error during embedding: {e}",
                    original_error=e,
                )

            # Permanent errors
            raise PermanentError(
                message=f"Embedding generation failed: {e}",
                original_error=e,
                should_update_status=False,
            )

    async def on_success(
        self,
        payload: EmbeddingPayload,
        context: MessageContext,
        result: dict[str, Any],
    ) -> None:
        """Log success."""
        logger.info(
            f"Embedding {payload.extraction_id} completed: "
            f"content={result.get('content_embedding_dims')}, "
            f"id={result.get('summary_id_embedding_dims')}, "
            f"en={result.get('summary_en_embedding_dims')}, "
            f"time={result.get('processing_time', 0):.2f}s"
        )

    async def on_failure(
        self,
        payload: EmbeddingPayload | None,
        context: MessageContext,
        error: Exception,
    ) -> None:
        """Log failure."""
        if payload:
            logger.error(
                f"Embedding {payload.extraction_id} failed: {error} "
                f"(delivery {context.delivery_count})"
            )
