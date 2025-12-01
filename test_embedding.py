"""
Test script for embedding creation and saving functionality.

This script tests:
1. Fetching an existing LLMExtraction with extraction_result and summaries
2. Generating embeddings using Gemini embedding-001
3. Saving embeddings to the database
4. Verifying the embeddings were saved correctly

Usage:
    uv run python test_embedding.py [extraction_id]

If no extraction_id is provided, uses the most recent completed extraction.
"""

import asyncio
import logging
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from settings import get_settings
from src.extraction import LLMExtraction, ExtractionStatus
from src.embedding import (
    generate_embedding,
    generate_all_embeddings,
    save_extraction_embeddings,
    prepare_content_text,
    ensure_pgvector_extension,
    get_embedding_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def get_test_extraction(
    session: AsyncSession, extraction_id: str | None = None
) -> LLMExtraction | None:
    """
    Fetch an extraction for testing.

    If extraction_id provided, fetch that specific one.
    Otherwise, fetch the most recent completed extraction with summaries.
    """
    if extraction_id:
        result = await session.execute(
            select(LLMExtraction).where(
                LLMExtraction.extraction_id == extraction_id
            )
        )
        return result.scalar_one_or_none()

    # Get most recent completed extraction with summaries
    result = await session.execute(
        select(LLMExtraction)
        .where(
            LLMExtraction.status == ExtractionStatus.COMPLETED.value,
            LLMExtraction.extraction_result.isnot(None),
        )
        .order_by(LLMExtraction.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def verify_embeddings(
    session: AsyncSession, extraction_id: str
) -> dict:
    """Verify embeddings were saved correctly."""
    # Use vector_dims() function to get dimensions (pgvector native function)
    result = await session.execute(
        text("""
            SELECT
                extraction_id,
                embedding_generated,
                content_embedding IS NOT NULL as has_content_emb,
                summary_embedding_id IS NOT NULL as has_summary_id_emb,
                summary_embedding_en IS NOT NULL as has_summary_en_emb,
                COALESCE(vector_dims(content_embedding), 0) as content_dims,
                COALESCE(vector_dims(summary_embedding_id), 0) as summary_id_dims,
                COALESCE(vector_dims(summary_embedding_en), 0) as summary_en_dims
            FROM llm_extractions
            WHERE extraction_id = :extraction_id
        """),
        {"extraction_id": extraction_id}
    )
    row = result.fetchone()
    if row:
        return {
            "extraction_id": row.extraction_id,
            "embedding_generated": row.embedding_generated,
            "has_content_emb": row.has_content_emb,
            "has_summary_id_emb": row.has_summary_id_emb,
            "has_summary_en_emb": row.has_summary_en_emb,
            "content_dims": row.content_dims,
            "summary_id_dims": row.summary_id_dims,
            "summary_en_dims": row.summary_en_dims,
        }
    return {}


async def test_single_embedding():
    """Test generating a single embedding."""
    logger.info("=" * 60)
    logger.info("TEST 1: Single Embedding Generation")
    logger.info("=" * 60)

    test_text = "Terdakwa terbukti bersalah melakukan tindak pidana korupsi"
    logger.info(f"Test text: {test_text}")

    config = get_embedding_config()
    logger.info(f"Using model: {config.model}, dims: {config.dimensions}")

    embedding = await generate_embedding(test_text, config)

    logger.info(f"✓ Generated embedding with {len(embedding)} dimensions")
    logger.info(f"  First 5 values: {embedding[:5]}")
    logger.info(f"  Last 5 values: {embedding[-5:]}")

    # Verify normalization (L2 norm should be ~1)
    import numpy as np
    norm = np.linalg.norm(embedding)
    logger.info(f"  L2 norm: {norm:.6f} (should be ~1.0)")

    return True


async def test_full_embedding_pipeline(extraction_id: str | None = None):
    """Test the full embedding generation and saving pipeline."""
    logger.info("=" * 60)
    logger.info("TEST 2: Full Embedding Pipeline")
    logger.info("=" * 60)

    settings = get_settings()
    db_url = settings.get_database_url()
    connect_args = settings.get_connect_args()

    logger.info(f"Connecting to database: {db_url.host or 'unix socket'}")

    engine = create_async_engine(
        db_url,
        connect_args=connect_args,
        echo=False,
    )

    async_session = async_sessionmaker(bind=engine, class_=AsyncSession)

    try:
        async with async_session() as session:
            # Ensure pgvector is enabled
            logger.info("Ensuring pgvector extension is enabled...")
            await ensure_pgvector_extension(engine)

            # Fetch test extraction
            logger.info("Fetching test extraction...")
            extraction = await get_test_extraction(session, extraction_id)

            if not extraction:
                logger.error("No suitable extraction found for testing!")
                logger.error("Need a completed extraction with extraction_result")
                return False

            logger.info(f"Found extraction: {extraction.extraction_id}")
            logger.info(f"  Status: {extraction.status}")
            logger.info(f"  Has extraction_result: {extraction.extraction_result is not None}")
            logger.info(f"  Has summary_id: {extraction.summary_id is not None}")
            logger.info(f"  Has summary_en: {extraction.summary_en is not None}")
            logger.info(f"  Embedding generated: {extraction.embedding_generated}")

        # Prepare content text
        logger.info("\nPreparing content text for embedding...")
        content_text = prepare_content_text(extraction.extraction_result or {})
        logger.info(f"  Content text length: {len(content_text)} chars")
        if content_text:
            logger.info(f"  Preview: {content_text[:200]}...")

        # Generate all embeddings
        logger.info("\nGenerating all embeddings...")
        content_emb, summary_id_emb, summary_en_emb = await generate_all_embeddings(
            extraction_result=extraction.extraction_result or {},
            summary_id=extraction.summary_id,
            summary_en=extraction.summary_en,
        )

        logger.info(f"✓ Content embedding: {len(content_emb)} dims")
        logger.info(f"✓ Summary ID embedding: {len(summary_id_emb)} dims")
        logger.info(f"✓ Summary EN embedding: {len(summary_en_emb)} dims")

        # Save embeddings
        logger.info("\nSaving embeddings to database...")
        await save_extraction_embeddings(
            db_engine=engine,
            extraction_id=extraction.extraction_id,
            content_embedding=content_emb,
            summary_embedding_id=summary_id_emb,
            summary_embedding_en=summary_en_emb,
        )
        logger.info("✓ Embeddings saved successfully")

        # Verify
        logger.info("\nVerifying saved embeddings...")
        async with async_session() as session:
            verification = await verify_embeddings(session, extraction.extraction_id)

        logger.info(f"✓ Verification results:")
        for key, value in verification.items():
            logger.info(f"    {key}: {value}")

        # Final check
        success = (
            verification.get("embedding_generated", False)
            and verification.get("content_dims", 0) == 768
        )

        if success:
            logger.info("\n" + "=" * 60)
            logger.info("✅ ALL TESTS PASSED!")
            logger.info("=" * 60)
        else:
            logger.error("\n❌ Some verification checks failed")

        return success

    finally:
        await engine.dispose()


async def main():
    """Run all embedding tests."""
    extraction_id = sys.argv[1] if len(sys.argv) > 1 else None

    logger.info("Starting embedding functionality tests...")
    logger.info(f"Extraction ID: {extraction_id or 'auto-select most recent'}")

    # Test 1: Single embedding
    try:
        await test_single_embedding()
    except Exception as e:
        logger.exception(f"Single embedding test failed: {e}")
        return 1

    # Test 2: Full pipeline
    try:
        success = await test_full_embedding_pipeline(extraction_id)
        return 0 if success else 1
    except Exception as e:
        logger.exception(f"Full pipeline test failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
