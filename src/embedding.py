"""
Embedding module for generating vector embeddings from court case extractions.

This module provides:
1. Embedding generation using Gemini embedding-001
2. Storage of embeddings in llm_extractions table
3. Semantic search functionality for AI Agent council

Three embedding types for comprehensive search:
- content_embedding: Structured extraction data (crime, defendant, verdict, etc.)
- summary_embedding_id: Indonesian summary for narrative search
- summary_embedding_en: English summary for international queries

For structured queries (crime_category, verdict, etc.), use JSONB queries
on extraction_result column for exact filtering.
"""

import asyncio
import logging
from typing import Any

import numpy as np
from litellm import aembedding
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from tenacity import retry, stop_after_attempt, wait_exponential

from settings import get_settings
from src.extraction import LLMExtraction

logger = logging.getLogger(__name__)


# =============================================================================
# Embedding Configuration
# =============================================================================


class EmbeddingConfig(BaseModel):
    """Configuration for embedding generation."""

    model: str = "gemini/gemini-embedding-001"
    dimensions: int = 768
    task_type: str = "RETRIEVAL_DOCUMENT"
    query_task_type: str = "RETRIEVAL_QUERY"
    max_input_tokens: int = 2048  # Gemini embedding limit
    normalize: bool = True  # Normalize embeddings for cosine similarity


def get_embedding_config() -> EmbeddingConfig:
    """Get embedding configuration from settings."""
    settings = get_settings()
    return EmbeddingConfig(
        model=settings.embedding_model,
        dimensions=settings.embedding_dimensions,
        task_type=settings.embedding_task_type,
    )


# =============================================================================
# Text Preparation
# =============================================================================


def prepare_content_text(extraction_result: dict[str, Any]) -> str:
    """
    Prepare structured text for content embedding from extraction result.

    Combines key fields that are most useful for semantic search:
    - Defendant information
    - Crime category and subcategory
    - Case chronology
    - Cited articles
    - Legal facts
    - State loss amount
    - Verdict information

    Args:
        extraction_result: The extraction result dictionary

    Returns:
        Combined text optimized for embedding (max ~8000 chars)
    """
    parts = []

    # Defendant info
    defendant = extraction_result.get("defendant", {}) or {}
    if defendant:
        defendant_parts = []
        if defendant.get("name"):
            defendant_parts.append(f"Terdakwa: {defendant['name']}")
        if defendant.get("occupation"):
            defendant_parts.append(f"Pekerjaan: {defendant['occupation']}")
        if defendant.get("education"):
            defendant_parts.append(f"Pendidikan: {defendant['education']}")
        if defendant_parts:
            parts.append(" | ".join(defendant_parts))

    # Case metadata
    case_meta = extraction_result.get("case_metadata", {}) or {}
    if case_meta:
        if case_meta.get("crime_category"):
            parts.append(f"Kategori: {case_meta['crime_category']}")
        if case_meta.get("crime_subcategory"):
            parts.append(f"Subkategori: {case_meta['crime_subcategory']}")
        if case_meta.get("institution_involved"):
            parts.append(f"Instansi: {case_meta['institution_involved']}")

    # Indictment chronology
    indictment = extraction_result.get("indictment", {}) or {}
    if indictment:
        if indictment.get("chronology"):
            # Truncate long chronology
            chronology = indictment["chronology"][:1500]
            parts.append(f"Kronologi: {chronology}")
        if indictment.get("crime_location"):
            parts.append(f"Lokasi: {indictment['crime_location']}")

        # Cited articles
        cited_articles = indictment.get("cited_articles", []) or []
        if cited_articles:
            articles_text = []
            for article in cited_articles[:5]:  # Limit to first 5
                if article and article.get("full_citation"):
                    articles_text.append(article["full_citation"])
                elif article and article.get("article"):
                    articles_text.append(article["article"])
            if articles_text:
                parts.append(f"Pasal: {', '.join(articles_text)}")

    # Legal facts (categorized)
    legal_facts = extraction_result.get("legal_facts", {}) or {}
    if legal_facts:
        fact_parts = []
        for key in ["violations", "financial_irregularities", "other_facts"]:
            facts = legal_facts.get(key, []) or []
            if facts:
                # Take first 2 facts from each category
                fact_parts.extend(facts[:2])
        if fact_parts:
            parts.append(f"Fakta hukum: {' | '.join(fact_parts[:5])}")

    # State loss
    state_loss = extraction_result.get("state_loss", {}) or {}
    if state_loss:
        if state_loss.get("proven_amount"):
            parts.append(f"Kerugian negara: Rp {state_loss['proven_amount']:,.0f}")

    # Judicial considerations (aggravating/mitigating factors)
    judicial = extraction_result.get("judicial_considerations", {}) or {}
    if judicial:
        aggravating = judicial.get("aggravating_factors", []) or []
        if aggravating:
            # Take first 3 aggravating factors
            parts.append(f"Hal memberatkan: {' | '.join(aggravating[:3])}")

        mitigating = judicial.get("mitigating_factors", []) or []
        if mitigating:
            # Take first 3 mitigating factors
            parts.append(f"Hal meringankan: {' | '.join(mitigating[:3])}")

    # Verdict
    verdict = extraction_result.get("verdict", {}) or {}
    if verdict:
        if verdict.get("result"):
            result_map = {
                "guilty": "Terbukti bersalah",
                "not_guilty": "Bebas",
                "acquitted": "Lepas dari tuntutan",
            }
            parts.append(
                f"Putusan: {result_map.get(verdict['result'], verdict['result'])}"
            )

        sentences = verdict.get("sentences", {}) or {}
        if sentences:
            imprisonment = sentences.get("imprisonment", {}) or {}
            if imprisonment and imprisonment.get("description"):
                parts.append(f"Hukuman: {imprisonment['description']}")

            fine = sentences.get("fine", {}) or {}
            if fine and fine.get("amount"):
                parts.append(f"Denda: Rp {fine['amount']:,.0f}")

    # Combine all parts
    combined_text = " | ".join(parts)

    # Truncate if too long (rough estimate: 1 token ~ 4 chars for Indonesian)
    max_chars = 8000  # Conservative limit for 2048 tokens
    if len(combined_text) > max_chars:
        combined_text = combined_text[:max_chars] + "..."

    return combined_text


# =============================================================================
# Embedding Generation
# =============================================================================


def normalize_embedding(embedding: list[float]) -> list[float]:
    """
    Normalize embedding vector for cosine similarity.

    Required when using dimensions < 3072 with gemini-embedding-001.

    Args:
        embedding: Raw embedding vector

    Returns:
        Normalized embedding vector
    """
    arr = np.array(embedding)
    norm = np.linalg.norm(arr)
    if norm > 0:
        arr = arr / norm
    return arr.tolist()


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def generate_embedding(
    text: str,
    config: EmbeddingConfig | None = None,
    task_type: str | None = None,
) -> list[float]:
    """
    Generate embedding for a single text using Gemini embedding-001.

    Args:
        text: Text to embed
        config: Embedding configuration (uses default if not provided)
        task_type: Override task type (e.g., RETRIEVAL_QUERY for searches)

    Returns:
        Embedding vector as list of floats
    """
    if not config:
        config = get_embedding_config()

    effective_task_type = task_type or config.task_type

    logger.debug(
        f"Generating embedding: model={config.model}, "
        f"dims={config.dimensions}, task={effective_task_type}"
    )

    response = await aembedding(
        model=config.model,
        input=[text],
        dimensions=config.dimensions,
        task_type=effective_task_type,
    )

    embedding = response.data[0]["embedding"]

    # Normalize if dimensions < 3072
    if config.normalize and config.dimensions < 3072:
        embedding = normalize_embedding(embedding)

    logger.debug(f"Generated embedding with {len(embedding)} dimensions")
    return embedding


async def generate_query_embedding(
    query: str,
    config: EmbeddingConfig | None = None,
) -> list[float]:
    """
    Generate embedding for a search query.

    Uses RETRIEVAL_QUERY task type for optimal search performance.

    Args:
        query: Search query text
        config: Embedding configuration

    Returns:
        Query embedding vector
    """
    if not config:
        config = get_embedding_config()

    return await generate_embedding(
        text=query,
        config=config,
        task_type=config.query_task_type,
    )


# =============================================================================
# Full Embedding Generation (all 3 types)
# =============================================================================


async def generate_all_embeddings(
    extraction_result: dict[str, Any],
    summary_id: str | None = None,
    summary_en: str | None = None,
    config: EmbeddingConfig | None = None,
) -> tuple[list[float], list[float], list[float]]:
    """
    Generate all three embedding types for an extraction.

    Args:
        extraction_result: The extraction result dictionary
        summary_id: Indonesian summary
        summary_en: English summary
        config: Embedding configuration

    Returns:
        Tuple of (content_embedding, summary_embedding_id, summary_embedding_en)
    """
    if not config:
        config = get_embedding_config()

    # Prepare content text from extraction result
    content_text = prepare_content_text(extraction_result)

    logger.info(
        f"Generating embeddings: content={len(content_text)} chars, "
        f"summary_id={len(summary_id or '')} chars, "
        f"summary_en={len(summary_en or '')} chars"
    )

    # Build tasks for concurrent execution
    tasks: list = []
    task_labels: list[str] = []

    # Always generate content embedding
    if content_text:
        tasks.append(generate_embedding(content_text, config))
        task_labels.append("content")

    if summary_id:
        tasks.append(generate_embedding(summary_id, config))
        task_labels.append("id")

    if summary_en:
        tasks.append(generate_embedding(summary_en, config))
        task_labels.append("en")

    if not tasks:
        logger.warning("No text provided for embedding generation")
        return [], [], []

    # Generate embeddings concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Extract results
    content_embedding: list[float] = []
    summary_embedding_id: list[float] = []
    summary_embedding_en: list[float] = []

    for i, label in enumerate(task_labels):
        result = results[i]
        if isinstance(result, Exception):
            logger.error(f"Failed to generate {label} embedding: {result}")
            continue

        if label == "content":
            content_embedding = result
        elif label == "id":
            summary_embedding_id = result
        elif label == "en":
            summary_embedding_en = result

    logger.info(
        f"Generated embeddings: content={len(content_embedding)} dims, "
        f"id={len(summary_embedding_id)} dims, en={len(summary_embedding_en)} dims"
    )

    return content_embedding, summary_embedding_id, summary_embedding_en


# =============================================================================
# Semantic Search
# =============================================================================


class SearchResult(BaseModel):
    """Search result with similarity score."""

    extraction_id: str
    similarity: float
    crime_category: str | None = None
    crime_subcategory: str | None = None
    verdict_result: str | None = None
    summary_id: str | None = None
    summary_en: str | None = None


async def semantic_search(
    db_engine: AsyncEngine,
    query: str,
    limit: int = 10,
    crime_category: str | None = None,
    search_type: str = "content",  # "content", "summary_id", "summary_en"
    config: EmbeddingConfig | None = None,
) -> list[SearchResult]:
    """
    Perform semantic search across case embeddings.

    Uses vector similarity combined with optional JSONB filtering.

    Args:
        db_engine: Database engine
        query: Search query text
        limit: Maximum number of results
        crime_category: Optional filter by crime category (JSONB query)
        search_type: Which embedding to search against:
            - "content": Structured extraction data (default)
            - "summary_id": Indonesian summary
            - "summary_en": English summary
        config: Embedding configuration

    Returns:
        List of search results with similarity scores
    """
    if not config:
        config = get_embedding_config()

    # Generate query embedding
    query_embedding = await generate_query_embedding(query, config)
    query_vector = f"[{','.join(str(x) for x in query_embedding)}]"

    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    # Select embedding column based on search type
    embedding_col_map = {
        "content": "content_embedding",
        "summary_id": "summary_embedding_id",
        "summary_en": "summary_embedding_en",
    }
    embedding_col = embedding_col_map.get(search_type, "content_embedding")

    async with async_session() as session:
        # Build SQL query with cosine similarity
        # Note: We interpolate query_vector directly since it's a sanitized
        # float array string, and SQLAlchemy misinterprets ::vector cast
        sql = f"""
        SELECT
            extraction_id,
            summary_id,
            summary_en,
            extraction_result->>'crime_category' as crime_category,
            extraction_result->'case_metadata'->>'crime_subcategory'
                as crime_subcategory,
            extraction_result->'verdict'->>'result' as verdict_result,
            1 - ({embedding_col} <=> '{query_vector}'::vector) as similarity
        FROM llm_extractions
        WHERE {embedding_col} IS NOT NULL
        """

        params: dict[str, Any] = {}

        # Apply JSONB filters
        if crime_category:
            sql += """
            AND (
                extraction_result->>'crime_category' = :crime_category
                OR extraction_result->'case_metadata'->>'crime_category'
                    = :crime_category
            )
            """
            params["crime_category"] = crime_category

        # Order by similarity and limit
        sql += """
        ORDER BY similarity DESC
        LIMIT :limit
        """
        params["limit"] = limit

        result = await session.execute(text(sql), params)
        rows = result.fetchall()

        return [
            SearchResult(
                extraction_id=row.extraction_id,
                similarity=float(row.similarity),
                crime_category=row.crime_category,
                crime_subcategory=row.crime_subcategory,
                verdict_result=row.verdict_result,
                summary_id=row.summary_id,
                summary_en=row.summary_en,
            )
            for row in rows
        ]


async def find_similar_cases(
    db_engine: AsyncEngine,
    extraction_id: str,
    limit: int = 10,
    search_type: str = "content",
    config: EmbeddingConfig | None = None,
) -> list[SearchResult]:
    """
    Find cases similar to a given extraction.

    Args:
        db_engine: Database engine
        extraction_id: ID of the reference extraction
        limit: Maximum number of results
        search_type: Which embedding to use for similarity
        config: Embedding configuration

    Returns:
        List of similar cases with similarity scores
    """
    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    # Select embedding column based on search type
    embedding_col_map = {
        "content": "content_embedding",
        "summary_id": "summary_embedding_id",
        "summary_en": "summary_embedding_en",
    }
    embedding_col = embedding_col_map.get(search_type, "content_embedding")

    async with async_session() as session:
        # First get the reference embedding
        ref_sql = f"""
        SELECT {embedding_col}
        FROM llm_extractions
        WHERE extraction_id = :extraction_id
        """

        ref_result = await session.execute(
            text(ref_sql), {"extraction_id": extraction_id}
        )
        ref_row = ref_result.fetchone()

        if not ref_row or ref_row[0] is None:
            logger.warning(f"No embedding found for extraction: {extraction_id}")
            return []

        ref_embedding = ref_row[0]

        # Search for similar cases (excluding the reference)
        # Note: We interpolate ref_embedding directly since SQLAlchemy
        # misinterprets ::vector cast as a named parameter
        ref_emb_str = str(ref_embedding)
        sql = f"""
        SELECT
            extraction_id,
            summary_id,
            summary_en,
            extraction_result->>'crime_category' as crime_category,
            extraction_result->'case_metadata'->>'crime_subcategory'
                as crime_subcategory,
            extraction_result->'verdict'->>'result' as verdict_result,
            1 - ({embedding_col} <=> '{ref_emb_str}'::vector) as similarity
        FROM llm_extractions
        WHERE extraction_id != :extraction_id
        AND {embedding_col} IS NOT NULL
        ORDER BY similarity DESC
        LIMIT :limit
        """

        result = await session.execute(
            text(sql),
            {
                "extraction_id": extraction_id,
                "limit": limit,
            },
        )
        rows = result.fetchall()

        return [
            SearchResult(
                extraction_id=row.extraction_id,
                similarity=float(row.similarity),
                crime_category=row.crime_category,
                crime_subcategory=row.crime_subcategory,
                verdict_result=row.verdict_result,
                summary_id=row.summary_id,
                summary_en=row.summary_en,
            )
            for row in rows
        ]


# =============================================================================
# Database Operations
# =============================================================================


async def ensure_pgvector_extension(db_engine: AsyncEngine) -> None:
    """
    Ensure pgvector extension is enabled.

    Must be run before using embedding features.
    Note: Run the migration SQL to add embedding columns to llm_extractions.
    """
    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    async with async_session() as session:
        # Enable pgvector extension
        await session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await session.commit()
        logger.info("pgvector extension enabled")


async def save_extraction_embeddings(
    db_engine: AsyncEngine,
    extraction_id: str,
    content_embedding: list[float],
    summary_embedding_id: list[float],
    summary_embedding_en: list[float],
) -> None:
    """
    Save all embeddings directly to llm_extractions table.

    Args:
        db_engine: Database engine
        extraction_id: Extraction ID
        content_embedding: Content embedding vector
        summary_embedding_id: Indonesian summary embedding vector
        summary_embedding_en: English summary embedding vector
    """
    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    async with async_session() as session:
        # Convert lists to pgvector format string or NULL
        # Note: asyncpg misinterprets ::vector cast as named param due to colons
        # We directly interpolate the sanitized float array strings which are safe
        def to_vector_sql(embedding: list[float] | None) -> str:
            if not embedding:
                return "NULL"
            vec_str = f"[{','.join(str(x) for x in embedding)}]"
            return f"'{vec_str}'::vector"

        content_sql = to_vector_sql(content_embedding)
        summary_id_sql = to_vector_sql(summary_embedding_id)
        summary_en_sql = to_vector_sql(summary_embedding_en)

        sql = f"""
        UPDATE llm_extractions
        SET
            content_embedding = {content_sql},
            summary_embedding_id = {summary_id_sql},
            summary_embedding_en = {summary_en_sql},
            embedding_generated = TRUE
        WHERE extraction_id = :extraction_id
        """

        await session.execute(
            text(sql),
            {"extraction_id": extraction_id},
        )
        await session.commit()
        logger.info(f"Saved embeddings for extraction: {extraction_id}")


async def get_extractions_needing_embeddings(
    db_engine: AsyncEngine,
    limit: int = 100,
) -> list[str]:
    """
    Get extraction IDs that need embeddings generated.

    Args:
        db_engine: Database engine
        limit: Maximum number of IDs to return

    Returns:
        List of extraction_id values
    """
    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    async with async_session() as session:
        result = await session.execute(
            select(LLMExtraction.extraction_id)
            .where(
                LLMExtraction.embedding_generated == False,  # noqa: E712
                LLMExtraction.status == "completed",
            )
            .limit(limit)
        )
        return [row[0] for row in result.fetchall()]


# =============================================================================
# Legacy Compatibility (deprecated, will be removed)
# =============================================================================


async def generate_summary_embeddings(
    summary_id: str | None = None,
    summary_en: str | None = None,
    config: EmbeddingConfig | None = None,
) -> tuple[list[float], list[float]]:
    """
    Legacy function for backwards compatibility.

    DEPRECATED: Use generate_all_embeddings instead.
    """
    logger.warning(
        "generate_summary_embeddings is deprecated. "
        "Use generate_all_embeddings instead."
    )

    _, summary_emb_id, summary_emb_en = await generate_all_embeddings(
        extraction_result={},
        summary_id=summary_id,
        summary_en=summary_en,
        config=config,
    )

    return summary_emb_id, summary_emb_en
