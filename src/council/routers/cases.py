"""
Case search and retrieval endpoints for the Virtual Judicial Council.

Provides endpoints for:
- Searching cases (semantic and text-based)
- Retrieving case details
- Getting case statistics
"""

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.council.database import CaseDatabase
from src.council.schemas import (
    CaseStatisticsResponse,
    CaseType,
    GetCaseResponse,
    SearchCasesRequest,
    SearchCasesResponse,
)
from src.extraction import LLMExtraction

logger = logging.getLogger(__name__)

router = APIRouter()


# Dependency to get database engine
async def get_db_engine() -> AsyncEngine:
    """Get database engine - must be set up by main app."""
    from src.council.routers.sessions import _db_engine

    if _db_engine is None:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Database not initialized",
        )
    return _db_engine


@router.post("/search", response_model=SearchCasesResponse)
async def search_cases(
    request: SearchCasesRequest,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> SearchCasesResponse:
    """
    Search for cases.

    Supports both semantic search (using embeddings) and text search.
    Semantic search finds conceptually similar cases even if exact
    keywords don't match.

    Optional filters can be applied based on structured case data.
    """
    logger.info(
        f"Searching cases: query='{request.query[:50]}...', "
        f"semantic={request.semantic_search}"
    )

    case_db = CaseDatabase(db_engine)

    # Convert filters if provided
    filters = None
    if request.filters:
        filters = request.filters.model_dump(exclude_none=True)

    cases = await case_db.search_cases(
        query=request.query,
        limit=request.limit,
        semantic_search=request.semantic_search,
        filters=filters,
    )

    return SearchCasesResponse(
        cases=cases,
        total=len(cases),
    )


@router.get("/search", response_model=SearchCasesResponse)
async def search_cases_get(
    query: str,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
    limit: int = Query(default=10, ge=1, le=100),
    semantic: bool = Query(default=True),
    case_type: CaseType | None = Query(default=None),
) -> SearchCasesResponse:
    """
    Search for cases (GET version).

    Simpler version of case search using query parameters.
    """
    logger.info(f"Searching cases (GET): query='{query[:50]}...'")

    case_db = CaseDatabase(db_engine)

    filters = None
    if case_type:
        filters = {"case_type": case_type.value}

    cases = await case_db.search_cases(
        query=query,
        limit=limit,
        semantic_search=semantic,
        filters=filters,
    )

    return SearchCasesResponse(
        cases=cases,
        total=len(cases),
    )


@router.get("/{case_id}", response_model=GetCaseResponse)
async def get_case(
    case_id: str,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> GetCaseResponse:
    """
    Get a case by ID.

    Returns the full case record including extraction results,
    summaries, and verdict information.
    """
    logger.info(f"Getting case: {case_id}")

    case_db = CaseDatabase(db_engine)
    case = await case_db.get_case(case_id)

    if not case:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Case not found: {case_id}",
        )

    return GetCaseResponse(case=case)


@router.get("/statistics/summary", response_model=CaseStatisticsResponse)
async def get_case_statistics(
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> CaseStatisticsResponse:
    """
    Get aggregate statistics about cases in the database.

    Returns:
    - Total number of cases with embeddings
    - Sentence distribution by crime category
    - Verdict distribution
    """
    logger.info("Getting case statistics")

    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    async with async_session() as session:
        # Total cases with embeddings
        total_result = await session.execute(
            select(func.count())
            .select_from(LLMExtraction)
            .where(
                LLMExtraction.embedding_generated == True,  # noqa: E712
                LLMExtraction.status == "completed",
            )
        )
        total_cases = total_result.scalar_one()

        # Sentence distribution by crime category
        sentence_sql = """
        SELECT
            COALESCE(
                extraction_result->'case_metadata'->>'crime_category',
                extraction_result->>'crime_category',
                'Unknown'
            ) as category,
            AVG(
                COALESCE(
                    (extraction_result->'verdict'->'sentences'->'imprisonment'->>'duration_months')::float,
                    (extraction_result->'verdict'->'sentences'->'imprisonment'->>'duration_years')::float * 12,
                    0
                )
            ) as avg_months,
            COUNT(*) as case_count
        FROM llm_extractions
        WHERE status = 'completed'
        AND extraction_result IS NOT NULL
        GROUP BY category
        ORDER BY case_count DESC
        LIMIT 10
        """
        sentence_result = await session.execute(text(sentence_sql))
        sentence_rows = sentence_result.fetchall()

        sentence_distribution = {
            row.category: {
                "avg_months": round(row.avg_months or 0, 1),
                "count": row.case_count,
            }
            for row in sentence_rows
        }

        # Verdict distribution
        verdict_sql = """
        SELECT
            COALESCE(
                extraction_result->'verdict'->>'result',
                'Unknown'
            ) as verdict_result,
            COUNT(*) as count
        FROM llm_extractions
        WHERE status = 'completed'
        AND extraction_result IS NOT NULL
        GROUP BY verdict_result
        ORDER BY count DESC
        """
        verdict_result = await session.execute(text(verdict_sql))
        verdict_rows = verdict_result.fetchall()

        verdict_distribution = {
            row.verdict_result: row.count for row in verdict_rows
        }

    return CaseStatisticsResponse(
        total_cases=total_cases,
        sentence_distribution=sentence_distribution,
        verdict_distribution=verdict_distribution,
    )


@router.get("/by-type/{case_type}", response_model=SearchCasesResponse)
async def get_cases_by_type(
    case_type: CaseType,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> SearchCasesResponse:
    """
    Get cases filtered by case type.

    A convenience endpoint for filtering by narcotics, corruption,
    or other case types.
    """
    logger.info(f"Getting cases by type: {case_type.value}")

    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    # Map case type to search patterns
    type_patterns = {
        CaseType.NARCOTICS: ["%narkotika%", "%narcotics%"],
        CaseType.CORRUPTION: ["%korupsi%", "%corruption%"],
        CaseType.GENERAL_CRIMINAL: [],  # Will use NOT IN for others
        CaseType.OTHER: [],
    }

    patterns = type_patterns.get(case_type, [])

    async with async_session() as session:
        if case_type in [CaseType.NARCOTICS, CaseType.CORRUPTION]:
            # Search for specific types
            pattern = patterns[0] if patterns else f"%{case_type.value}%"
            sql = """
            SELECT
                extraction_id,
                summary_id,
                summary_en,
                extraction_result,
                1.0 as similarity
            FROM llm_extractions
            WHERE status = 'completed'
            AND (
                extraction_result->'case_metadata'->>'crime_category' ILIKE :pattern
                OR extraction_result->>'crime_category' ILIKE :pattern
            )
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
            """
            result = await session.execute(
                text(sql),
                {"pattern": pattern, "limit": limit, "offset": offset},
            )
        else:
            # Get all cases for general/other
            sql = """
            SELECT
                extraction_id,
                summary_id,
                summary_en,
                extraction_result,
                1.0 as similarity
            FROM llm_extractions
            WHERE status = 'completed'
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
            """
            result = await session.execute(
                text(sql),
                {"limit": limit, "offset": offset},
            )

        rows = result.fetchall()

    # Convert to case records
    case_db = CaseDatabase(db_engine)
    cases = [
        case_db._row_to_case_record(row)
        for row in rows
        if case_db._row_to_case_record(row)
    ]

    return SearchCasesResponse(
        cases=cases,
        total=len(cases),
    )
