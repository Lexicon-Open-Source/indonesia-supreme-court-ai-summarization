"""
Database operations for the Virtual Judicial Council.

Provides:
- Session storage and retrieval (in-memory with optional persistence)
- Similar case search using pgvector embeddings
- Case data access from llm_extractions table
"""

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.council.schemas import (
    CaseInput,
    CaseRecord,
    CaseType,
    DeliberationMessage,
    DeliberationSession,
    SessionStatus,
    SimilarCase,
)
from src.council.services.embeddings import (
    get_council_embedding_service,
)
from src.extraction import LLMExtraction

logger = logging.getLogger(__name__)


class SessionStore:
    """
    In-memory store for deliberation sessions.

    For production, this could be extended to persist sessions
    to a database table.
    """

    def __init__(self):
        """Initialize the session store."""
        self._sessions: dict[str, DeliberationSession] = {}
        logger.info("Session store initialized")

    def create_session(
        self,
        case_input: CaseInput,
        user_id: str | None = None,
    ) -> DeliberationSession:
        """
        Create a new deliberation session.

        Args:
            case_input: Parsed case information
            user_id: Optional user identifier

        Returns:
            New DeliberationSession
        """
        now = datetime.now(timezone.utc)
        session = DeliberationSession(
            id=str(uuid4()),
            user_id=user_id,
            status=SessionStatus.ACTIVE,
            case_input=case_input,
            similar_cases=[],
            messages=[],
            created_at=now,
            updated_at=now,
        )
        self._sessions[session.id] = session
        logger.info(f"Created session: {session.id}")
        return session

    def get_session(self, session_id: str) -> DeliberationSession | None:
        """Get a session by ID."""
        return self._sessions.get(session_id)

    def update_session(self, session: DeliberationSession) -> None:
        """Update a session in the store."""
        session.updated_at = datetime.now(timezone.utc)
        self._sessions[session.id] = session
        logger.debug(f"Updated session: {session.id}")

    def add_message(
        self,
        session_id: str,
        message: DeliberationMessage,
    ) -> DeliberationSession | None:
        """Add a message to a session."""
        session = self._sessions.get(session_id)
        if session:
            session.messages.append(message)
            session.updated_at = datetime.now(timezone.utc)
            return session
        return None

    def add_messages(
        self,
        session_id: str,
        messages: list[DeliberationMessage],
    ) -> DeliberationSession | None:
        """Add multiple messages to a session."""
        session = self._sessions.get(session_id)
        if session:
            session.messages.extend(messages)
            session.updated_at = datetime.now(timezone.utc)
            return session
        return None

    def set_similar_cases(
        self,
        session_id: str,
        similar_cases: list[SimilarCase],
    ) -> None:
        """Set similar cases for a session."""
        session = self._sessions.get(session_id)
        if session:
            session.similar_cases = similar_cases
            session.updated_at = datetime.now(timezone.utc)

    def conclude_session(self, session_id: str) -> DeliberationSession | None:
        """Mark a session as concluded."""
        session = self._sessions.get(session_id)
        if session:
            now = datetime.now(timezone.utc)
            session.status = SessionStatus.CONCLUDED
            session.concluded_at = now
            session.updated_at = now
            return session
        return None

    def list_sessions(
        self,
        user_id: str | None = None,
        status: SessionStatus | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[DeliberationSession]:
        """List sessions with optional filtering."""
        sessions = list(self._sessions.values())

        # Filter by user
        if user_id:
            sessions = [s for s in sessions if s.user_id == user_id]

        # Filter by status
        if status:
            sessions = [s for s in sessions if s.status == status]

        # Sort by created_at descending
        sessions.sort(key=lambda s: s.created_at, reverse=True)

        # Paginate
        return sessions[offset : offset + limit]

    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"Deleted session: {session_id}")
            return True
        return False


class CaseDatabase:
    """
    Database operations for case data and similarity search.

    Uses the existing llm_extractions table with pgvector embeddings.
    """

    def __init__(self, db_engine: AsyncEngine):
        """
        Initialize the case database.

        Args:
            db_engine: SQLAlchemy async engine
        """
        self.db_engine = db_engine
        self.embedding_service = get_council_embedding_service()
        logger.info("Case database initialized")

    async def find_similar_cases(
        self,
        case_input: CaseInput,
        limit: int = 5,
    ) -> list[SimilarCase]:
        """
        Find similar cases using semantic search.

        Args:
            case_input: Parsed case information
            limit: Maximum number of similar cases

        Returns:
            List of similar cases with similarity scores
        """
        # Build search text from case input
        search_text = self.embedding_service.build_search_text(
            case_input.parsed_case.model_dump()
        )

        if not search_text:
            logger.warning("No search text generated from case input")
            return []

        # Generate query embedding
        query_embedding = await self.embedding_service.generate_query_embedding(
            search_text
        )

        if not query_embedding:
            logger.warning("Failed to generate query embedding")
            return []

        # Search using pgvector
        async_session = async_sessionmaker(
            bind=self.db_engine, class_=AsyncSession
        )

        query_vector = f"[{','.join(str(x) for x in query_embedding)}]"

        async with async_session() as session:
            # Query similar cases using content embedding
            sql = f"""
            SELECT
                extraction_id,
                summary_id,
                summary_en,
                extraction_result,
                1 - (content_embedding <=> '{query_vector}'::vector) as similarity
            FROM llm_extractions
            WHERE content_embedding IS NOT NULL
            AND status = 'completed'
            ORDER BY similarity DESC
            LIMIT :limit
            """

            result = await session.execute(text(sql), {"limit": limit})
            rows = result.fetchall()

        similar_cases = []
        for row in rows:
            case = self._row_to_similar_case(row)
            if case:
                similar_cases.append(case)

        logger.info(f"Found {len(similar_cases)} similar cases")
        return similar_cases

    def _row_to_similar_case(self, row) -> SimilarCase | None:
        """Convert a database row to SimilarCase."""
        try:
            extraction_result = row.extraction_result or {}

            # Extract case number from court info (primary location)
            case_number = "Unknown"
            court = extraction_result.get("court", {}) or {}
            if court.get("verdict_number"):
                case_number = court["verdict_number"]
            elif court.get("case_register_number"):
                case_number = court["case_register_number"]
            else:
                # Fallback to case_metadata
                case_meta = extraction_result.get("case_metadata", {}) or {}
                if case_meta.get("case_number"):
                    case_number = case_meta["case_number"]

            # Extract verdict info
            verdict = extraction_result.get("verdict", {}) or {}
            verdict_result = verdict.get("result", "unknown")
            verdict_summary = f"Verdict: {verdict_result}"

            # Extract sentence months
            sentences = verdict.get("sentences", {}) or {}
            imprisonment = sentences.get("imprisonment", {}) or {}
            sentence_months = 0
            if imprisonment.get("duration_months"):
                sentence_months = imprisonment["duration_months"]
            elif imprisonment.get("duration_years"):
                sentence_months = imprisonment["duration_years"] * 12

            # Build similarity reason with crime category and defendant info
            case_meta = extraction_result.get("case_metadata", {}) or {}
            crime_category = (
                case_meta.get("crime_category")
                or extraction_result.get("crime_category")
                or "Unknown"
            )
            defendant = extraction_result.get("defendant", {}) or {}
            defendant_name = defendant.get("name", "")
            if defendant_name:
                similarity_reason = f"{crime_category} case involving {defendant_name}"
            else:
                similarity_reason = f"Similar {crime_category} case"

            return SimilarCase(
                case_id=row.extraction_id,
                case_number=case_number,
                similarity_score=float(row.similarity),
                similarity_reason=similarity_reason,
                verdict_summary=verdict_summary,
                sentence_months=sentence_months,
            )
        except Exception as e:
            logger.error(f"Failed to parse similar case: {e}")
            return None

    async def get_case(self, extraction_id: str) -> CaseRecord | None:
        """
        Get a case record by extraction ID.

        Args:
            extraction_id: ID of the extraction

        Returns:
            CaseRecord or None if not found
        """
        async_session = async_sessionmaker(
            bind=self.db_engine, class_=AsyncSession
        )

        async with async_session() as session:
            result = await session.execute(
                select(LLMExtraction).where(
                    LLMExtraction.extraction_id == extraction_id
                )
            )
            llm_extraction = result.scalar_one_or_none()

        if not llm_extraction:
            return None

        return self._extraction_to_case_record(llm_extraction)

    async def search_cases(
        self,
        query: str,
        limit: int = 10,
        semantic_search: bool = True,
        filters: dict[str, Any] | None = None,
    ) -> list[CaseRecord]:
        """
        Search for cases by query.

        Args:
            query: Search query
            limit: Maximum results
            semantic_search: Whether to use semantic search
            filters: Optional structured filters

        Returns:
            List of matching case records
        """
        if semantic_search:
            return await self._semantic_search(query, limit, filters)
        else:
            return await self._text_search(query, limit, filters)

    async def _semantic_search(
        self,
        query: str,
        limit: int,
        filters: dict[str, Any] | None = None,
    ) -> list[CaseRecord]:
        """Perform semantic search using embeddings."""
        # Generate query embedding
        query_embedding = await self.embedding_service.generate_query_embedding(query)

        if not query_embedding:
            logger.warning("Failed to generate query embedding for search")
            return []

        async_session = async_sessionmaker(
            bind=self.db_engine, class_=AsyncSession
        )

        query_vector = f"[{','.join(str(x) for x in query_embedding)}]"

        async with async_session() as session:
            # Base query
            sql = f"""
            SELECT
                extraction_id,
                summary_id,
                summary_en,
                extraction_result,
                1 - (content_embedding <=> '{query_vector}'::vector) as similarity
            FROM llm_extractions
            WHERE content_embedding IS NOT NULL
            AND status = 'completed'
            """

            params: dict[str, Any] = {"limit": limit}

            # Add filters if provided
            if filters:
                if filters.get("case_type"):
                    sql += """
                    AND (
                        extraction_result->>'crime_category' ILIKE :case_type
                        OR extraction_result->'case_metadata'->>'crime_category'
                            ILIKE :case_type
                    )
                    """
                    params["case_type"] = f"%{filters['case_type']}%"

            sql += """
            ORDER BY similarity DESC
            LIMIT :limit
            """

            result = await session.execute(text(sql), params)
            rows = result.fetchall()

        return [
            self._row_to_case_record(row)
            for row in rows
            if self._row_to_case_record(row)
        ]

    async def _text_search(
        self,
        query: str,
        limit: int,
        filters: dict[str, Any] | None = None,
    ) -> list[CaseRecord]:
        """Perform text-based search on summaries."""
        async_session = async_sessionmaker(
            bind=self.db_engine, class_=AsyncSession
        )

        async with async_session() as session:
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
                summary_id ILIKE :query
                OR summary_en ILIKE :query
            )
            ORDER BY created_at DESC
            LIMIT :limit
            """

            result = await session.execute(
                text(sql),
                {"query": f"%{query}%", "limit": limit},
            )
            rows = result.fetchall()

        return [
            self._row_to_case_record(row)
            for row in rows
            if self._row_to_case_record(row)
        ]

    def _extraction_to_case_record(
        self,
        extraction: LLMExtraction,
    ) -> CaseRecord:
        """Convert LLMExtraction to CaseRecord."""
        result = extraction.extraction_result or {}
        case_meta = result.get("case_metadata", {}) or {}
        court = result.get("court", {}) or {}
        defendant = result.get("defendant", {}) or {}
        verdict = result.get("verdict", {}) or {}
        indictment = result.get("indictment", {}) or {}

        # Extract case number from court info (primary) or case_metadata (fallback)
        case_number = (
            court.get("verdict_number")
            or court.get("case_register_number")
            or case_meta.get("case_number")
        )

        # Determine case type
        crime_category = (
            case_meta.get("crime_category")
            or result.get("crime_category")
            or ""
        ).lower()

        if "narkotika" in crime_category or "narcotics" in crime_category:
            case_type = CaseType.NARCOTICS
        elif "korupsi" in crime_category or "corruption" in crime_category:
            case_type = CaseType.CORRUPTION
        elif crime_category:
            case_type = CaseType.GENERAL_CRIMINAL
        else:
            case_type = CaseType.OTHER

        return CaseRecord(
            id=extraction.extraction_id,
            case_number=case_number,
            case_type=case_type,
            court_name=court.get("court_name") or case_meta.get("court_name"),
            court_type=court.get("court_level") or case_meta.get("court_type"),
            decision_date=case_meta.get("decision_date"),
            defendant_name=defendant.get("name"),
            defendant_age=defendant.get("age"),
            defendant_first_offender=None,  # Not always available
            indictment=indictment,
            narcotics_details=result.get("narcotics"),
            corruption_details=result.get("state_loss"),
            legal_facts=result.get("legal_facts"),
            verdict=verdict,
            legal_basis=self._extract_legal_basis(result),
            extraction_result=result,
            summary_en=extraction.summary_en,
            summary_id=extraction.summary_id,
        )

    def _row_to_case_record(self, row) -> CaseRecord | None:
        """Convert a database row to CaseRecord."""
        try:
            result = row.extraction_result or {}
            case_meta = result.get("case_metadata", {}) or {}
            court = result.get("court", {}) or {}
            defendant = result.get("defendant", {}) or {}
            verdict = result.get("verdict", {}) or {}

            # Extract case number from court info (primary) or case_metadata (fallback)
            case_number = (
                court.get("verdict_number")
                or court.get("case_register_number")
                or case_meta.get("case_number")
            )

            # Determine case type
            crime_category = (
                case_meta.get("crime_category")
                or result.get("crime_category")
                or ""
            ).lower()

            if "narkotika" in crime_category or "narcotics" in crime_category:
                case_type = CaseType.NARCOTICS
            elif "korupsi" in crime_category or "corruption" in crime_category:
                case_type = CaseType.CORRUPTION
            elif crime_category:
                case_type = CaseType.GENERAL_CRIMINAL
            else:
                case_type = CaseType.OTHER

            return CaseRecord(
                id=row.extraction_id,
                case_number=case_number,
                case_type=case_type,
                court_name=court.get("court_name") or case_meta.get("court_name"),
                court_type=court.get("court_level") or case_meta.get("court_type"),
                decision_date=case_meta.get("decision_date"),
                defendant_name=defendant.get("name"),
                defendant_age=defendant.get("age"),
                defendant_first_offender=None,
                indictment=result.get("indictment"),
                narcotics_details=result.get("narcotics"),
                corruption_details=result.get("state_loss"),
                legal_facts=result.get("legal_facts"),
                verdict=verdict,
                legal_basis=self._extract_legal_basis(result),
                extraction_result=result,
                summary_en=row.summary_en,
                summary_id=row.summary_id,
            )
        except Exception as e:
            logger.error(f"Failed to parse case record: {e}")
            return None

    def _extract_legal_basis(self, result: dict[str, Any]) -> list[str]:
        """Extract legal basis from extraction result."""
        legal_basis = []
        indictment = result.get("indictment", {}) or {}
        cited_articles = indictment.get("cited_articles", []) or []

        for article in cited_articles[:10]:
            if article and article.get("full_citation"):
                legal_basis.append(article["full_citation"])
            elif article and article.get("article"):
                legal_basis.append(article["article"])

        return legal_basis


# =============================================================================
# Singletons
# =============================================================================

_session_store: SessionStore | None = None


def get_session_store() -> SessionStore:
    """Get or create the session store singleton."""
    global _session_store
    if _session_store is None:
        _session_store = SessionStore()
    return _session_store
