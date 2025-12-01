"""
Database operations for the Virtual Judicial Council.

Provides:
- Session storage and retrieval with PostgreSQL persistence
- Similar case search using pgvector embeddings
- Case data access from llm_extractions table
"""

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import TIMESTAMP, Column, Integer, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import Field as SQLField
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.council.schemas import (
    AgentId,
    AgentSender,
    CaseInput,
    CaseRecord,
    CaseType,
    DeliberationMessage,
    DeliberationSession,
    InputType,
    ParsedCaseInput,
    SessionStatus,
    SimilarCase,
    SystemSender,
    UserSender,
)
from src.council.services.embeddings import (
    get_council_embedding_service,
)
from src.extraction import LLMExtraction

logger = logging.getLogger(__name__)


# =============================================================================
# SQLModel Table Definitions
# =============================================================================

# Schema for all council tables
COUNCIL_SCHEMA = "council_v1"


class DeliberationSessionDB(SQLModel, table=True):
    """SQLModel for deliberation_sessions table in council_v1 schema."""

    __tablename__ = "deliberation_sessions"
    __table_args__ = {"schema": COUNCIL_SCHEMA}

    id: str = SQLField(primary_key=True)
    user_id: str | None = SQLField(default=None, index=True)
    status: str = SQLField(default=SessionStatus.ACTIVE.value, index=True)

    # Complex nested data stored as JSONB
    case_input: dict = SQLField(sa_column=Column(JSONB, nullable=False))
    similar_cases: list = SQLField(
        default_factory=list, sa_column=Column(JSONB, nullable=False)
    )
    legal_opinion: dict | None = SQLField(
        default=None, sa_column=Column(JSONB, nullable=True)
    )

    # Timestamps
    created_at: datetime = SQLField(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
    )
    updated_at: datetime = SQLField(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
    )
    concluded_at: datetime | None = SQLField(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True),
    )


class DeliberationMessageDB(SQLModel, table=True):
    """SQLModel for deliberation_messages table in council_v1 schema."""

    __tablename__ = "deliberation_messages"
    __table_args__ = {"schema": COUNCIL_SCHEMA}

    id: str = SQLField(primary_key=True)
    session_id: str = SQLField(
        index=True, foreign_key=f"{COUNCIL_SCHEMA}.deliberation_sessions.id"
    )

    # Sender stored as JSONB to handle union type
    sender: dict = SQLField(sa_column=Column(JSONB, nullable=False))
    content: str = SQLField(sa_column=Column(Text, nullable=False))
    intent: str | None = SQLField(default=None)

    # Lists stored as JSONB
    cited_cases: list = SQLField(
        default_factory=list, sa_column=Column(JSONB, nullable=False)
    )
    cited_laws: list = SQLField(
        default_factory=list, sa_column=Column(JSONB, nullable=False)
    )

    # Ordering and timestamp
    sequence_number: int = SQLField(
        sa_column=Column(Integer, nullable=False)
    )
    timestamp: datetime = SQLField(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
    )


# =============================================================================
# Conversion Helpers
# =============================================================================


def _sender_to_dict(sender: UserSender | AgentSender | SystemSender) -> dict:
    """Convert a MessageSender to a dictionary for JSONB storage."""
    if isinstance(sender, AgentSender):
        return {"type": "agent", "agent_id": sender.agent_id.value}
    elif isinstance(sender, UserSender):
        return {"type": "user"}
    else:
        return {"type": "system"}


def _dict_to_sender(data: dict) -> UserSender | AgentSender | SystemSender:
    """Convert a dictionary from JSONB to a MessageSender."""
    sender_type = data.get("type")
    if sender_type == "agent":
        return AgentSender(agent_id=AgentId(data["agent_id"]))
    elif sender_type == "user":
        return UserSender()
    else:
        return SystemSender()


def _db_session_to_schema(
    db_session: DeliberationSessionDB,
    messages: list[DeliberationMessageDB],
) -> DeliberationSession:
    """Convert database models to Pydantic schema."""
    # Convert messages
    schema_messages = [
        DeliberationMessage(
            id=msg.id,
            session_id=msg.session_id,
            sender=_dict_to_sender(msg.sender),
            content=msg.content,
            intent=msg.intent,
            cited_cases=msg.cited_cases or [],
            cited_laws=msg.cited_laws or [],
            timestamp=msg.timestamp,
        )
        for msg in sorted(messages, key=lambda m: m.sequence_number)
    ]

    # Convert case_input from dict to CaseInput
    case_input_dict = db_session.case_input
    case_input = CaseInput(
        input_type=InputType(case_input_dict["input_type"]),
        raw_input=case_input_dict["raw_input"],
        parsed_case=ParsedCaseInput.model_validate(case_input_dict["parsed_case"]),
    )

    # Convert similar_cases from list of dicts to list of SimilarCase
    similar_cases = [
        SimilarCase.model_validate(sc) for sc in (db_session.similar_cases or [])
    ]

    return DeliberationSession(
        id=db_session.id,
        user_id=db_session.user_id,
        status=SessionStatus(db_session.status),
        case_input=case_input,
        similar_cases=similar_cases,
        messages=schema_messages,
        legal_opinion=db_session.legal_opinion,
        created_at=db_session.created_at,
        updated_at=db_session.updated_at,
        concluded_at=db_session.concluded_at,
    )


# =============================================================================
# Database Session Store
# =============================================================================


class SessionStore:
    """
    Database-backed store for deliberation sessions.

    Persists sessions and messages to PostgreSQL using SQLModel.
    All methods are async to support database operations.
    """

    def __init__(self, db_engine: AsyncEngine):
        """
        Initialize the session store with a database engine.

        Args:
            db_engine: SQLAlchemy async engine for database operations
        """
        self._db_engine = db_engine
        self._async_session = async_sessionmaker(
            bind=db_engine, class_=AsyncSession, expire_on_commit=False
        )
        logger.info("Database session store initialized")

    async def create_tables(self) -> None:
        """Create database tables if they don't exist."""
        async with self._db_engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        logger.info("Deliberation tables created/verified")

    async def create_session(
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
        session_id = str(uuid4())

        # Create DB model
        db_session = DeliberationSessionDB(
            id=session_id,
            user_id=user_id,
            status=SessionStatus.ACTIVE.value,
            case_input=case_input.model_dump(mode="json"),
            similar_cases=[],
            legal_opinion=None,
            created_at=now,
            updated_at=now,
        )

        async with self._async_session() as session:
            session.add(db_session)
            await session.commit()
            await session.refresh(db_session)

        logger.info(f"Created session: {session_id}")

        # Return as Pydantic schema
        return DeliberationSession(
            id=session_id,
            user_id=user_id,
            status=SessionStatus.ACTIVE,
            case_input=case_input,
            similar_cases=[],
            messages=[],
            created_at=now,
            updated_at=now,
        )

    async def get_session(self, session_id: str) -> DeliberationSession | None:
        """Get a session by ID with all its messages."""
        async with self._async_session() as session:
            # Get session
            result = await session.execute(
                select(DeliberationSessionDB).where(
                    DeliberationSessionDB.id == session_id
                )
            )
            db_session = result.scalar_one_or_none()

            if not db_session:
                return None

            # Get messages
            msg_result = await session.execute(
                select(DeliberationMessageDB)
                .where(DeliberationMessageDB.session_id == session_id)
                .order_by(DeliberationMessageDB.sequence_number)
            )
            messages = list(msg_result.scalars().all())

        return _db_session_to_schema(db_session, messages)

    async def update_session(self, session: DeliberationSession) -> None:
        """Update a session in the database."""
        async with self._async_session() as db_session:
            result = await db_session.execute(
                select(DeliberationSessionDB).where(
                    DeliberationSessionDB.id == session.id
                )
            )
            db_record = result.scalar_one_or_none()

            if db_record:
                db_record.status = session.status.value
                db_record.case_input = session.case_input.model_dump(mode="json")
                db_record.similar_cases = [
                    sc.model_dump(mode="json") for sc in session.similar_cases
                ]
                db_record.legal_opinion = session.legal_opinion
                db_record.updated_at = datetime.now(timezone.utc)
                db_record.concluded_at = session.concluded_at

                await db_session.commit()
                logger.debug(f"Updated session: {session.id}")

    async def add_message(
        self,
        session_id: str,
        message: DeliberationMessage,
    ) -> DeliberationSession | None:
        """Add a message to a session."""
        async with self._async_session() as session:
            # Verify session exists
            result = await session.execute(
                select(DeliberationSessionDB).where(
                    DeliberationSessionDB.id == session_id
                )
            )
            db_session = result.scalar_one_or_none()

            if not db_session:
                return None

            # Get current message count for sequence number
            count_result = await session.execute(
                select(DeliberationMessageDB)
                .where(DeliberationMessageDB.session_id == session_id)
            )
            current_count = len(list(count_result.scalars().all()))

            # Create message
            db_message = DeliberationMessageDB(
                id=message.id,
                session_id=session_id,
                sender=_sender_to_dict(message.sender),
                content=message.content,
                intent=message.intent,
                cited_cases=message.cited_cases,
                cited_laws=message.cited_laws,
                sequence_number=current_count,
                timestamp=message.timestamp or datetime.now(timezone.utc),
            )

            session.add(db_message)

            # Update session timestamp
            db_session.updated_at = datetime.now(timezone.utc)

            await session.commit()

        return await self.get_session(session_id)

    async def add_messages(
        self,
        session_id: str,
        messages: list[DeliberationMessage],
    ) -> DeliberationSession | None:
        """Add multiple messages to a session."""
        if not messages:
            return await self.get_session(session_id)

        async with self._async_session() as session:
            # Verify session exists
            result = await session.execute(
                select(DeliberationSessionDB).where(
                    DeliberationSessionDB.id == session_id
                )
            )
            db_session = result.scalar_one_or_none()

            if not db_session:
                return None

            # Get current message count for sequence numbers
            count_result = await session.execute(
                select(DeliberationMessageDB)
                .where(DeliberationMessageDB.session_id == session_id)
            )
            current_count = len(list(count_result.scalars().all()))

            # Create all messages
            for i, message in enumerate(messages):
                db_message = DeliberationMessageDB(
                    id=message.id,
                    session_id=session_id,
                    sender=_sender_to_dict(message.sender),
                    content=message.content,
                    intent=message.intent,
                    cited_cases=message.cited_cases,
                    cited_laws=message.cited_laws,
                    sequence_number=current_count + i,
                    timestamp=message.timestamp or datetime.now(timezone.utc),
                )
                session.add(db_message)

            # Update session timestamp
            db_session.updated_at = datetime.now(timezone.utc)

            await session.commit()

        return await self.get_session(session_id)

    async def set_similar_cases(
        self,
        session_id: str,
        similar_cases: list[SimilarCase],
    ) -> None:
        """Set similar cases for a session."""
        async with self._async_session() as session:
            result = await session.execute(
                select(DeliberationSessionDB).where(
                    DeliberationSessionDB.id == session_id
                )
            )
            db_session = result.scalar_one_or_none()

            if db_session:
                db_session.similar_cases = [
                    sc.model_dump(mode="json") for sc in similar_cases
                ]
                db_session.updated_at = datetime.now(timezone.utc)
                await session.commit()

    async def conclude_session(
        self, session_id: str
    ) -> DeliberationSession | None:
        """Mark a session as concluded."""
        async with self._async_session() as session:
            result = await session.execute(
                select(DeliberationSessionDB).where(
                    DeliberationSessionDB.id == session_id
                )
            )
            db_session = result.scalar_one_or_none()

            if not db_session:
                return None

            now = datetime.now(timezone.utc)
            db_session.status = SessionStatus.CONCLUDED.value
            db_session.concluded_at = now
            db_session.updated_at = now

            await session.commit()

        return await self.get_session(session_id)

    async def list_sessions(
        self,
        user_id: str | None = None,
        status: SessionStatus | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[DeliberationSession]:
        """List sessions with optional filtering."""
        async with self._async_session() as session:
            query = select(DeliberationSessionDB)

            # Apply filters
            if user_id:
                query = query.where(DeliberationSessionDB.user_id == user_id)
            if status:
                query = query.where(DeliberationSessionDB.status == status.value)

            # Order by created_at descending
            query = query.order_by(DeliberationSessionDB.created_at.desc())

            # Apply pagination
            query = query.offset(offset).limit(limit)

            result = await session.execute(query)
            db_sessions = list(result.scalars().all())

        # Convert to schemas (without messages for listing)
        sessions = []
        for db_session in db_sessions:
            # For listing, we don't load all messages - just get the session
            async with self._async_session() as session:
                msg_result = await session.execute(
                    select(DeliberationMessageDB)
                    .where(DeliberationMessageDB.session_id == db_session.id)
                    .order_by(DeliberationMessageDB.sequence_number)
                )
                messages = list(msg_result.scalars().all())

            sessions.append(_db_session_to_schema(db_session, messages))

        return sessions

    async def count_sessions(
        self,
        user_id: str | None = None,
        status: SessionStatus | None = None,
    ) -> int:
        """Count sessions with optional filtering."""
        async with self._async_session() as session:
            query = select(DeliberationSessionDB)

            if user_id:
                query = query.where(DeliberationSessionDB.user_id == user_id)
            if status:
                query = query.where(DeliberationSessionDB.status == status.value)

            result = await session.execute(query)
            return len(list(result.scalars().all()))

    async def delete_session(self, session_id: str) -> bool:
        """Delete a session and all its messages."""
        async with self._async_session() as session:
            # Delete messages first (or rely on CASCADE)
            await session.execute(
                text(
                    f"DELETE FROM {COUNCIL_SCHEMA}.deliberation_messages "
                    "WHERE session_id = :session_id"
                ),
                {"session_id": session_id},
            )

            # Delete session
            result = await session.execute(
                select(DeliberationSessionDB).where(
                    DeliberationSessionDB.id == session_id
                )
            )
            db_session = result.scalar_one_or_none()

            if db_session:
                await session.delete(db_session)
                await session.commit()
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
        async_session = async_sessionmaker(bind=self.db_engine, class_=AsyncSession)

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
        async_session = async_sessionmaker(bind=self.db_engine, class_=AsyncSession)

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

        async_session = async_sessionmaker(bind=self.db_engine, class_=AsyncSession)

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
        async_session = async_sessionmaker(bind=self.db_engine, class_=AsyncSession)

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
            case_meta.get("crime_category") or result.get("crime_category") or ""
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
                case_meta.get("crime_category") or result.get("crime_category") or ""
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


def init_session_store(db_engine: AsyncEngine) -> SessionStore:
    """
    Initialize the session store singleton with a database engine.

    Must be called once at application startup before using get_session_store().

    Args:
        db_engine: SQLAlchemy async engine for database operations

    Returns:
        The initialized SessionStore instance
    """
    global _session_store
    _session_store = SessionStore(db_engine)
    logger.info("Session store initialized with database engine")
    return _session_store


def get_session_store() -> SessionStore:
    """
    Get the session store singleton.

    Raises:
        RuntimeError: If init_session_store() hasn't been called yet
    """
    global _session_store
    if _session_store is None:
        raise RuntimeError(
            "Session store not initialized. Call init_session_store() first."
        )
    return _session_store
