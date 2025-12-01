"""
Session management endpoints for the Virtual Judicial Council.

Provides endpoints for:
- Creating new deliberation sessions
- Retrieving session details
- Listing sessions
- Deleting sessions
"""

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncEngine

from src.council.agents.orchestrator import get_agent_orchestrator
from src.council.database import CaseDatabase, get_session_store
from src.council.schemas import (
    CreateSessionRequest,
    CreateSessionResponse,
    GetSessionResponse,
    ListSessionsResponse,
    SessionStatus,
)
from src.council.services.case_parser import get_case_parser_service

logger = logging.getLogger(__name__)

router = APIRouter()


# Dependency to get database engine (will be injected from main app)
async def get_db_engine() -> AsyncEngine:
    """Get database engine - must be set up by main app."""
    from src.council.routers import _db_engine

    if _db_engine is None:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Database not initialized",
        )
    return _db_engine


# This will be set by main.py when including the router
_db_engine: AsyncEngine | None = None


def set_db_engine(engine: AsyncEngine) -> None:
    """Set the database engine for council routes."""
    global _db_engine
    _db_engine = engine
    logger.info("Council database engine set")


@router.post("", response_model=CreateSessionResponse)
async def create_session(
    request: CreateSessionRequest,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> CreateSessionResponse:
    """
    Create a new deliberation session.

    This endpoint:
    1. Parses the case summary into structured data
    2. Finds similar cases via semantic search
    3. Generates initial opinions from all three judges
    4. Creates and stores the session

    The response includes the session ID, parsed case, similar cases,
    and the first agent message to display.
    """
    logger.info(f"Creating new session with case type: {request.case_type}")

    # Parse the case
    parser = get_case_parser_service()
    try:
        case_input = await parser.parse_case(
            case_text=request.case_summary,
            structured_data=request.structured_data,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Failed to parse case: {str(e)}",
        )

    # Find similar cases
    case_db = CaseDatabase(db_engine)
    similar_cases = await case_db.find_similar_cases(
        case_input=case_input,
        limit=5,
    )

    # Create session (now async)
    store = get_session_store()
    session = await store.create_session(case_input=case_input)
    await store.set_similar_cases(session.id, similar_cases)

    # Generate initial opinion from a randomly selected judge
    # This makes session creation faster (~5s instead of ~15s)
    # Users can use /stream/initial or /stream/continue for full deliberation
    orchestrator = get_agent_orchestrator()
    try:
        initial_message = await orchestrator.generate_random_initial_opinion(
            session_id=session.id,
            case_input=case_input.parsed_case,
            similar_cases=similar_cases,
        )
    except Exception as e:
        logger.error(f"Failed to generate initial opinion: {e}")
        # Still create session, just without initial message
        initial_message = None

    # Add message to session (now async)
    if initial_message:
        await store.add_message(session.id, initial_message)

    return CreateSessionResponse(
        session_id=session.id,
        parsed_case=case_input.parsed_case,
        similar_cases=similar_cases,
        initial_message=initial_message,
    )


@router.get("/{session_id}", response_model=GetSessionResponse)
async def get_session(session_id: str) -> GetSessionResponse:
    """
    Get a deliberation session by ID.

    Returns the full session including case input, similar cases,
    and all messages exchanged so far.
    """
    store = get_session_store()
    session = await store.get_session(session_id)

    if not session:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    return GetSessionResponse(session=session)


@router.get("", response_model=ListSessionsResponse)
async def list_sessions(
    status: SessionStatus | None = None,
    limit: int = 20,
    offset: int = 0,
) -> ListSessionsResponse:
    """
    List deliberation sessions.

    Supports filtering by status and pagination.
    """
    store = get_session_store()
    sessions = await store.list_sessions(
        status=status,
        limit=limit,
        offset=offset,
    )

    # Get total count using dedicated method
    total = await store.count_sessions(status=status)

    return ListSessionsResponse(
        sessions=sessions,
        pagination={
            "limit": limit,
            "offset": offset,
            "total": total,
        },
    )


@router.delete("/{session_id}")
async def delete_session(session_id: str) -> dict:
    """
    Delete a deliberation session.

    This permanently removes the session and all its messages.
    """
    store = get_session_store()

    if not await store.get_session(session_id):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    await store.delete_session(session_id)
    return {"message": f"Session {session_id} deleted"}


@router.post("/{session_id}/conclude")
async def conclude_session(session_id: str) -> GetSessionResponse:
    """
    Conclude a deliberation session.

    Marks the session as concluded, preventing further messages.
    The session can still be read and used to generate opinions.
    """
    store = get_session_store()

    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    if session.status == SessionStatus.CONCLUDED:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Session is already concluded",
        )

    concluded = await store.conclude_session(session_id)
    return GetSessionResponse(session=concluded)
