"""
Deliberation endpoints for the Virtual Judicial Council.

Provides endpoints for:
- Sending messages to the council
- Getting agent responses
- Generating legal opinions
"""

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncEngine

from src.council.agents.orchestrator import get_agent_orchestrator
from src.council.database import CaseDatabase, get_session_store
from src.council.schemas import (
    GenerateOpinionRequest,
    GenerateOpinionResponse,
    GetMessagesResponse,
    SendMessageRequest,
    SendMessageResponse,
    SessionStatus,
)
from src.council.services.opinion_generator import get_opinion_generator_service

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


@router.post("/{session_id}/message", response_model=SendMessageResponse)
async def send_message(
    session_id: str,
    request: SendMessageRequest,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> SendMessageResponse:
    """
    Send a message to the judicial council.

    The message is processed by the orchestrator, which determines
    which agent(s) should respond based on:
    - Target agent (if specified)
    - Message intent (inferred from content)
    - Conversation balance

    Returns the user's message and all agent responses.
    """
    store = get_session_store()
    session = store.get_session(session_id)

    if not session:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    if session.status != SessionStatus.ACTIVE:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Session is not active",
        )

    logger.info(
        f"Processing message for session {session_id}: "
        f"target={request.target_agent}"
    )

    # Get similar cases for context
    case_db = CaseDatabase(db_engine)
    if not session.similar_cases:
        similar_cases = await case_db.find_similar_cases(
            case_input=session.case_input,
            limit=5,
        )
        store.set_similar_cases(session_id, similar_cases)
    else:
        similar_cases = session.similar_cases

    # Process message through orchestrator
    orchestrator = get_agent_orchestrator()
    try:
        user_msg, agent_responses = await orchestrator.process_user_message(
            session_id=session_id,
            user_message=request.content,
            case_input=session.case_input.parsed_case,
            similar_cases=similar_cases,
            history=session.messages,
            target_agent=request.target_agent,
        )
    except Exception as e:
        logger.error(f"Failed to process message: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Failed to process message: {str(e)}",
        )

    # Add all messages to session
    store.add_message(session_id, user_msg)
    store.add_messages(session_id, agent_responses)

    return SendMessageResponse(
        user_message=user_msg,
        agent_responses=agent_responses,
    )


@router.get("/{session_id}/messages", response_model=GetMessagesResponse)
async def get_messages(
    session_id: str,
    limit: int = 50,
    offset: int = 0,
) -> GetMessagesResponse:
    """
    Get messages from a deliberation session.

    Returns messages in chronological order with pagination.
    """
    store = get_session_store()
    session = store.get_session(session_id)

    if not session:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    # Paginate messages
    messages = session.messages[offset : offset + limit]

    return GetMessagesResponse(messages=messages)


@router.post("/{session_id}/opinion", response_model=GenerateOpinionResponse)
async def generate_opinion(
    session_id: str,
    request: GenerateOpinionRequest = GenerateOpinionRequest(),
) -> GenerateOpinionResponse:
    """
    Generate a legal opinion from the deliberation.

    Synthesizes the discussion between all three judges into a
    structured legal opinion document including:
    - Verdict recommendation with confidence level
    - Sentence recommendation with ranges
    - Categorized legal arguments
    - Cited precedents and applicable laws
    - Dissenting views (optional)

    Requires at least 3 messages in the session to generate.
    """
    store = get_session_store()
    session = store.get_session(session_id)

    if not session:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    if len(session.messages) < 3:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Need at least 3 messages to generate opinion",
        )

    logger.info(f"Generating opinion for session {session_id}")

    generator = get_opinion_generator_service()
    try:
        opinion = await generator.generate_opinion(
            session_id=session_id,
            case_input=session.case_input.parsed_case,
            similar_cases=session.similar_cases,
            messages=session.messages,
            include_dissent=request.include_dissent,
        )
    except Exception as e:
        logger.error(f"Failed to generate opinion: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate opinion: {str(e)}",
        )

    # Store opinion in session
    session.legal_opinion = opinion.model_dump()
    store.update_session(session)

    return GenerateOpinionResponse(opinion=opinion)


@router.get("/{session_id}/opinion")
async def get_opinion(session_id: str) -> dict:
    """
    Get the generated legal opinion for a session.

    Returns the previously generated opinion, or an error if none exists.
    """
    store = get_session_store()
    session = store.get_session(session_id)

    if not session:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    if not session.legal_opinion:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="No opinion generated for this session yet",
        )

    return {"opinion": session.legal_opinion}
