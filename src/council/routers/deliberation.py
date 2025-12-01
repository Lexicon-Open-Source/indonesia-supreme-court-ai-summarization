"""
Deliberation endpoints for the Virtual Judicial Council.

Provides endpoints for:
- Sending messages to the council
- Getting agent responses
- Generating legal opinions
- Streaming deliberation responses (SSE)
"""

import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncEngine

from src.council.agents.orchestrator import StreamEvent, get_agent_orchestrator
from src.council.database import CaseDatabase, SessionStore, get_session_store
from src.council.schemas import (
    AgentSender,
    ContinueDiscussionRequest,
    ContinueDiscussionResponse,
    DeliberationMessage,
    GenerateOpinionRequest,
    GenerateOpinionResponse,
    GetMessagesResponse,
    SendMessageRequest,
    SendMessageResponse,
    SessionStatus,
    StreamContinueRequest,
    StreamEventData,
    StreamEventType,
    StreamMessageRequest,
    UserSender,
)
from src.council.services.opinion_generator import get_opinion_generator_service
from src.council.services.pdf_generator import get_pdf_generator_service

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
    session = await store.get_session(session_id)

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
        f"Processing message for session {session_id}: target={request.target_agent}"
    )

    # Get similar cases for context
    case_db = CaseDatabase(db_engine)
    if not session.similar_cases:
        similar_cases = await case_db.find_similar_cases(
            case_input=session.case_input,
            limit=5,
        )
        await store.set_similar_cases(session_id, similar_cases)
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
    await store.add_message(session_id, user_msg)
    await store.add_messages(session_id, agent_responses)

    return SendMessageResponse(
        user_message=user_msg,
        agent_responses=agent_responses,
    )


@router.post("/{session_id}/continue", response_model=ContinueDiscussionResponse)
async def continue_discussion(
    session_id: str,
    request: ContinueDiscussionRequest,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> ContinueDiscussionResponse:
    """
    Continue the judicial discussion without user input.

    This allows the judges to continue deliberating amongst themselves,
    responding to each other's points, building consensus, or exploring
    disagreements. Use this to let the discussion flow naturally.

    Each "round" means all three judges get a chance to respond to the
    current state of the discussion.

    Returns new messages generated in this continuation.
    """
    store = get_session_store()
    session = await store.get_session(session_id)

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

    if len(session.messages) < 3:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Need at least 3 messages before continuing discussion",
        )

    logger.info(
        f"Continuing discussion for session {session_id}, {request.num_rounds} round(s)"
    )

    # Get similar cases for context
    case_db = CaseDatabase(db_engine)
    if not session.similar_cases:
        similar_cases = await case_db.find_similar_cases(
            case_input=session.case_input,
            limit=5,
        )
        await store.set_similar_cases(session_id, similar_cases)
    else:
        similar_cases = session.similar_cases

    # Continue the discussion
    orchestrator = get_agent_orchestrator()
    try:
        new_messages = await orchestrator.continue_discussion(
            session_id=session_id,
            case_input=session.case_input.parsed_case,
            similar_cases=similar_cases,
            history=session.messages,
            num_rounds=request.num_rounds,
        )
    except Exception as e:
        logger.error(f"Failed to continue discussion: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Failed to continue discussion: {str(e)}",
        )

    # Add new messages to session
    await store.add_messages(session_id, new_messages)

    # Get updated message count
    updated_session = await store.get_session(session_id)
    total_messages = len(updated_session.messages) if updated_session else 0

    return ContinueDiscussionResponse(
        new_messages=new_messages,
        total_messages=total_messages,
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
    session = await store.get_session(session_id)

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
    session = await store.get_session(session_id)

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
    await store.update_session(session)

    return GenerateOpinionResponse(opinion=opinion)


@router.get("/{session_id}/opinion")
async def get_opinion(session_id: str) -> dict:
    """
    Get the generated legal opinion for a session.

    Returns the previously generated opinion, or an error if none exists.
    """
    store = get_session_store()
    session = await store.get_session(session_id)

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


# =============================================================================
# Streaming Endpoints (SSE)
# =============================================================================


def _stream_event_to_sse(event: StreamEvent) -> str:
    """Convert a StreamEvent to SSE format."""
    data = StreamEventData(
        event_type=StreamEventType(event.event_type),
        agent_id=event.agent_id,
        content=event.content,
        message_id=event.message_id,
        full_content=event.full_content,
    )
    return f"data: {json.dumps(data.model_dump())}\n\n"


async def _stream_generator(
    events: AsyncIterator[StreamEvent],
) -> AsyncIterator[str]:
    """Generator that converts StreamEvents to SSE format."""
    async for event in events:
        yield _stream_event_to_sse(event)


async def _stream_with_persistence(
    events: AsyncIterator[StreamEvent],
    session_id: str,
    store: SessionStore,
) -> AsyncIterator[str]:
    """
    Generator that persists messages to DB as they complete, then yields SSE.

    Intercepts:
    - user_message events: saves user message to database
    - agent_complete events: saves agent message to database

    All events are yielded to the client unchanged.
    """
    async for event in events:
        # Persist user message when recorded
        if event.event_type == "user_message":
            user_msg = DeliberationMessage(
                id=event.message_id or str(uuid4()),
                session_id=session_id,
                sender=UserSender(),
                content=event.content,
                timestamp=datetime.now(UTC),
            )
            try:
                await store.add_message(session_id, user_msg)
                logger.debug(f"Persisted user message: {user_msg.id}")
            except Exception as e:
                logger.error(f"Failed to persist user message: {e}")

        # Persist agent message when complete
        elif event.event_type == "agent_complete" and event.agent_id:
            agent_msg = DeliberationMessage(
                id=event.message_id or str(uuid4()),
                session_id=session_id,
                sender=AgentSender(agent_id=event.agent_id),
                content=event.full_content or event.content,
                timestamp=datetime.now(UTC),
            )
            try:
                await store.add_message(session_id, agent_msg)
                logger.debug(
                    f"Persisted agent message from {event.agent_id}: {agent_msg.id}"
                )
            except Exception as e:
                logger.error(f"Failed to persist agent message: {e}")

        # Always yield the event to the client
        yield _stream_event_to_sse(event)


@router.post("/{session_id}/stream/initial")
async def stream_initial_opinions(
    session_id: str,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> StreamingResponse:
    """
    Stream remaining initial opinions from judges (SSE).

    Session creation generates 1 random judge's opinion. This endpoint
    streams opinions from the remaining judges to complete the initial
    deliberation round.

    SSE Event Types:
    - agent_start: A judge is about to speak
    - chunk: A piece of text from the current judge
    - agent_complete: A judge finished speaking (includes full content)
    - deliberation_complete: All judges have spoken

    Returns Server-Sent Events stream.
    """
    store = get_session_store()
    session = await store.get_session(session_id)

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

    # Need at least the initial message from session creation
    if len(session.messages) == 0:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="No initial message found. Session may not be properly created.",
        )

    # Check if all 3 judges have already spoken in the initial round
    agent_messages = [
        msg for msg in session.messages
        if hasattr(msg.sender, "agent_id")
    ]
    if len(agent_messages) >= 3:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Initial opinions already complete. Use /stream/continue instead.",
        )

    logger.info(
        f"Streaming remaining initial opinions for session {session_id} "
        f"({len(agent_messages)} judges have spoken)"
    )

    # Get similar cases for context
    case_db = CaseDatabase(db_engine)
    if not session.similar_cases:
        similar_cases = await case_db.find_similar_cases(
            case_input=session.case_input,
            limit=5,
        )
        await store.set_similar_cases(session_id, similar_cases)
    else:
        similar_cases = session.similar_cases

    # Use continue_discussion_stream to get remaining judges' opinions
    # This will check who has spoken and get responses from others
    orchestrator = get_agent_orchestrator()
    event_stream = orchestrator.continue_discussion_stream(
        session_id=session_id,
        case_input=session.case_input.parsed_case,
        similar_cases=similar_cases,
        history=session.messages,
        num_rounds=1,
    )

    return StreamingResponse(
        _stream_with_persistence(event_stream, session_id, store),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{session_id}/stream/message")
async def stream_message_response(
    session_id: str,
    request: StreamMessageRequest,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> StreamingResponse:
    """
    Send a message and stream agent responses (SSE).

    The message is processed by the orchestrator, which determines
    which agent(s) should respond. Responses are streamed in real-time.

    SSE Event Types:
    - user_message: The user's message was recorded
    - agent_start: A judge is about to respond
    - chunk: A piece of text from the current judge
    - agent_complete: A judge finished responding (includes full content)
    - deliberation_complete: All responding judges have finished

    Returns Server-Sent Events stream.
    """
    store = get_session_store()
    session = await store.get_session(session_id)

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
        f"Streaming message response for session {session_id}: "
        f"target={request.target_agent}"
    )

    # Get similar cases for context
    case_db = CaseDatabase(db_engine)
    if not session.similar_cases:
        similar_cases = await case_db.find_similar_cases(
            case_input=session.case_input,
            limit=5,
        )
        await store.set_similar_cases(session_id, similar_cases)
    else:
        similar_cases = session.similar_cases

    # Create the stream
    orchestrator = get_agent_orchestrator()
    event_stream = orchestrator.process_user_message_stream(
        session_id=session_id,
        user_message=request.content,
        case_input=session.case_input.parsed_case,
        similar_cases=similar_cases,
        history=session.messages,
        target_agent=request.target_agent,
    )

    return StreamingResponse(
        _stream_with_persistence(event_stream, session_id, store),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{session_id}/stream/continue")
async def stream_continue_discussion(
    session_id: str,
    request: StreamContinueRequest,
    db_engine: Annotated[AsyncEngine, Depends(get_db_engine)],
) -> StreamingResponse:
    """
    Continue the judicial discussion with streaming (SSE).

    This allows the judges to continue deliberating amongst themselves,
    with responses streamed in real-time.

    SSE Event Types:
    - agent_start: A judge is about to speak
    - chunk: A piece of text from the current judge
    - agent_complete: A judge finished speaking (includes full content)
    - agent_error: An error occurred with a specific judge
    - deliberation_complete: All rounds have finished

    Returns Server-Sent Events stream.
    """
    store = get_session_store()
    session = await store.get_session(session_id)

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

    if len(session.messages) < 3:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Need at least 3 messages before continuing discussion",
        )

    logger.info(
        f"Streaming continued discussion for session {session_id}, "
        f"{request.num_rounds} round(s)"
    )

    # Get similar cases for context
    case_db = CaseDatabase(db_engine)
    if not session.similar_cases:
        similar_cases = await case_db.find_similar_cases(
            case_input=session.case_input,
            limit=5,
        )
        await store.set_similar_cases(session_id, similar_cases)
    else:
        similar_cases = session.similar_cases

    # Create the stream
    orchestrator = get_agent_orchestrator()
    event_stream = orchestrator.continue_discussion_stream(
        session_id=session_id,
        case_input=session.case_input.parsed_case,
        similar_cases=similar_cases,
        history=session.messages,
        num_rounds=request.num_rounds,
    )

    return StreamingResponse(
        _stream_with_persistence(event_stream, session_id, store),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# =============================================================================
# PDF Download Endpoint
# =============================================================================


@router.get("/{session_id}/download/pdf")
async def download_deliberation_pdf(session_id: str) -> Response:
    """
    Download the deliberation session as a PDF document.

    Generates a professional PDF containing:
    - Case information and summary
    - Similar cases for reference
    - Full deliberation transcript
    - Legal opinion (if generated)

    Returns the PDF as a downloadable file.
    """
    store = get_session_store()
    session = await store.get_session(session_id)

    if not session:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )

    if len(session.messages) == 0:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="No messages in session. Cannot generate PDF.",
        )

    logger.info(f"Generating PDF for session {session_id}")

    # Parse legal opinion if available
    legal_opinion = None
    if session.legal_opinion:
        from src.council.schemas import LegalOpinionDraft

        try:
            legal_opinion = LegalOpinionDraft.model_validate(session.legal_opinion)
        except Exception as e:
            logger.warning(f"Failed to parse legal opinion: {e}")

    # Generate PDF
    pdf_generator = get_pdf_generator_service()
    try:
        pdf_bytes = pdf_generator.generate_deliberation_pdf(
            session_id=session_id,
            case_input=session.case_input,
            similar_cases=session.similar_cases or [],
            messages=session.messages,
            legal_opinion=legal_opinion,
        )
    except Exception as e:
        logger.error(f"Failed to generate PDF: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate PDF: {str(e)}",
        )

    # Generate filename
    filename = f"deliberation_{session_id[:8]}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(pdf_bytes)),
        },
    )
