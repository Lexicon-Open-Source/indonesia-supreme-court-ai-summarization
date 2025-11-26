import asyncio
import json
import logging
import sys
from collections import deque
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from http import HTTPStatus
from statistics import mean, median
from typing import Annotated

from fastapi import Depends, FastAPI, Query
from fastapi.exceptions import HTTPException
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from tenacity import retry, stop_after_attempt, wait_exponential

from contexts import AppContexts
from nats_consumer import (
    CONSUMER_CONFIG,
    STREAM_NAME,
    STREAM_SUBJECTS,
    SUBJECT,
    close_nats_connection,
    create_job_consumer_async_task,
)
from settings import get_settings
from src.extraction import ExtractionStatus, LLMExtraction
from src.io import Extraction
from src.pipeline import run_extraction_pipeline

# Add direct print statements for Docker logs
print("DIRECT LOG: Starting application initialization", flush=True)
print(f"DIRECT LOG: Python version: {sys.version}", flush=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
for handler in logging.root.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.flush = sys.stdout.flush

logger = logging.getLogger("extraction-api")

# Set SQLAlchemy logging level to WARNING
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)


# =============================================================================
# Request/Response Models
# =============================================================================


class ExtractionRequest(BaseModel):
    extraction_id: str = Field(..., description="ID of the extraction to process")


class ExtractionResponse(BaseModel):
    id: str
    extraction_id: str
    status: str
    extraction_result: dict | None = None
    summary_en: str | None = None
    summary_id: str | None = None
    created_at: str
    updated_at: str


class ExtractionListResponse(BaseModel):
    total: int
    items: list[ExtractionResponse]


class ExtractionStatusResponse(BaseModel):
    extraction_id: str
    status: str
    message: str


class JobSubmitResponse(BaseModel):
    message: str
    extraction_id: str
    estimated_processing_time_seconds: float


class HealthResponse(BaseModel):
    status: str
    nats_connected: bool
    database_connected: bool


class BatchExtractionRequest(BaseModel):
    concurrency: int = Field(
        default=5, ge=1, le=20, description="Number of concurrent extractions"
    )
    limit: int | None = Field(
        default=None, ge=1, description="Maximum number of extractions to process"
    )


class BatchExtractionResponse(BaseModel):
    message: str
    total_pending: int
    processing: int
    estimated_time_seconds: float


# =============================================================================
# App Context & Processing Times
# =============================================================================

CONTEXTS = AppContexts()


async def get_db_only_contexts() -> AppContexts:
    """Get app contexts with database only (no NATS initialization)."""
    return await CONTEXTS.get_app_contexts(init_nats=False)


async def get_full_contexts() -> AppContexts:
    """Get app contexts with NATS initialization."""
    return await CONTEXTS.get_app_contexts(init_nats=True)


PROCESSING_TIMES = {
    "total": deque(maxlen=100),
    "extraction": deque(maxlen=100),
}


def get_time_estimate() -> dict[str, float]:
    estimates = {}
    if len(PROCESSING_TIMES["total"]) >= 5:
        estimates["total"] = median(PROCESSING_TIMES["total"])
    elif len(PROCESSING_TIMES["total"]) > 0:
        estimates["total"] = mean(PROCESSING_TIMES["total"])
    else:
        estimates["total"] = 120.0  # Default estimate

    if len(PROCESSING_TIMES["extraction"]) >= 5:
        estimates["extraction"] = median(PROCESSING_TIMES["extraction"])
    elif len(PROCESSING_TIMES["extraction"]) > 0:
        estimates["extraction"] = mean(PROCESSING_TIMES["extraction"])
    else:
        estimates["extraction"] = 100.0
    return estimates


def update_processing_times(stage: str, duration: float) -> None:
    if stage in PROCESSING_TIMES:
        PROCESSING_TIMES[stage].append(duration)
        logger.debug(f"Updated {stage} time: {duration:.2f}s")


# =============================================================================
# Lifespan (Startup/Shutdown)
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    global CONTEXTS
    print("DIRECT LOG: Starting up extraction API", flush=True)
    logger.info("Starting up extraction API")
    nats_consumer_job_connection = []

    try:
        contexts = await CONTEXTS.get_app_contexts()

        # Ensure NATS stream exists
        try:
            js = contexts.nats_client.jetstream()
            await js.add_stream(name=STREAM_NAME, subjects=[STREAM_SUBJECTS])
            logger.info(f"Stream {STREAM_NAME} confirmed")
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Stream creation warning: {e}")

        # Create NATS consumer tasks
        num_consumers = get_settings().nats__num_of_summarizer_consumer_instances
        logger.info(f"Creating {num_consumers} NATS consumer instances")

        consumer_tasks = create_job_consumer_async_task(
            nats_client=contexts.nats_client,
            jetstream_client=contexts.jetstream_client,
            consumer_config=CONSUMER_CONFIG,
            processing_func=process_nats_message,
            num_of_consumer_instances=num_consumers,
        )
        nats_consumer_job_connection.extend(consumer_tasks)
        logger.info("Startup completed successfully")

    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down extraction API")
    for task in nats_consumer_job_connection:
        try:
            await close_nats_connection(task)
        except Exception as e:
            logger.error(f"Error closing NATS connection: {e}")
    logger.info("Shutdown completed")


app = FastAPI(
    title="Court Decision Extraction API",
    description="API for extracting structured data from Indonesian Supreme Court decisions",
    version="2.0.0",
    lifespan=lifespan,
)


# =============================================================================
# NATS Message Processor
# =============================================================================


async def process_nats_message(msg: Msg) -> None:
    """Process extraction request from NATS queue."""
    global CONTEXTS
    logger.info("Processing NATS message")
    total_start_time = asyncio.get_event_loop().time()
    extraction_id = None

    try:
        contexts = await CONTEXTS.get_app_contexts(init_nats=True)
        data = json.loads(msg.data.decode())
        extraction_id = data.get("extraction_id")

        if not extraction_id:
            logger.error(f"Missing extraction_id in message: {data}")
            await msg.ack()
            return

        logger.info(f"Processing extraction: {extraction_id}")

        # Check if extraction is already completed (idempotency check)
        async_session = async_sessionmaker(
            bind=contexts.crawler_db_engine, class_=AsyncSession
        )
        async with async_session() as session:
            result = await session.execute(
                select(LLMExtraction).where(
                    LLMExtraction.extraction_id == extraction_id
                )
            )
            existing = result.scalar_one_or_none()
            if existing and existing.status == ExtractionStatus.COMPLETED.value:
                logger.info(
                    f"Extraction {extraction_id} already completed, skipping"
                )
                await msg.ack()
                return

        extract_start = asyncio.get_event_loop().time()
        extraction_result, summary_id, summary_en, decision_number = (
            await run_extraction_pipeline(
                extraction_id=extraction_id,
                crawler_db_engine=contexts.crawler_db_engine,
                case_db_engine=contexts.case_db_engine,
            )
        )
        extract_duration = asyncio.get_event_loop().time() - extract_start
        update_processing_times("extraction", extract_duration)

        logger.info(f"Completed extraction for {decision_number}")
        logger.info(f"Extracted {len(extraction_result.model_dump(exclude_none=True))} fields")
        await msg.ack()

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in message: {e}")
        await msg.ack()
    except Exception as e:
        logger.error(f"Failed to process extraction {extraction_id}: {e}")
        await msg.ack()
    finally:
        total_duration = asyncio.get_event_loop().time() - total_start_time
        logger.info(f"Total processing time: {total_duration:.2f}s")
        if extraction_id:
            update_processing_times("total", total_duration)


# =============================================================================
# API Endpoints
# =============================================================================


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(
    app_contexts: Annotated[AppContexts, Depends(get_full_contexts)],
) -> HealthResponse:
    """Check API health status."""
    nats_connected = (
        app_contexts.nats_client is not None
        and app_contexts.nats_client.is_connected
    )

    # Check database connection
    db_connected = False
    try:
        async_session = async_sessionmaker(
            bind=app_contexts.crawler_db_engine, class_=AsyncSession
        )
        async with async_session() as session:
            await session.execute(select(1))
            db_connected = True
    except Exception:
        pass

    return HealthResponse(
        status="healthy" if (nats_connected and db_connected) else "degraded",
        nats_connected=nats_connected,
        database_connected=db_connected,
    )


@app.post(
    "/extractions",
    response_model=JobSubmitResponse,
    tags=["Extractions"],
    summary="Submit extraction job",
)
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def submit_extraction(
    payload: ExtractionRequest,
    app_contexts: Annotated[AppContexts, Depends(get_full_contexts)],
) -> JobSubmitResponse:
    """
    Submit extraction job to NATS queue for processing.

    Returns immediately with job info. Check status via GET /extractions/{id}/status.
    """
    logger.info(f"Submitting async extraction: {payload.extraction_id}")

    # Create PENDING entry in database immediately
    async_session = async_sessionmaker(
        bind=app_contexts.crawler_db_engine, class_=AsyncSession
    )
    async with async_session() as session:
        # Check if already exists
        result = await session.execute(
            select(LLMExtraction).where(
                LLMExtraction.extraction_id == payload.extraction_id
            )
        )
        existing = result.scalar_one_or_none()

        if not existing:
            # Create new PENDING record
            llm_extraction = LLMExtraction(
                extraction_id=payload.extraction_id,
                status=ExtractionStatus.PENDING.value,
            )
            session.add(llm_extraction)
            await session.commit()
            logger.info(f"Created PENDING record for: {payload.extraction_id}")
        else:
            # Reset status to PENDING for re-processing
            existing.status = ExtractionStatus.PENDING.value
            session.add(existing)
            await session.commit()
            logger.info(f"Reset status to PENDING for: {payload.extraction_id}")

    try:
        nats_client: NATS = app_contexts.nats_client
        if not nats_client.is_connected:
            app_contexts = await CONTEXTS.get_app_contexts(init_nats=True)
            nats_client = app_contexts.nats_client

        js = nats_client.jetstream()

        try:
            await js.add_stream(name=STREAM_NAME, subjects=[STREAM_SUBJECTS])
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Stream creation warning: {e}")

        json_payload = payload.model_dump_json()
        await js.publish(SUBJECT, json_payload.encode())
        logger.info(f"Published to NATS: {payload.extraction_id}")

        estimates = get_time_estimate()
        return JobSubmitResponse(
            message="Extraction job queued",
            extraction_id=payload.extraction_id,
            estimated_processing_time_seconds=estimates["total"],
        )

    except Exception as e:
        logger.error(f"Failed to queue extraction: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Failed to queue extraction: {str(e)}",
        )


@app.get(
    "/extractions/{extraction_id}",
    response_model=ExtractionResponse,
    tags=["Extractions"],
    summary="Get extraction result",
)
async def get_extraction(
    extraction_id: str,
    app_contexts: Annotated[AppContexts, Depends(get_db_only_contexts)],
) -> ExtractionResponse:
    """Get extraction result by extraction_id."""
    async_session = async_sessionmaker(
        bind=app_contexts.crawler_db_engine, class_=AsyncSession
    )

    async with async_session() as session:
        result = await session.execute(
            select(LLMExtraction).where(LLMExtraction.extraction_id == extraction_id)
        )
        record = result.scalar_one_or_none()

    if not record:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Extraction not found: {extraction_id}",
        )

    return ExtractionResponse(
        id=record.id,
        extraction_id=record.extraction_id,
        status=record.status,
        extraction_result=record.extraction_result,
        summary_en=record.summary_en,
        summary_id=record.summary_id,
        created_at=record.created_at.isoformat(),
        updated_at=record.updated_at.isoformat(),
    )


@app.get(
    "/extractions/{extraction_id}/status",
    response_model=ExtractionStatusResponse,
    tags=["Extractions"],
    summary="Get extraction status",
)
async def get_extraction_status(
    extraction_id: str,
    app_contexts: Annotated[AppContexts, Depends(get_db_only_contexts)],
) -> ExtractionStatusResponse:
    """Get extraction status by extraction_id."""
    async_session = async_sessionmaker(
        bind=app_contexts.crawler_db_engine, class_=AsyncSession
    )

    async with async_session() as session:
        result = await session.execute(
            select(LLMExtraction).where(LLMExtraction.extraction_id == extraction_id)
        )
        record = result.scalar_one_or_none()

    if not record:
        return ExtractionStatusResponse(
            extraction_id=extraction_id,
            status="not_found",
            message="Extraction not started or does not exist",
        )

    messages = {
        ExtractionStatus.PENDING.value: "Extraction is pending",
        ExtractionStatus.PROCESSING.value: "Extraction is in progress",
        ExtractionStatus.COMPLETED.value: "Extraction completed successfully",
        ExtractionStatus.FAILED.value: "Extraction failed",
    }

    return ExtractionStatusResponse(
        extraction_id=extraction_id,
        status=record.status,
        message=messages.get(record.status, "Unknown status"),
    )


@app.get(
    "/extractions",
    response_model=ExtractionListResponse,
    tags=["Extractions"],
    summary="List extractions",
)
async def list_extractions(
    app_contexts: Annotated[AppContexts, Depends(get_db_only_contexts)],
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> ExtractionListResponse:
    """List all extractions with optional filtering."""
    async_session = async_sessionmaker(
        bind=app_contexts.crawler_db_engine, class_=AsyncSession
    )

    async with async_session() as session:
        # Build query
        query = select(LLMExtraction)
        if status:
            query = query.where(LLMExtraction.status == status)
        query = query.order_by(LLMExtraction.created_at.desc())
        query = query.offset(offset).limit(limit)

        result = await session.execute(query)
        records = result.scalars().all()

        # Get total count
        count_query = select(func.count()).select_from(LLMExtraction)
        if status:
            count_query = count_query.where(LLMExtraction.status == status)
        count_result = await session.execute(count_query)
        total = count_result.scalar_one()

    items = [
        ExtractionResponse(
            id=r.id,
            extraction_id=r.extraction_id,
            status=r.status,
            extraction_result=r.extraction_result,
            summary_en=r.summary_en,
            summary_id=r.summary_id,
            created_at=r.created_at.isoformat(),
            updated_at=r.updated_at.isoformat(),
        )
        for r in records
    ]

    return ExtractionListResponse(total=total, items=items)


@app.delete(
    "/extractions/{extraction_id}",
    tags=["Extractions"],
    summary="Delete extraction",
)
async def delete_extraction(
    extraction_id: str,
    app_contexts: Annotated[AppContexts, Depends(get_db_only_contexts)],
) -> dict:
    """Delete an extraction record."""
    async_session = async_sessionmaker(
        bind=app_contexts.crawler_db_engine, class_=AsyncSession
    )

    async with async_session() as session:
        result = await session.execute(
            select(LLMExtraction).where(LLMExtraction.extraction_id == extraction_id)
        )
        record = result.scalar_one_or_none()

        if not record:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"Extraction not found: {extraction_id}",
            )

        await session.delete(record)
        await session.commit()

    return {"message": f"Extraction {extraction_id} deleted"}


# =============================================================================
# Batch Extraction Endpoints
# =============================================================================


async def get_pending_extraction_ids(
    crawler_db_engine: AsyncEngine, limit: int | None = None
) -> list[str]:
    """
    Get extraction IDs that don't have LLM extraction results yet.

    Uses a NOT IN subquery to find extractions without corresponding
    llm_extractions records (with COMPLETED or PROCESSING status).
    Only includes extractions where raw_page_link starts with 'https://putusan3'.
    """
    async_session = async_sessionmaker(bind=crawler_db_engine, class_=AsyncSession)

    async with async_session() as session:
        # Subquery for existing LLM extractions with COMPLETED or PROCESSING status
        existing_subquery = select(LLMExtraction.extraction_id).where(
            LLMExtraction.status.in_([
                ExtractionStatus.COMPLETED.value,
                ExtractionStatus.PROCESSING.value,
            ])
        )

        # Main query: get extractions not in the existing subquery
        query = select(Extraction.id).where(
            Extraction.raw_page_link.startswith("https://putusan3"),
            Extraction.id.not_in(existing_subquery),
        )

        if limit:
            query = query.limit(limit)

        result = await session.execute(query)
        return [row[0] for row in result.fetchall()]


@app.post(
    "/extractions/batch",
    response_model=BatchExtractionResponse,
    tags=["Batch Extractions"],
    summary="Submit all pending extractions to NATS queue",
)
async def submit_batch_extraction_async(
    payload: BatchExtractionRequest,
    app_contexts: Annotated[AppContexts, Depends(get_full_contexts)],
) -> BatchExtractionResponse:
    """
    Submit all pending extractions to NATS queue for processing.

    Returns immediately with job info. Use /extractions endpoint to check progress.
    """
    logger.info(f"Submitting batch extraction with limit={payload.limit}")

    # Get pending extraction IDs
    pending_ids = await get_pending_extraction_ids(
        crawler_db_engine=app_contexts.crawler_db_engine,
        limit=payload.limit,
    )

    if not pending_ids:
        return BatchExtractionResponse(
            message="No pending extractions found",
            total_pending=0,
            processing=0,
            estimated_time_seconds=0,
        )

    # Create PENDING records and publish to NATS
    async_session = async_sessionmaker(
        bind=app_contexts.crawler_db_engine, class_=AsyncSession
    )

    nats_client: NATS = app_contexts.nats_client
    if not nats_client.is_connected:
        app_contexts = await CONTEXTS.get_app_contexts(init_nats=True)
        nats_client = app_contexts.nats_client

    js = nats_client.jetstream()

    try:
        await js.add_stream(name=STREAM_NAME, subjects=[STREAM_SUBJECTS])
    except Exception as e:
        if "already exists" not in str(e):
            logger.warning(f"Stream creation warning: {e}")

    published_count = 0
    for extraction_id in pending_ids:
        try:
            # Create or update PENDING record
            async with async_session() as session:
                result = await session.execute(
                    select(LLMExtraction).where(
                        LLMExtraction.extraction_id == extraction_id
                    )
                )
                existing = result.scalar_one_or_none()

                if not existing:
                    llm_extraction = LLMExtraction(
                        extraction_id=extraction_id,
                        status=ExtractionStatus.PENDING.value,
                    )
                    session.add(llm_extraction)
                    await session.commit()
                else:
                    # Reset status to PENDING for re-processing
                    existing.status = ExtractionStatus.PENDING.value
                    session.add(existing)
                    await session.commit()

            # Publish to NATS
            json_payload = ExtractionRequest(extraction_id=extraction_id).model_dump_json()
            await js.publish(SUBJECT, json_payload.encode())
            published_count += 1

        except Exception as e:
            logger.error(f"Failed to queue {extraction_id}: {e}")

    logger.info(f"Published {published_count}/{len(pending_ids)} extractions to NATS")

    estimates = get_time_estimate()
    estimated_time = len(pending_ids) * estimates["total"]

    return BatchExtractionResponse(
        message=f"Batch extraction queued: {published_count} jobs",
        total_pending=len(pending_ids),
        processing=published_count,
        estimated_time_seconds=estimated_time,
    )


@app.get(
    "/extractions/pending/count",
    tags=["Batch Extractions"],
    summary="Get count of pending extractions",
)
async def get_pending_count(
    app_contexts: Annotated[AppContexts, Depends(get_full_contexts)],
) -> dict:
    """Get the number of extractions pending LLM processing."""
    pending_ids = await get_pending_extraction_ids(
        crawler_db_engine=app_contexts.crawler_db_engine,
    )
    return {
        "pending_count": len(pending_ids),
        "message": f"{len(pending_ids)} extractions pending LLM processing",
    }
