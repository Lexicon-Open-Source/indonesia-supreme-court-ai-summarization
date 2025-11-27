"""FastAPI application for Court Decision Extraction API.

This module provides:
- REST API endpoints for submitting and managing extractions
- NATS JetStream consumer integration for async processing
- Health and diagnostic endpoints
"""

import asyncio
import logging
import os
import sys
from collections import deque
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from http import HTTPStatus
from statistics import mean, median
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Query
from fastapi.exceptions import HTTPException
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from tenacity import retry, stop_after_attempt, wait_exponential

from datetime import datetime, timedelta, timezone

from settings import _temp_credentials_file, get_settings
from src.extraction import ExtractionStatus, LLMExtraction
from src.io import Extraction
from src.queue import (
    ConsumerSettings,
    ExtractionHandler,
    NatsConfig,
    NatsConsumer,
    NatsProducer,
    QueueSubject,
    StreamSettings,
    WorkerSettings,
)

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

# Reduce SQLAlchemy and httpx logging noise
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
        default=5, ge=1, le=20, description="Number of concurrent submissions"
    )
    limit: int | None = Field(
        default=None, ge=1, description="Maximum number of extractions to queue"
    )


class BatchExtractionResponse(BaseModel):
    message: str
    total_pending: int
    processing: int
    estimated_time_seconds: float


# =============================================================================
# Application State
# =============================================================================


class AppState:
    """Application state container."""

    def __init__(self):
        self.consumer: NatsConsumer | None = None
        self.producer: NatsProducer | None = None
        self.crawler_db_engine: AsyncEngine | None = None
        self.processing_times: dict[str, deque] = {
            "total": deque(maxlen=100),
            "extraction": deque(maxlen=100),
        }
        self._stale_recovery_task: asyncio.Task | None = None
        self._shutdown_event = asyncio.Event()


app_state = AppState()


def get_time_estimate() -> dict[str, float]:
    """Get estimated processing times based on recent history."""
    estimates = {}
    for key in ["total", "extraction"]:
        times = app_state.processing_times[key]
        if len(times) >= 5:
            estimates[key] = median(times)
        elif len(times) > 0:
            estimates[key] = mean(times)
        else:
            estimates[key] = 120.0 if key == "total" else 100.0
    return estimates


def update_processing_time(stage: str, duration: float) -> None:
    """Record a processing time measurement."""
    if stage in app_state.processing_times:
        app_state.processing_times[stage].append(duration)


# Stale record recovery settings
STALE_RECORD_TIMEOUT_MINUTES = 30  # Records stuck in PROCESSING for > 30 min
STALE_RECOVERY_INTERVAL_SECONDS = 300  # Check every 5 minutes


async def recover_stale_processing_records() -> int:
    """
    Find and reset records stuck in PROCESSING status.

    Returns the number of records recovered.
    """
    if app_state.crawler_db_engine is None:
        return 0

    async_session = async_sessionmaker(
        bind=app_state.crawler_db_engine, class_=AsyncSession
    )

    cutoff_time = datetime.now(timezone.utc) - timedelta(
        minutes=STALE_RECORD_TIMEOUT_MINUTES
    )

    async with async_session() as session:
        # Find stale PROCESSING records
        result = await session.execute(
            select(LLMExtraction).where(
                LLMExtraction.status == ExtractionStatus.PROCESSING.value,
                LLMExtraction.updated_at < cutoff_time,
            )
        )
        stale_records = result.scalars().all()

        if not stale_records:
            return 0

        # Reset them to PENDING so they can be reprocessed
        for record in stale_records:
            logger.warning(
                f"Recovering stale record: {record.extraction_id} "
                f"(stuck since {record.updated_at})"
            )
            record.status = ExtractionStatus.PENDING.value
            record.updated_at = datetime.now(timezone.utc)
            session.add(record)

        await session.commit()
        logger.info(f"Recovered {len(stale_records)} stale PROCESSING records")
        return len(stale_records)


async def stale_record_recovery_loop() -> None:
    """Background task that periodically recovers stale records."""
    logger.info(
        f"Starting stale record recovery (interval={STALE_RECOVERY_INTERVAL_SECONDS}s, "
        f"timeout={STALE_RECORD_TIMEOUT_MINUTES}min)"
    )

    while not app_state._shutdown_event.is_set():
        try:
            await asyncio.sleep(STALE_RECOVERY_INTERVAL_SECONDS)
            if app_state._shutdown_event.is_set():
                break
            recovered = await recover_stale_processing_records()
            if recovered > 0:
                logger.info(f"Stale recovery: reset {recovered} stuck records")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in stale record recovery: {e}")

    logger.info("Stale record recovery stopped")


# =============================================================================
# Database Setup
# =============================================================================


def create_database_engine() -> AsyncEngine:
    """Create database engine for crawler database."""
    from sqlalchemy.ext.asyncio import create_async_engine

    settings = get_settings()

    crawler_engine = create_async_engine(
        f"postgresql+asyncpg://{settings.crawler_db_user}:{settings.crawler_db_pass}"
        f"@{settings.crawler_db_addr}/postgres",
        future=True,
        connect_args={"server_settings": {"search_path": settings.crawler_db_schema}},
    )

    return crawler_engine


# =============================================================================
# API Key Authentication
# =============================================================================

API_KEY_HEADER = APIKeyHeader(name="X-LEXICON-API-KEY", auto_error=False)


async def verify_api_key(
    api_key: Annotated[str | None, Depends(API_KEY_HEADER)],
) -> str:
    """Verify the API key from X-LEXICON-API-KEY header."""
    if not api_key:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Missing X-LEXICON-API-KEY header",
        )

    if api_key != get_settings().lexicon_api_key:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Invalid API key",
        )

    return api_key


# =============================================================================
# Dependencies
# =============================================================================


async def get_crawler_db() -> AsyncEngine:
    """Get crawler database engine."""
    if app_state.crawler_db_engine is None:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Database not initialized",
        )
    return app_state.crawler_db_engine




async def get_producer() -> NatsProducer:
    """Get NATS producer."""
    if app_state.producer is None:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="NATS not initialized",
        )
    return app_state.producer


async def get_consumer() -> NatsConsumer:
    """Get NATS consumer."""
    if app_state.consumer is None:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="NATS consumer not initialized",
        )
    return app_state.consumer


# =============================================================================
# Lifespan (Startup/Shutdown)
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Application lifespan manager."""
    logger.info("Starting extraction API")

    settings = get_settings()

    # Initialize database engine
    app_state.crawler_db_engine = create_database_engine()
    logger.info("Database engine initialized")

    # Configure NATS
    nats_config = NatsConfig(url=settings.nats__url)

    stream_settings = StreamSettings()

    consumer_settings = ConsumerSettings(
        # ack_wait=300 (5 min) with heartbeat - fast crash recovery
        # heartbeat_interval=60 extends deadline during processing
        max_deliver=3,
        max_ack_pending=10,  # Allow more parallelism
    )

    worker_settings = WorkerSettings(
        num_workers=settings.nats__num_of_summarizer_consumer_instances,
        shutdown_timeout=60.0,  # Wait up to 60s for graceful shutdown
    )

    # Create extraction handler
    handler = ExtractionHandler(
        crawler_db_engine=app_state.crawler_db_engine,
    )

    # Initialize and start consumer
    app_state.consumer = NatsConsumer(
        nats_config=nats_config,
        handler=handler,
        stream_settings=stream_settings,
        consumer_settings=consumer_settings,
        worker_settings=worker_settings,
    )

    try:
        await app_state.consumer.connect()
        await app_state.consumer.start()
        logger.info(f"NATS consumer started with {worker_settings.num_workers} workers")

        # Create producer from the same connection
        app_state.producer = NatsProducer(
            app_state.consumer._nats_client,
            app_state.consumer._jetstream,
            stream_settings,
        )
        logger.info("NATS producer initialized")

    except Exception as e:
        logger.error(f"Failed to initialize NATS: {e}")
        raise

    # Start stale record recovery background task
    app_state._stale_recovery_task = asyncio.create_task(
        stale_record_recovery_loop(),
        name="stale-record-recovery",
    )
    logger.info("Stale record recovery task started")

    logger.info("Startup complete")
    yield

    # Shutdown
    logger.info("Shutting down extraction API")

    # Stop stale record recovery
    app_state._shutdown_event.set()
    if app_state._stale_recovery_task:
        app_state._stale_recovery_task.cancel()
        try:
            await app_state._stale_recovery_task
        except asyncio.CancelledError:
            pass

    if app_state.consumer:
        await app_state.consumer.shutdown()

    # Clean up temporary GCP credentials file
    if _temp_credentials_file and os.path.exists(_temp_credentials_file):
        try:
            os.unlink(_temp_credentials_file)
        except Exception as e:
            logger.warning(f"Failed to remove temporary credentials file: {e}")

    logger.info("Shutdown complete")


app = FastAPI(
    title="Court Decision Extraction API",
    description="API for extracting structured data from Indonesian Supreme Court "
    "decisions",
    version="3.0.0",
    lifespan=lifespan,
)


# =============================================================================
# Health Endpoints
# =============================================================================


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
) -> HealthResponse:
    """Check API health status."""
    # Check NATS connection
    nats_connected = app_state.consumer is not None and app_state.consumer.is_connected

    # Check database connection
    db_connected = False
    try:
        async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)
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


# =============================================================================
# Extraction Endpoints
# =============================================================================


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
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    producer: Annotated[NatsProducer, Depends(get_producer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> JobSubmitResponse:
    """Submit extraction job to NATS queue for processing."""
    logger.info(f"Submitting extraction: {payload.extraction_id}")

    async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)

    # First, validate that the extraction exists in source table
    async with async_session() as session:
        result = await session.execute(
            select(Extraction).where(Extraction.id == payload.extraction_id)
        )
        source_extraction = result.scalar_one_or_none()

        if not source_extraction:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"Extraction {payload.extraction_id} not found in source table",
            )

        if not source_extraction.artifact_link:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Extraction {payload.extraction_id} has no PDF (artifact_link)",
            )

    # Create PENDING entry in database
    async with async_session() as session:
        result = await session.execute(
            select(LLMExtraction).where(
                LLMExtraction.extraction_id == payload.extraction_id
            )
        )
        existing = result.scalar_one_or_none()

        if not existing:
            llm_extraction = LLMExtraction(
                extraction_id=payload.extraction_id,
                status=ExtractionStatus.PENDING.value,
            )
            session.add(llm_extraction)
            await session.commit()
            logger.info(f"Created PENDING record for: {payload.extraction_id}")
        else:
            existing.status = ExtractionStatus.PENDING.value
            session.add(existing)
            await session.commit()
            logger.info(f"Reset status to PENDING for: {payload.extraction_id}")

    # Publish to NATS
    result = await producer.publish_extraction(payload.extraction_id)

    if not result.success:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Failed to queue extraction: {result.error}",
        )

    estimates = get_time_estimate()

    # Check if this was a duplicate submission
    message = "Extraction job queued"
    if result.duplicate:
        message = "Extraction job already queued (duplicate)"
        logger.info(f"Duplicate submission for {payload.extraction_id}")

    return JobSubmitResponse(
        message=message,
        extraction_id=payload.extraction_id,
        estimated_processing_time_seconds=estimates["total"],
    )


@app.get(
    "/extractions/{extraction_id}",
    response_model=ExtractionResponse,
    tags=["Extractions"],
    summary="Get extraction result",
)
async def get_extraction(
    extraction_id: str,
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> ExtractionResponse:
    """Get extraction result by extraction_id."""
    async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)

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
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> ExtractionStatusResponse:
    """Get extraction status by extraction_id."""
    async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)

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
        ExtractionStatus.PENDING.value: "Extraction is queued for processing",
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
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> ExtractionListResponse:
    """List all extractions with optional filtering."""
    async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)

    async with async_session() as session:
        query = select(LLMExtraction)
        if status:
            query = query.where(LLMExtraction.status == status)
        query = query.order_by(LLMExtraction.created_at.desc())
        query = query.offset(offset).limit(limit)

        result = await session.execute(query)
        records = result.scalars().all()

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
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict:
    """Delete an extraction record."""
    async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)

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
    """Get extraction IDs without completed LLM extraction results."""
    async_session = async_sessionmaker(bind=crawler_db_engine, class_=AsyncSession)

    async with async_session() as session:
        existing_subquery = select(LLMExtraction.extraction_id).where(
            LLMExtraction.status.in_(
                [
                    ExtractionStatus.COMPLETED.value,
                    ExtractionStatus.PROCESSING.value,
                ]
            )
        )

        query = select(Extraction.id).where(
            Extraction.raw_page_link.startswith("https://putusan3"),
            Extraction.artifact_link.is_not(None),
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
    summary="Submit all pending extractions to queue",
)
async def submit_batch_extraction(
    payload: BatchExtractionRequest,
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    producer: Annotated[NatsProducer, Depends(get_producer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> BatchExtractionResponse:
    """Submit all pending extractions to NATS queue for processing."""
    logger.info(f"Submitting batch extraction with limit={payload.limit}")

    pending_ids = await get_pending_extraction_ids(
        crawler_db_engine=crawler_db,
        limit=payload.limit,
    )

    if not pending_ids:
        return BatchExtractionResponse(
            message="No pending extractions found",
            total_pending=0,
            processing=0,
            estimated_time_seconds=0,
        )

    # Create PENDING records in database
    async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)

    async def create_pending_record(extraction_id: str) -> None:
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
            else:
                existing.status = ExtractionStatus.PENDING.value
                session.add(existing)
            await session.commit()

    # Create pending records with limited concurrency
    semaphore = asyncio.Semaphore(payload.concurrency)

    async def create_with_semaphore(extraction_id: str) -> None:
        async with semaphore:
            await create_pending_record(extraction_id)

    await asyncio.gather(
        *[create_with_semaphore(eid) for eid in pending_ids],
        return_exceptions=True,
    )

    # Publish to NATS
    results = await producer.publish_batch(
        pending_ids, max_concurrent=payload.concurrency
    )

    published_count = sum(1 for r in results.values() if r.success)

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
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict:
    """Get the number of extractions pending LLM processing."""
    pending_ids = await get_pending_extraction_ids(crawler_db_engine=crawler_db)
    return {
        "pending_count": len(pending_ids),
        "message": f"{len(pending_ids)} extractions pending LLM processing",
    }


# =============================================================================
# Diagnostics Endpoints
# =============================================================================


@app.get(
    "/nats/diagnostics",
    tags=["Diagnostics"],
    summary="Get NATS stream and consumer diagnostics",
)
async def get_nats_diagnostics(
    consumer: Annotated[NatsConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """Get NATS stream and consumer state for debugging."""
    return await consumer.get_queue_stats()


@app.post(
    "/nats/consumer/reset",
    tags=["Diagnostics"],
    summary="Reset stuck consumer",
)
async def reset_nats_consumer(
    consumer: Annotated[NatsConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """
    Reset the NATS consumer by deleting and recreating it.

    Use when messages are stuck in "ack_pending" state from crashed consumers.
    """
    return await consumer.reset_consumer()


@app.post(
    "/nats/test-publish",
    tags=["Diagnostics"],
    summary="Test publishing a message",
)
async def test_nats_publish(
    producer: Annotated[NatsProducer, Depends(get_producer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """Test publishing a single message and verify it's stored."""
    result = await producer.publish(
        subject=QueueSubject.EXTRACTION.value,
        payload={"extraction_id": "test-message-123", "test": True},
    )

    return {
        "success": result.success,
        "stream": result.stream,
        "sequence": result.sequence,
        "duplicate": result.duplicate,
        "error": result.error,
    }


@app.get(
    "/metrics",
    tags=["Diagnostics"],
    summary="Get processing metrics",
)
async def get_metrics(
    consumer: Annotated[NatsConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """Get consumer processing metrics."""
    return {
        "consumer_metrics": consumer.metrics.to_dict(),
        "time_estimates": get_time_estimate(),
    }


@app.post(
    "/extractions/recover-stale",
    tags=["Diagnostics"],
    summary="Recover stale PROCESSING records",
)
async def recover_stale_extractions(
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """
    Manually trigger recovery of stale PROCESSING records.

    Records stuck in PROCESSING for more than 30 minutes will be reset
    to PENDING so they can be reprocessed.
    """
    recovered = await recover_stale_processing_records()
    return {
        "message": f"Recovered {recovered} stale records",
        "recovered_count": recovered,
        "stale_timeout_minutes": STALE_RECORD_TIMEOUT_MINUTES,
    }


@app.post(
    "/nats/stream/purge",
    tags=["Diagnostics"],
    summary="Purge all messages from stream",
)
async def purge_nats_stream(
    consumer: Annotated[NatsConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """
    Purge all messages from the NATS stream.

    Use this after a consumer reset to clear old/processed messages.
    WARNING: This removes ALL messages including unprocessed ones.
    """
    if not consumer._jetstream:
        return {"error": "Not connected"}

    try:
        stream_name = consumer.stream_settings.name

        # Get current state
        old_info = await consumer._jetstream.stream_info(stream_name)
        old_count = old_info.state.messages

        # Purge all messages
        await consumer._jetstream.purge_stream(stream_name)

        # Get new state
        new_info = await consumer._jetstream.stream_info(stream_name)
        new_count = new_info.state.messages

        logger.info(f"Purged stream {stream_name}: {old_count} -> {new_count} messages")
        return {
            "success": True,
            "stream": stream_name,
            "messages_purged": old_count - new_count,
            "messages_remaining": new_count,
        }
    except Exception as e:
        logger.error(f"Failed to purge stream: {e}")
        return {"error": str(e)}
