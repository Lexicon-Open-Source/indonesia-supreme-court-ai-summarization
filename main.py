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
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from statistics import mean, median
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Query
from fastapi.exceptions import HTTPException
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from sqlalchemy import func, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from tenacity import retry, stop_after_attempt, wait_exponential

from settings import QueueBackendType, _temp_credentials_file, get_settings
from src.council import council_router
from src.council import set_db_engine as set_council_db_engine
from src.embedding import (
    SearchResult,
    ensure_pgvector_extension,
    find_similar_cases,
    get_extractions_needing_embeddings,
    semantic_search,
)
from src.extraction import ExtractionStatus, LLMExtraction
from src.io import Extraction
from src.queue import (
    EmbeddingHandler,
    ExtractionHandler,
    QueueSubject,
)
from src.queue.base import BaseConsumer, BaseProducer, QueueBackend
from src.queue.factory import (
    QueueConfig,
    create_consumer,
    create_producer_from_consumer,
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
# Semantic Search Request/Response Models
# =============================================================================


class SemanticSearchRequest(BaseModel):
    query: str = Field(..., description="Search query text")
    limit: int = Field(default=10, ge=1, le=100, description="Max results")
    crime_category: str | None = Field(
        default=None, description="Filter by crime category"
    )
    search_type: str = Field(
        default="content",
        description="Embedding: 'content', 'summary_id', or 'summary_en'",
    )


class SemanticSearchResponse(BaseModel):
    query: str
    results: list[SearchResult]
    total: int


class SimilarCasesResponse(BaseModel):
    extraction_id: str
    similar_cases: list[SearchResult]
    total: int


# =============================================================================
# Application State
# =============================================================================


class AppState:
    """Application state container."""

    def __init__(self):
        self.consumer: BaseConsumer | None = None
        self.embedding_consumer: BaseConsumer | None = None
        self.producer: BaseProducer | None = None
        self.queue_config: QueueConfig | None = None
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
        settings.get_database_url(),
        future=True,
        connect_args=settings.get_connect_args(),
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


async def get_producer() -> BaseProducer:
    """Get queue producer."""
    if app_state.producer is None:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Queue producer not initialized",
        )
    return app_state.producer


async def get_consumer() -> BaseConsumer:
    """Get queue consumer."""
    if app_state.consumer is None:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Queue consumer not initialized",
        )
    return app_state.consumer


# =============================================================================
# Lifespan (Startup/Shutdown)
# =============================================================================


def _build_nats_kwargs(settings: Any) -> dict[str, Any]:
    """Build NATS-specific consumer/producer kwargs."""
    from src.queue.config import ConsumerSettings, StreamSettings, WorkerSettings

    return {
        "stream_settings": StreamSettings(),
        "consumer_settings": ConsumerSettings(
            ack_wait=settings.nats__ack_wait_seconds,
            max_deliver=3,
            max_ack_pending=10,
        ),
        "worker_settings": WorkerSettings(
            num_workers=settings.get_num_consumer_instances(),
            shutdown_timeout=60.0,
        ),
    }


def _build_pubsub_kwargs(settings: Any) -> dict[str, Any]:
    """Build Pub/Sub-specific consumer/producer kwargs."""
    from src.queue.pubsub_config import (
        PubSubDeadLetterSettings,
        PubSubSubscriptionSettings,
        PubSubTopicSettings,
        PubSubWorkerSettings,
    )

    return {
        "topic_settings": PubSubTopicSettings(
            name=settings.pubsub__topic_name,
        ),
        "subscription_settings": PubSubSubscriptionSettings(
            name=settings.pubsub__subscription_name,
            topic_name=settings.pubsub__topic_name,
            dead_letter_topic=settings.pubsub__dlq_topic_name,
            max_delivery_attempts=3,
        ),
        "dead_letter_settings": PubSubDeadLetterSettings(
            topic_name=settings.pubsub__dlq_topic_name,
            subscription_name=settings.pubsub__dlq_subscription_name,
        ),
        "worker_settings": PubSubWorkerSettings(
            num_workers=settings.get_num_consumer_instances(),
            shutdown_timeout=60.0,
        ),
    }


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Application lifespan manager."""
    logger.info("Starting extraction API")

    settings = get_settings()

    # Initialize database engine
    app_state.crawler_db_engine = create_database_engine()
    logger.info("Database engine initialized")

    # Configure queue backend based on settings
    backend = (
        QueueBackend.PUBSUB
        if settings.queue_backend == QueueBackendType.PUBSUB
        else QueueBackend.NATS
    )

    app_state.queue_config = QueueConfig(
        backend=backend,
        nats_url=settings.nats__url,
        pubsub_project_id=settings.get_pubsub_project_id(),
        num_workers=settings.get_num_consumer_instances(),
        shutdown_timeout=60.0,
    )

    logger.info(f"Using queue backend: {backend.value}")

    # Create extraction handler
    handler = ExtractionHandler(
        crawler_db_engine=app_state.crawler_db_engine,
    )

    # Build backend-specific settings
    extra_kwargs = (
        _build_pubsub_kwargs(settings)
        if backend == QueueBackend.PUBSUB
        else _build_nats_kwargs(settings)
    )

    # Initialize and start consumer
    app_state.consumer = create_consumer(
        app_state.queue_config,
        handler,
        **extra_kwargs,
    )

    try:
        await app_state.consumer.connect()
        await app_state.consumer.start()
        logger.info(
            f"{backend.value.upper()} consumer started with "
            f"{settings.get_num_consumer_instances()} workers"
        )

        # Create producer from the same connection
        producer_key = (
            "topic_settings" if backend == QueueBackend.PUBSUB else "stream_settings"
        )
        producer_kwargs = {producer_key: extra_kwargs.get(producer_key)}

        # Add embedding topic name for Pub/Sub backend
        if backend == QueueBackend.PUBSUB:
            producer_kwargs["embedding_topic_name"] = (
                settings.pubsub__embedding_topic_name
            )

        app_state.producer = create_producer_from_consumer(
            app_state.queue_config,
            app_state.consumer,
            **producer_kwargs,
        )
        await app_state.producer.connect()
        logger.info(f"{backend.value.upper()} producer initialized")

        # Initialize embedding consumer if enabled
        if settings.embedding_enabled:
            embedding_handler = EmbeddingHandler(
                crawler_db_engine=app_state.crawler_db_engine,
            )

            # Build embedding-specific consumer settings
            if backend == QueueBackend.NATS:
                from src.queue.config import (
                    ConsumerSettings,
                    StreamSettings,
                    WorkerSettings,
                )

                embedding_kwargs = {
                    "stream_settings": StreamSettings(),
                    "consumer_settings": ConsumerSettings(
                        durable_name="SUPREME_COURT_EMBEDDING",
                        filter_subject=QueueSubject.EMBEDDING.value,
                        ack_wait=120,
                        max_deliver=3,
                        max_ack_pending=20,
                    ),
                    "worker_settings": WorkerSettings(
                        num_workers=settings.get_num_consumer_instances(),
                        shutdown_timeout=60.0,
                    ),
                }
            else:
                # For Pub/Sub, configure embedding-specific topic and subscription
                from src.queue.pubsub_config import (
                    PubSubSubscriptionSettings,
                    PubSubTopicSettings,
                    PubSubWorkerSettings,
                )

                embedding_kwargs = {
                    "topic_settings": PubSubTopicSettings(
                        name=settings.pubsub__embedding_topic_name,
                    ),
                    "subscription_settings": PubSubSubscriptionSettings(
                        name=settings.pubsub__embedding_subscription_name,
                        topic_name=settings.pubsub__embedding_topic_name,
                        # No DLQ for embedding jobs - they can be retried via backfill
                        dead_letter_topic=None,
                        max_delivery_attempts=5,
                    ),
                    "dead_letter_settings": None,
                    "worker_settings": PubSubWorkerSettings(
                        num_workers=settings.get_num_consumer_instances(),
                        shutdown_timeout=60.0,
                    ),
                }

            app_state.embedding_consumer = create_consumer(
                app_state.queue_config,
                embedding_handler,
                **embedding_kwargs,
            )
            await app_state.embedding_consumer.connect()
            await app_state.embedding_consumer.start()
            logger.info(
                f"{backend.value.upper()} embedding consumer started with "
                f"{settings.get_num_consumer_instances()} workers"
            )

    except Exception as e:
        logger.error(f"Failed to initialize {backend.value.upper()}: {e}")
        raise

    # Start stale record recovery background task
    app_state._stale_recovery_task = asyncio.create_task(
        stale_record_recovery_loop(),
        name="stale-record-recovery",
    )
    logger.info("Stale record recovery task started")

    # Initialize council feature with database engine
    set_council_db_engine(app_state.crawler_db_engine)
    logger.info("Virtual Judicial Council initialized")

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

    if app_state.embedding_consumer:
        await app_state.embedding_consumer.shutdown()

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

# Include the Virtual Judicial Council router
app.include_router(
    council_router,
    prefix="/council",
    tags=["Virtual Judicial Council"],
)


# =============================================================================
# Health Endpoints
# =============================================================================


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
) -> HealthResponse:
    """Check API health status."""
    # Check queue connection
    queue_connected = app_state.consumer is not None and app_state.consumer.is_connected

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
        status="healthy" if (queue_connected and db_connected) else "degraded",
        nats_connected=queue_connected,  # Kept for backwards compatibility
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
    producer: Annotated[BaseProducer, Depends(get_producer)],
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
    producer: Annotated[BaseProducer, Depends(get_producer)],
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
    "/queue/diagnostics",
    tags=["Diagnostics"],
    summary="Get queue stream and consumer diagnostics",
)
async def get_queue_diagnostics(
    consumer: Annotated[BaseConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """Get queue stream/topic and consumer/subscription state for debugging."""
    stats = await consumer.get_queue_stats()
    # Add backend info
    if app_state.queue_config:
        stats["backend"] = app_state.queue_config.backend.value
    return stats


@app.post(
    "/queue/consumer/reset",
    tags=["Diagnostics"],
    summary="Reset stuck consumer",
)
async def reset_queue_consumer(
    consumer: Annotated[BaseConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """
    Reset the consumer.

    For NATS: deletes and recreates the consumer.
    For Pub/Sub: seeks subscription to current time.
    Use when messages are stuck from crashed consumers.
    """
    return await consumer.reset_consumer()


@app.post(
    "/queue/test-publish",
    tags=["Diagnostics"],
    summary="Test publishing a message",
)
async def test_queue_publish(
    producer: Annotated[BaseProducer, Depends(get_producer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """Test publishing a single message and verify it's stored."""
    settings = get_settings()

    # Determine the subject/topic based on backend
    subject = QueueSubject.EXTRACTION.value
    if settings.queue_backend == QueueBackendType.PUBSUB:
        subject = settings.pubsub__topic_name

    result = await producer.publish(
        subject=subject,
        payload={"extraction_id": "test-message-123", "test": True},
    )

    return {
        "success": result.success,
        "stream": result.stream,
        "sequence": result.sequence,
        "message_id": result.message_id,
        "duplicate": result.duplicate,
        "error": result.error,
    }


@app.get(
    "/metrics",
    tags=["Diagnostics"],
    summary="Get processing metrics",
)
async def get_metrics(
    consumer: Annotated[BaseConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """Get consumer processing metrics."""
    metrics = {
        "consumer_metrics": consumer.metrics.to_dict(),
        "time_estimates": get_time_estimate(),
    }
    if app_state.queue_config:
        metrics["backend"] = app_state.queue_config.backend.value
    return metrics


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
    "/queue/stream/purge",
    tags=["Diagnostics"],
    summary="Purge all messages from stream/topic",
)
async def purge_queue_stream(
    consumer: Annotated[BaseConsumer, Depends(get_consumer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """
    Purge all messages from the queue stream/topic.

    For NATS: Purges all messages from the stream.
    For Pub/Sub: Seeks subscription to current time (skips unacked messages).

    Use this after a consumer reset to clear old/processed messages.
    WARNING: This removes/skips ALL messages including unprocessed ones.
    """
    settings = get_settings()

    if settings.queue_backend == QueueBackendType.PUBSUB:
        # For Pub/Sub, use the reset_consumer which seeks to current time
        return await consumer.reset_consumer()

    # For NATS, use the stream purge
    from src.queue.consumer import NatsConsumer

    if not isinstance(consumer, NatsConsumer):
        return {"error": "Consumer is not a NATS consumer"}

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


# =============================================================================
# Semantic Search Endpoints (AI Agent Council)
# =============================================================================


@app.post(
    "/search/semantic",
    response_model=SemanticSearchResponse,
    tags=["Semantic Search"],
    summary="Semantic search across case embeddings",
)
async def search_cases_semantic(
    payload: SemanticSearchRequest,
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> SemanticSearchResponse:
    """
    Perform semantic search across court case embeddings.

    This endpoint allows AI agents to find relevant cases based on meaning,
    not just keyword matching. Useful for:
    - Finding similar precedents
    - Legal research queries
    - Case law analysis

    The search uses cosine similarity with vector embeddings generated
    from case summaries and structured extraction data.
    """
    logger.info(
        f"Semantic search: query='{payload.query[:50]}...', limit={payload.limit}"
    )

    try:
        results = await semantic_search(
            db_engine=crawler_db,
            query=payload.query,
            limit=payload.limit,
            crime_category=payload.crime_category,
            search_type=payload.search_type,
        )

        return SemanticSearchResponse(
            query=payload.query,
            results=results,
            total=len(results),
        )
    except Exception as e:
        logger.error(f"Semantic search failed: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}",
        )


@app.get(
    "/search/similar/{extraction_id}",
    response_model=SimilarCasesResponse,
    tags=["Semantic Search"],
    summary="Find similar cases to a given extraction",
)
async def get_similar_cases(
    extraction_id: str,
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
    limit: int = Query(default=10, ge=1, le=50, description="Max results"),
) -> SimilarCasesResponse:
    """
    Find cases semantically similar to a given extraction.

    This is useful for:
    - Finding precedent cases
    - Comparing similar corruption schemes
    - Identifying related judgments
    """
    logger.info(f"Finding similar cases for: {extraction_id}")

    try:
        results = await find_similar_cases(
            db_engine=crawler_db,
            extraction_id=extraction_id,
            limit=limit,
        )

        return SimilarCasesResponse(
            extraction_id=extraction_id,
            similar_cases=results,
            total=len(results),
        )
    except Exception as e:
        logger.error(f"Similar cases search failed: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}",
        )


@app.get(
    "/embeddings/stats",
    tags=["Semantic Search"],
    summary="Get embedding statistics",
)
async def get_embedding_stats(
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, Any]:
    """Get statistics about the case embeddings stored in llm_extractions."""
    async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)

    async with async_session() as session:
        # Total extractions with embeddings
        total_result = await session.execute(
            select(func.count())
            .select_from(LLMExtraction)
            .where(
                LLMExtraction.embedding_generated == True  # noqa: E712
            )
        )
        total = total_result.scalar_one()

        # Total extractions without embeddings (pending)
        pending_result = await session.execute(
            select(func.count())
            .select_from(LLMExtraction)
            .where(
                LLMExtraction.embedding_generated == False,  # noqa: E712
                LLMExtraction.status == ExtractionStatus.COMPLETED.value,
            )
        )
        pending = pending_result.scalar_one()

        # By crime category (using JSONB query)
        category_result = await session.execute(
            text("""
                SELECT
                    COALESCE(
                        extraction_result->>'crime_category',
                        extraction_result->'case_metadata'->>'crime_category',
                        'Unknown'
                    ) as crime_category,
                    COUNT(*) as count
                FROM llm_extractions
                WHERE embedding_generated = TRUE
                GROUP BY 1
                ORDER BY count DESC
            """)
        )
        by_category = {
            row.crime_category: row.count for row in category_result.fetchall()
        }

        # By verdict result (using JSONB query)
        verdict_result = await session.execute(
            text("""
                SELECT
                    COALESCE(
                        extraction_result->'verdict'->>'result',
                        'Unknown'
                    ) as verdict_result,
                    COUNT(*) as count
                FROM llm_extractions
                WHERE embedding_generated = TRUE
                GROUP BY 1
                ORDER BY count DESC
            """)
        )
        by_verdict = {
            row.verdict_result: row.count for row in verdict_result.fetchall()
        }

    settings = get_settings()

    return {
        "total_embeddings": total,
        "pending_embeddings": pending,
        "by_crime_category": by_category,
        "by_verdict_result": by_verdict,
        "embedding_config": {
            "model": settings.embedding_model,
            "dimensions": settings.embedding_dimensions,
            "task_type": settings.embedding_task_type,
            "enabled": settings.embedding_enabled,
        },
    }


@app.post(
    "/embeddings/initialize",
    tags=["Semantic Search"],
    summary="Initialize pgvector extension",
)
async def initialize_embeddings(
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> dict[str, str]:
    """
    Initialize the pgvector extension in the database.

    This must be run once before using embedding features.
    Requires superuser privileges on the database.
    """
    try:
        await ensure_pgvector_extension(crawler_db)
        return {"message": "pgvector extension initialized successfully"}
    except Exception as e:
        logger.error(f"Failed to initialize pgvector: {e}")
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Failed to initialize: {str(e)}",
        )


class BackfillRequest(BaseModel):
    limit: int | None = Field(
        default=None, ge=1, description="Max extractions to queue (None = all)"
    )
    concurrency: int = Field(
        default=10, ge=1, le=50, description="Number of concurrent publish operations"
    )
    force: bool = Field(
        default=False, description="Force regeneration even if embedding exists"
    )


class BackfillResponse(BaseModel):
    message: str
    total_pending: int
    queued: int
    failed: int


@app.post(
    "/embeddings/backfill",
    response_model=BackfillResponse,
    tags=["Semantic Search"],
    summary="Queue embedding jobs for existing extractions",
)
async def backfill_embeddings(
    payload: BackfillRequest,
    crawler_db: Annotated[AsyncEngine, Depends(get_crawler_db)],
    producer: Annotated[BaseProducer, Depends(get_producer)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> BackfillResponse:
    """
    Queue embedding jobs for existing extractions that don't have embeddings.

    This endpoint finds completed extractions without embeddings and publishes
    them to the embedding queue for async processing by worker consumers.

    The embedding consumer will:
    1. Validate the extraction exists and is completed
    2. Generate summary embeddings (ID and EN) via Gemini API
    3. Save embeddings directly to llm_extractions table

    Use this to backfill embeddings after enabling the feature on an
    existing database with historical extractions.
    """
    # Find completed extractions without embeddings (or all if force=True)
    if payload.force:
        # If forcing, queue all completed extractions
        async_session = async_sessionmaker(bind=crawler_db, class_=AsyncSession)
        async with async_session() as session:
            query = (
                select(LLMExtraction.extraction_id)
                .where(
                    LLMExtraction.status == ExtractionStatus.COMPLETED.value,
                    LLMExtraction.extraction_result.is_not(None),
                )
                .order_by(LLMExtraction.created_at.desc())
            )
            if payload.limit:
                query = query.limit(payload.limit)
            result = await session.execute(query)
            pending_ids = [row[0] for row in result.fetchall()]
    else:
        # Use the helper function that checks embedding_generated flag
        pending_ids = await get_extractions_needing_embeddings(
            db_engine=crawler_db,
            limit=payload.limit or 1000,
        )

    total_pending = len(pending_ids)
    logger.info(f"Backfill: found {total_pending} extractions to queue for embedding")

    if total_pending == 0:
        return BackfillResponse(
            message="No extractions pending embedding generation",
            total_pending=0,
            queued=0,
            failed=0,
        )

    # Publish to embedding queue
    results = await producer.publish_embedding_batch(
        extraction_ids=pending_ids,
        max_concurrent=payload.concurrency,
        force=payload.force,
    )

    queued = sum(1 for r in results.values() if r.success)
    failed = sum(1 for r in results.values() if not r.success)

    logger.info(f"Backfill: queued {queued}/{total_pending} embedding jobs")

    return BackfillResponse(
        message=f"Embedding jobs queued: {queued} queued, {failed} failed",
        total_pending=total_pending,
        queued=queued,
        failed=failed,
    )
