"""
FastAPI routers for the Virtual Judicial Council API.

Provides endpoints for:
- Session management (create, get, list, delete)
- Deliberation (send messages, get responses)
- Opinion generation
- Case search and retrieval
"""

import logging

from fastapi import APIRouter
from sqlalchemy.ext.asyncio import AsyncEngine

from src.council.routers.cases import router as cases_router
from src.council.routers.deliberation import router as deliberation_router
from src.council.routers.sessions import router as sessions_router
from src.council.routers.sessions import set_db_engine as _set_sessions_db

logger = logging.getLogger(__name__)

# Module-level database engine reference
_db_engine: AsyncEngine | None = None

# Create the main council router
council_router = APIRouter()

# Include sub-routers
council_router.include_router(
    sessions_router,
    prefix="/sessions",
    tags=["Council Sessions"],
)
council_router.include_router(
    deliberation_router,
    prefix="/deliberation",
    tags=["Council Deliberation"],
)
council_router.include_router(
    cases_router,
    prefix="/cases",
    tags=["Council Cases"],
)


def set_db_engine(engine: AsyncEngine) -> None:
    """
    Set the database engine for all council routes.

    This must be called during app startup before any routes are used.
    """
    global _db_engine
    _db_engine = engine
    _set_sessions_db(engine)
    logger.info("Council database engine configured")


__all__ = ["council_router", "set_db_engine"]
