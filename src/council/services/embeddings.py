"""
Embedding Service for the Virtual Judicial Council.

Generates text embeddings using litellm with Gemini embedding-001
for semantic similarity search across court cases.

This service wraps the main project's embedding module to provide
council-specific functionality like case data vectorization.
"""

import logging
from typing import Any

from settings import get_settings
from src.embedding import (
    EmbeddingConfig,
    generate_embedding,
    generate_query_embedding,
    prepare_content_text,
)

logger = logging.getLogger(__name__)


class CouncilEmbeddingService:
    """
    Embedding service for the Council feature.

    Uses gemini-embedding-001 via litellm for generating embeddings.
    Wraps the main embedding module with council-specific methods.
    """

    def __init__(self):
        """Initialize the embedding service with settings."""
        settings = get_settings()
        self.config = EmbeddingConfig(
            model=settings.embedding_model,
            dimensions=settings.embedding_dimensions,
            task_type=settings.embedding_task_type,
        )
        logger.info(
            f"Council embedding service initialized: "
            f"model={self.config.model}, dims={self.config.dimensions}"
        )

    async def generate_embedding(self, text: str) -> list[float]:
        """
        Generate an embedding for a single text.

        Args:
            text: Input text to embed (max ~8000 chars recommended)

        Returns:
            List of floats representing the embedding vector
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for embedding")
            return []

        # Truncate if too long
        max_length = 8000
        if len(text) > max_length:
            logger.warning(
                f"Text too long ({len(text)} chars), truncating to {max_length}"
            )
            text = text[:max_length]

        return await generate_embedding(text, self.config)

    async def generate_query_embedding(self, query: str) -> list[float]:
        """
        Generate an embedding optimized for search queries.

        Uses RETRIEVAL_QUERY task type for better search performance.

        Args:
            query: Search query text

        Returns:
            Query embedding vector
        """
        if not query or not query.strip():
            logger.warning("Empty query provided for embedding")
            return []

        return await generate_query_embedding(query, self.config)

    def build_search_text(self, case_data: dict[str, Any]) -> str:
        """
        Build searchable text from case data for embedding.

        Combines key fields from case data into a text representation
        optimized for semantic search.

        Args:
            case_data: Parsed case information dict

        Returns:
            Concatenated text suitable for embedding
        """
        parts = []

        # Case type
        if case_data.get("case_type"):
            case_type = case_data["case_type"]
            if hasattr(case_type, "value"):
                case_type = case_type.value
            parts.append(f"Case type: {case_type}")

        # Summary
        if case_data.get("summary"):
            parts.append(f"Summary: {case_data['summary']}")

        # Defendant profile
        defendant = case_data.get("defendant_profile") or {}
        if isinstance(defendant, dict):
            if defendant.get("is_first_offender") is not None:
                status = (
                    "first offender"
                    if defendant["is_first_offender"]
                    else "repeat offender"
                )
                parts.append(f"Defendant: {status}")
            if defendant.get("age"):
                parts.append(f"Age: {defendant['age']}")

        # Key facts
        if case_data.get("key_facts"):
            facts = case_data["key_facts"][:5]  # Limit to 5 facts
            parts.append(f"Facts: {'. '.join(facts)}")

        # Charges
        if case_data.get("charges"):
            charges = case_data["charges"][:3]  # Limit to 3 charges
            parts.append(f"Charges: {', '.join(charges)}")

        # Narcotics details
        narcotics = case_data.get("narcotics")
        if narcotics:
            if isinstance(narcotics, dict):
                parts.append(f"Substance: {narcotics.get('substance', 'unknown')}")
                parts.append(f"Weight: {narcotics.get('weight_grams', 0)} grams")
                intent = narcotics.get("intent", "unknown")
                if hasattr(intent, "value"):
                    intent = intent.value
                parts.append(f"Intent: {intent}")
            else:
                # Handle Pydantic model
                parts.append(f"Substance: {narcotics.substance}")
                parts.append(f"Weight: {narcotics.weight_grams} grams")
                parts.append(f"Intent: {narcotics.intent.value}")

        # Corruption details
        corruption = case_data.get("corruption")
        if corruption:
            if isinstance(corruption, dict):
                parts.append(f"State loss: {corruption.get('state_loss_idr', 0)} IDR")
                if corruption.get("position"):
                    parts.append(f"Position: {corruption['position']}")
            else:
                # Handle Pydantic model
                parts.append(f"State loss: {corruption.state_loss_idr} IDR")
                if corruption.position:
                    parts.append(f"Position: {corruption.position}")

        return ". ".join(parts)

    def build_extraction_text(self, extraction_result: dict[str, Any]) -> str:
        """
        Build searchable text from a full extraction result.

        Delegates to the main embedding module's prepare_content_text function.

        Args:
            extraction_result: Full extraction result from LLM extraction

        Returns:
            Text optimized for embedding
        """
        return prepare_content_text(extraction_result)


# =============================================================================
# Singleton
# =============================================================================

_council_embedding_service: CouncilEmbeddingService | None = None


def get_council_embedding_service() -> CouncilEmbeddingService:
    """Get or create the council embedding service singleton."""
    global _council_embedding_service
    if _council_embedding_service is None:
        _council_embedding_service = CouncilEmbeddingService()
    return _council_embedding_service
