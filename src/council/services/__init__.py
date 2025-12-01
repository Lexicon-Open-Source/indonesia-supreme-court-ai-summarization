"""
Services for the Virtual Judicial Council.

Provides business logic including:
- Embedding generation for semantic search
- Case parsing from text to structured data
- Legal opinion generation from deliberations
"""

from src.council.services.case_parser import (
    CaseParserService,
    get_case_parser_service,
)
from src.council.services.embeddings import (
    CouncilEmbeddingService,
    get_council_embedding_service,
)
from src.council.services.opinion_generator import (
    OpinionGeneratorService,
    get_opinion_generator_service,
)

__all__ = [
    "CouncilEmbeddingService",
    "get_council_embedding_service",
    "CaseParserService",
    "get_case_parser_service",
    "OpinionGeneratorService",
    "get_opinion_generator_service",
]
