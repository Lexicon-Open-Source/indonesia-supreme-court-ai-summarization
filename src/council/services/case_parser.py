"""
Case Parser Service for the Virtual Judicial Council.

Parses free-text case descriptions into structured case data
using LLM extraction with a defined schema.
"""

import json
import logging
from typing import Any

from litellm import acompletion
from tenacity import retry, stop_after_attempt, wait_exponential

from settings import get_settings
from src.council.schemas import (
    CaseInput,
    CaseType,
    CorruptionDetails,
    DefendantProfile,
    InputType,
    NarcoticsDetails,
    NarcoticsIntent,
    ParsedCaseInput,
    StructuredCaseData,
)

logger = logging.getLogger(__name__)


# JSON schema for case parsing
CASE_PARSING_SCHEMA = {
    "type": "object",
    "required": ["case_type", "summary", "key_facts", "charges"],
    "properties": {
        "case_type": {
            "type": "string",
            "enum": ["narcotics", "corruption", "general_criminal", "other"],
            "description": "Type of the legal case",
        },
        "summary": {
            "type": "string",
            "description": "Brief summary of the case (2-3 sentences)",
        },
        "key_facts": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Key factual findings from the case",
        },
        "charges": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Legal charges or articles cited",
        },
        "defendant_profile": {
            "type": "object",
            "properties": {
                "is_first_offender": {
                    "type": "boolean",
                    "description": "Whether this is the defendant's first offense",
                },
                "age": {
                    "type": "integer",
                    "description": "Defendant's age if mentioned",
                },
                "occupation": {
                    "type": "string",
                    "description": "Defendant's occupation if mentioned",
                },
            },
        },
        "narcotics": {
            "type": "object",
            "description": "Details for narcotics cases only",
            "properties": {
                "substance": {"type": "string"},
                "weight_grams": {"type": "number"},
                "intent": {
                    "type": "string",
                    "enum": ["personal_use", "distribution", "unknown"],
                },
            },
        },
        "corruption": {
            "type": "object",
            "description": "Details for corruption cases only",
            "properties": {
                "state_loss_idr": {"type": "number"},
                "position": {"type": "string"},
            },
        },
    },
}


CASE_PARSING_PROMPT = """You are a legal case parser for Indonesian court cases.
Analyze the following case description and extract structured information.

IMPORTANT: Always identify the case type correctly:
- narcotics: Cases involving drugs (narkotika, shabu, ganja, methamphetamine, etc.)
- corruption: Cases involving korupsi, state financial loss (kerugian negara), bribery
- general_criminal: Other criminal cases (theft, assault, fraud, etc.)
- other: Civil or administrative cases

For narcotics cases:
- Extract substance type (shabu/methamphetamine, ganja/marijuana, etc.)
- Extract weight in grams (convert if given in other units)
- Determine intent: personal_use (small amounts, no dealing evidence) vs distribution

For corruption cases:
- Extract state loss amount in IDR
- Extract the defendant's position if mentioned

Case Description:
{case_text}

{structured_context}

Return a valid JSON object matching the required schema.
"""


class CaseParserService:
    """
    Service for parsing free-text case descriptions into structured data.

    Uses Gemini LLM with structured output to extract:
    - Case type classification
    - Key facts and charges
    - Defendant profile
    - Crime-specific details (narcotics/corruption)
    """

    def __init__(self):
        """Initialize the case parser with settings."""
        settings = get_settings()
        self.model = settings.extraction_model  # Use same model as extraction
        logger.info(f"Case parser service initialized: model={self.model}")

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def parse_case(
        self,
        case_text: str,
        structured_data: StructuredCaseData | None = None,
    ) -> CaseInput:
        """
        Parse a case text into structured case input.

        Args:
            case_text: Free-text case description
            structured_data: Optional pre-structured data to augment parsing

        Returns:
            CaseInput with parsed case information

        Raises:
            ValueError: If parsing fails
        """
        if not case_text or len(case_text.strip()) < 50:
            raise ValueError("Case text must be at least 50 characters")

        logger.info(f"Parsing case text: {len(case_text)} chars")

        # Build structured context if provided
        structured_context = ""
        if structured_data:
            context_parts = []
            if structured_data.defendant_age:
                context_parts.append(f"Defendant age: {structured_data.defendant_age}")
            if structured_data.defendant_first_offender is not None:
                context_parts.append(
                    f"First offender: {structured_data.defendant_first_offender}"
                )
            if structured_data.substance_type:
                context_parts.append(
                    f"Substance type: {structured_data.substance_type}"
                )
            if structured_data.weight_grams:
                context_parts.append(
                    f"Weight: {structured_data.weight_grams} grams"
                )
            if structured_data.state_loss_idr:
                context_parts.append(
                    f"State loss: Rp {structured_data.state_loss_idr:,.0f}"
                )
            if context_parts:
                structured_context = "Additional structured data:\n" + "\n".join(
                    context_parts
                )

        # Call LLM with structured output
        prompt = CASE_PARSING_PROMPT.format(
            case_text=case_text,
            structured_context=structured_context,
        )

        try:
            response = await acompletion(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "case_parsing",
                        "schema": CASE_PARSING_SCHEMA,
                        "strict": True,
                    },
                },
            )

            result_text = response.choices[0].message.content
            result = json.loads(result_text)

        except Exception as e:
            logger.error(f"LLM parsing failed: {e}")
            raise ValueError(f"Failed to parse case: {str(e)}")

        # Build parsed case from result
        parsed_case = self._build_parsed_case(result, case_text)

        return CaseInput(
            input_type=InputType.TEXT_SUMMARY,
            raw_input=case_text,
            parsed_case=parsed_case,
        )

    def _build_parsed_case(
        self,
        result: dict[str, Any],
        original_text: str,
    ) -> ParsedCaseInput:
        """Build a ParsedCaseInput from LLM response."""
        # Case type
        case_type_str = result.get("case_type", "other")
        try:
            case_type = CaseType(case_type_str)
        except ValueError:
            case_type = CaseType.OTHER

        # Defendant profile
        defendant_data = result.get("defendant_profile")
        defendant_profile = None
        if defendant_data:
            defendant_profile = DefendantProfile(
                is_first_offender=defendant_data.get("is_first_offender", True),
                age=defendant_data.get("age"),
                occupation=defendant_data.get("occupation"),
            )

        # Narcotics details
        narcotics = None
        narcotics_data = result.get("narcotics")
        if case_type == CaseType.NARCOTICS and narcotics_data:
            intent_str = narcotics_data.get("intent", "unknown")
            try:
                intent = NarcoticsIntent(intent_str)
            except ValueError:
                intent = NarcoticsIntent.UNKNOWN

            narcotics = NarcoticsDetails(
                substance=narcotics_data.get("substance", "unknown"),
                weight_grams=float(narcotics_data.get("weight_grams", 0)),
                intent=intent,
            )

        # Corruption details
        corruption = None
        corruption_data = result.get("corruption")
        if case_type == CaseType.CORRUPTION and corruption_data:
            corruption = CorruptionDetails(
                state_loss_idr=float(corruption_data.get("state_loss_idr", 0)),
                position=corruption_data.get("position"),
            )

        return ParsedCaseInput(
            case_type=case_type,
            summary=result.get("summary", original_text[:500]),
            defendant_profile=defendant_profile,
            key_facts=result.get("key_facts", []),
            charges=result.get("charges", []),
            narcotics=narcotics,
            corruption=corruption,
        )

    async def parse_from_extraction(
        self,
        extraction_result: dict[str, Any],
        summary_id: str | None = None,
        summary_en: str | None = None,
    ) -> CaseInput:
        """
        Parse case input from an existing extraction result.

        Uses the structured extraction data to build a case input
        without needing an additional LLM call.

        Args:
            extraction_result: Full extraction result from LLM extraction
            summary_id: Indonesian summary
            summary_en: English summary

        Returns:
            CaseInput built from extraction data
        """
        # Determine case type
        case_meta = extraction_result.get("case_metadata", {}) or {}
        crime_category = case_meta.get("crime_category", "").lower()

        if "narkotika" in crime_category or "narcotics" in crime_category:
            case_type = CaseType.NARCOTICS
        elif "korupsi" in crime_category or "corruption" in crime_category:
            case_type = CaseType.CORRUPTION
        elif crime_category:
            case_type = CaseType.GENERAL_CRIMINAL
        else:
            case_type = CaseType.OTHER

        # Extract defendant info
        defendant_data = extraction_result.get("defendant", {}) or {}
        defendant_profile = DefendantProfile(
            is_first_offender=True,  # Default, not always available
            age=defendant_data.get("age"),
            occupation=defendant_data.get("occupation"),
        )

        # Extract key facts
        legal_facts = extraction_result.get("legal_facts", {}) or {}
        key_facts = []
        for key in ["violations", "financial_irregularities", "other_facts"]:
            facts = legal_facts.get(key, []) or []
            key_facts.extend(facts[:3])

        # Extract charges
        indictment = extraction_result.get("indictment", {}) or {}
        charges = []
        cited_articles = indictment.get("cited_articles", []) or []
        for article in cited_articles[:5]:
            if article and article.get("full_citation"):
                charges.append(article["full_citation"])
            elif article and article.get("article"):
                charges.append(article["article"])

        # Build summary
        summary = summary_en or summary_id or indictment.get("chronology", "")[:500]

        # Crime-specific details
        narcotics = None
        corruption = None

        if case_type == CaseType.NARCOTICS:
            # Try to extract from indictment
            narcotics = NarcoticsDetails(
                substance="unknown",
                weight_grams=0.0,
                intent=NarcoticsIntent.UNKNOWN,
            )

        if case_type == CaseType.CORRUPTION:
            state_loss = extraction_result.get("state_loss", {}) or {}
            corruption = CorruptionDetails(
                state_loss_idr=float(state_loss.get("proven_amount", 0)),
                position=defendant_data.get("occupation"),
            )

        parsed_case = ParsedCaseInput(
            case_type=case_type,
            summary=summary,
            defendant_profile=defendant_profile,
            key_facts=key_facts,
            charges=charges,
            narcotics=narcotics,
            corruption=corruption,
        )

        return CaseInput(
            input_type=InputType.EXTRACTION_ID,
            raw_input=str(extraction_result),
            parsed_case=parsed_case,
        )


# =============================================================================
# Singleton
# =============================================================================

_case_parser_service: CaseParserService | None = None


def get_case_parser_service() -> CaseParserService:
    """Get or create the case parser service singleton."""
    global _case_parser_service
    if _case_parser_service is None:
        _case_parser_service = CaseParserService()
    return _case_parser_service
