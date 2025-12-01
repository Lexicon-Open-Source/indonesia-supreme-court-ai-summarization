"""
Opinion Generator Service for the Virtual Judicial Council.

Synthesizes agent deliberations into a structured legal opinion draft,
including verdict recommendation, sentencing guidance, and cited precedents.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from litellm import acompletion
from tenacity import retry, stop_after_attempt, wait_exponential

from settings import get_settings
from src.council.schemas import (
    AgentId,
    ApplicableLaw,
    ArgumentPoint,
    CitedPrecedent,
    DeliberationMessage,
    LegalArguments,
    LegalOpinionDraft,
    ParsedCaseInput,
    SentenceRange,
    SentenceRecommendation,
    SimilarCase,
    VerdictDecision,
    VerdictRecommendation,
)

logger = logging.getLogger(__name__)


# JSON schema for opinion generation
OPINION_SCHEMA = {
    "type": "object",
    "required": [
        "case_summary",
        "verdict_recommendation",
        "sentence_recommendation",
        "legal_arguments",
    ],
    "properties": {
        "case_summary": {
            "type": "string",
            "description": (
                "Summary of the case being decided (in Bahasa Indonesia)"
            ),
        },
        "verdict_recommendation": {
            "type": "object",
            "required": ["decision", "confidence", "reasoning"],
            "properties": {
                "decision": {
                    "type": "string",
                    "enum": ["guilty", "not_guilty", "acquitted"],
                },
                "confidence": {
                    "type": "string",
                    "enum": ["high", "medium", "low"],
                },
                "reasoning": {
                    "type": "string",
                    "description": "Reasoning in Bahasa Indonesia",
                },
            },
        },
        "sentence_recommendation": {
            "type": "object",
            "required": ["imprisonment_months", "fine_idr"],
            "properties": {
                "imprisonment_months": {
                    "type": "object",
                    "properties": {
                        "minimum": {"type": "integer"},
                        "maximum": {"type": "integer"},
                        "recommended": {"type": "integer"},
                    },
                },
                "fine_idr": {
                    "type": "object",
                    "properties": {
                        "minimum": {"type": "integer"},
                        "maximum": {"type": "integer"},
                        "recommended": {"type": "integer"},
                    },
                },
                "additional_penalties": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
        },
        "legal_arguments": {
            "type": "object",
            "properties": {
                "for_conviction": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "argument": {
                                "type": "string",
                                "description": (
                                    "Legal argument in Bahasa Indonesia"
                                ),
                            },
                            "source_agent": {"type": "string"},
                            "supporting_cases": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "strength": {
                                "type": "string",
                                "enum": ["strong", "moderate", "weak"],
                            },
                        },
                    },
                },
                "for_leniency": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "argument": {
                                "type": "string",
                                "description": (
                                    "Legal argument in Bahasa Indonesia"
                                ),
                            },
                            "source_agent": {"type": "string"},
                            "supporting_cases": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "strength": {"type": "string"},
                        },
                    },
                },
                "for_severity": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "argument": {
                                "type": "string",
                                "description": (
                                    "Legal argument in Bahasa Indonesia"
                                ),
                            },
                            "source_agent": {"type": "string"},
                            "supporting_cases": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "strength": {"type": "string"},
                        },
                    },
                },
            },
        },
        "cited_precedents": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "case_id": {"type": "string"},
                    "case_number": {"type": "string"},
                    "relevance": {
                        "type": "string",
                        "description": "Relevance in Bahasa Indonesia",
                    },
                    "verdict_summary": {
                        "type": "string",
                        "description": "Summary in Bahasa Indonesia",
                    },
                    "how_it_applies": {
                        "type": "string",
                        "description": "Application in Bahasa Indonesia",
                    },
                },
            },
        },
        "applicable_laws": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "law_reference": {"type": "string"},
                    "description": {
                        "type": "string",
                        "description": "Description in Bahasa Indonesia",
                    },
                    "how_it_applies": {
                        "type": "string",
                        "description": "Application in Bahasa Indonesia",
                    },
                },
            },
        },
        "dissenting_views": {
            "type": "array",
            "items": {
                "type": "string",
                "description": "Dissenting view in Bahasa Indonesia",
            },
        },
    },
}


OPINION_GENERATION_PROMPT = """You are synthesizing a legal opinion from a judicial council deliberation.

IMPORTANT: Generate the entire opinion in Bahasa Indonesia (Indonesian language).

CASE INFORMATION:
{case_info}

SIMILAR PRECEDENT CASES:
{similar_cases}

DELIBERATION TRANSCRIPT:
{deliberation_transcript}

Based on the deliberation between the three judges (Strict
Constructionist, Humanist, and Historian), generate a comprehensive
legal opinion in Bahasa Indonesia that:

1. VERDICT RECOMMENDATION:
   - Synthesize the consensus or majority view on verdict
   - Assess confidence based on level of agreement
   - Provide clear reasoning

2. SENTENCE RECOMMENDATION:
   - Consider the ranges discussed by all judges
   - Provide minimum, maximum, and recommended values
   - Include any additional penalties mentioned

3. LEGAL ARGUMENTS:
   - Categorize arguments for conviction, leniency, and severity
   - Attribute to source agent when clear
   - Assess argument strength

4. PRECEDENTS & LAWS:
   - Cite similar cases discussed with how they apply
   - List applicable laws and articles

5. DISSENTING VIEWS (if include_dissent is true):
   - Capture any minority opinions or disagreements
   - Note which agent held the dissenting view

Return a valid JSON object matching the required schema.
"""


class OpinionGeneratorService:
    """
    Service for generating legal opinions from deliberation sessions.

    Synthesizes the discussion between AI judges into a structured
    legal opinion document with verdict, sentencing, and citations.
    """

    def __init__(self):
        """Initialize the opinion generator with settings."""
        settings = get_settings()
        # Use a more capable model for opinion synthesis
        self.model = settings.extraction_fallback_model or settings.extraction_model
        logger.info(f"Opinion generator service initialized: model={self.model}")

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def generate_opinion(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        messages: list[DeliberationMessage],
        include_dissent: bool = True,
    ) -> LegalOpinionDraft:
        """
        Generate a legal opinion from a deliberation session.

        Args:
            session_id: ID of the deliberation session
            case_input: Parsed case information
            similar_cases: Similar cases found via semantic search
            messages: Full deliberation message history
            include_dissent: Whether to include dissenting opinions

        Returns:
            Structured legal opinion draft

        Raises:
            ValueError: If generation fails
        """
        if len(messages) < 3:
            raise ValueError(
                "Insufficient deliberation: need at least 3 messages to generate opinion"
            )

        logger.info(
            f"Generating opinion for session {session_id}: "
            f"{len(messages)} messages, {len(similar_cases)} similar cases"
        )

        # Format case info
        case_info = self._format_case_info(case_input)

        # Format similar cases
        similar_cases_text = self._format_similar_cases(similar_cases)

        # Format deliberation transcript
        transcript = self._format_transcript(messages)

        # Build prompt
        prompt = OPINION_GENERATION_PROMPT.format(
            case_info=case_info,
            similar_cases=similar_cases_text,
            deliberation_transcript=transcript,
        )

        if not include_dissent:
            prompt += "\n\nNote: Do NOT include dissenting views in the output."

        try:
            response = await acompletion(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "legal_opinion",
                        "schema": OPINION_SCHEMA,
                        "strict": True,
                    },
                },
            )

            result_text = response.choices[0].message.content
            result = json.loads(result_text)

        except Exception as e:
            logger.error(f"Opinion generation failed: {e}")
            raise ValueError(f"Failed to generate opinion: {str(e)}")

        # Build opinion from result
        return self._build_opinion(session_id, result)

    def _format_case_info(self, case_input: ParsedCaseInput) -> str:
        """Format case input for the prompt."""
        parts = [
            f"Case Type: {case_input.case_type.value}",
            f"Summary: {case_input.summary}",
        ]

        if case_input.defendant_profile:
            dp = case_input.defendant_profile
            parts.append(
                f"Defendant: {'First offender' if dp.is_first_offender else 'Repeat offender'}"
            )
            if dp.age:
                parts.append(f"Age: {dp.age}")
            if dp.occupation:
                parts.append(f"Occupation: {dp.occupation}")

        if case_input.key_facts:
            parts.append(f"Key Facts: {'; '.join(case_input.key_facts)}")

        if case_input.charges:
            parts.append(f"Charges: {', '.join(case_input.charges)}")

        if case_input.narcotics:
            n = case_input.narcotics
            parts.append(
                f"Narcotics: {n.substance}, {n.weight_grams}g, intent={n.intent.value}"
            )

        if case_input.corruption:
            c = case_input.corruption
            parts.append(f"Corruption: State loss Rp {c.state_loss_idr:,.0f}")
            if c.position:
                parts.append(f"Position: {c.position}")

        return "\n".join(parts)

    def _format_similar_cases(self, similar_cases: list[SimilarCase]) -> str:
        """Format similar cases for the prompt."""
        if not similar_cases:
            return "No similar cases found."

        parts = []
        for i, case in enumerate(similar_cases, 1):
            parts.append(
                f"{i}. {case.case_number} (similarity: {case.similarity_score:.2f})\n"
                f"   Verdict: {case.verdict_summary}\n"
                f"   Sentence: {case.sentence_months} months\n"
                f"   Relevance: {case.similarity_reason}"
            )

        return "\n".join(parts)

    def _format_transcript(self, messages: list[DeliberationMessage]) -> str:
        """Format deliberation messages as a transcript."""
        parts = []

        for msg in messages:
            sender = msg.sender
            if hasattr(sender, "type"):
                if sender.type == "user":
                    speaker = "User"
                elif sender.type == "agent":
                    speaker = f"Judge ({sender.agent_id.value.title()})"
                else:
                    speaker = "System"
            else:
                speaker = "Unknown"

            parts.append(f"[{speaker}]: {msg.content}")

            # Add citations if present
            if msg.cited_cases:
                parts.append(f"  (Cited cases: {', '.join(msg.cited_cases)})")
            if msg.cited_laws:
                parts.append(f"  (Cited laws: {', '.join(msg.cited_laws)})")

        return "\n\n".join(parts)

    def _build_opinion(
        self,
        session_id: str,
        result: dict[str, Any],
    ) -> LegalOpinionDraft:
        """Build LegalOpinionDraft from LLM result."""
        # Verdict recommendation
        vr = result.get("verdict_recommendation", {})
        try:
            verdict_decision = VerdictDecision(vr.get("decision", "guilty"))
        except ValueError:
            verdict_decision = VerdictDecision.GUILTY

        verdict_recommendation = VerdictRecommendation(
            decision=verdict_decision,
            confidence=vr.get("confidence", "medium"),
            reasoning=vr.get("reasoning", ""),
        )

        # Sentence recommendation
        sr = result.get("sentence_recommendation", {})
        imp = sr.get("imprisonment_months", {})
        fine = sr.get("fine_idr", {})

        sentence_recommendation = SentenceRecommendation(
            imprisonment_months=SentenceRange(
                minimum=imp.get("minimum", 0),
                maximum=imp.get("maximum", 0),
                recommended=imp.get("recommended", 0),
            ),
            fine_idr=SentenceRange(
                minimum=fine.get("minimum", 0),
                maximum=fine.get("maximum", 0),
                recommended=fine.get("recommended", 0),
            ),
            additional_penalties=sr.get("additional_penalties", []),
        )

        # Legal arguments
        la = result.get("legal_arguments", {})

        def build_arguments(args_list: list[dict]) -> list[ArgumentPoint]:
            points = []
            for arg in args_list:
                agent_str = arg.get("source_agent", "strict")
                try:
                    agent = AgentId(agent_str)
                except ValueError:
                    agent = AgentId.STRICT

                points.append(
                    ArgumentPoint(
                        argument=arg.get("argument", ""),
                        source_agent=agent,
                        supporting_cases=arg.get("supporting_cases", []),
                        strength=arg.get("strength", "moderate"),
                    )
                )
            return points

        legal_arguments = LegalArguments(
            for_conviction=build_arguments(la.get("for_conviction", [])),
            for_leniency=build_arguments(la.get("for_leniency", [])),
            for_severity=build_arguments(la.get("for_severity", [])),
        )

        # Cited precedents
        cited_precedents = [
            CitedPrecedent(
                case_id=p.get("case_id", ""),
                case_number=p.get("case_number", ""),
                relevance=p.get("relevance", ""),
                verdict_summary=p.get("verdict_summary", ""),
                how_it_applies=p.get("how_it_applies", ""),
            )
            for p in result.get("cited_precedents", [])
        ]

        # Applicable laws
        applicable_laws = [
            ApplicableLaw(
                law_reference=law.get("law_reference", ""),
                description=law.get("description", ""),
                how_it_applies=law.get("how_it_applies", ""),
            )
            for law in result.get("applicable_laws", [])
        ]

        return LegalOpinionDraft(
            session_id=session_id,
            generated_at=datetime.now(timezone.utc),
            case_summary=result.get("case_summary", ""),
            verdict_recommendation=verdict_recommendation,
            sentence_recommendation=sentence_recommendation,
            legal_arguments=legal_arguments,
            cited_precedents=cited_precedents,
            applicable_laws=applicable_laws,
            dissenting_views=result.get("dissenting_views", []),
        )


# =============================================================================
# Singleton
# =============================================================================

_opinion_generator_service: OpinionGeneratorService | None = None


def get_opinion_generator_service() -> OpinionGeneratorService:
    """Get or create the opinion generator service singleton."""
    global _opinion_generator_service
    if _opinion_generator_service is None:
        _opinion_generator_service = OpinionGeneratorService()
    return _opinion_generator_service
