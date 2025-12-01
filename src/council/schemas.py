"""
Pydantic schemas for the Virtual Judicial Council feature.

Defines data models for:
- Case input and parsing
- Deliberation sessions and messages
- Agent identifiers and responses
- Legal opinion generation
"""

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field

# =============================================================================
# Enums
# =============================================================================


class AgentId(str, Enum):
    """Identifiers for the three judicial AI agents."""

    STRICT = "strict"
    HUMANIST = "humanist"
    HISTORIAN = "historian"


class CaseType(str, Enum):
    """Types of legal cases."""

    NARCOTICS = "narcotics"
    CORRUPTION = "corruption"
    GENERAL_CRIMINAL = "general_criminal"
    OTHER = "other"


class NarcoticsIntent(str, Enum):
    """Intent classification for narcotics cases."""

    PERSONAL_USE = "personal_use"
    DISTRIBUTION = "distribution"
    UNKNOWN = "unknown"


class SessionStatus(str, Enum):
    """Status of a deliberation session."""

    ACTIVE = "active"
    CONCLUDED = "concluded"
    ARCHIVED = "archived"


class InputType(str, Enum):
    """Type of case input."""

    TEXT_SUMMARY = "text_summary"
    EXTRACTION_ID = "extraction_id"


class VerdictDecision(str, Enum):
    """Possible verdict decisions."""

    GUILTY = "guilty"
    NOT_GUILTY = "not_guilty"
    ACQUITTED = "acquitted"


class MessageIntent(str, Enum):
    """Intent classification for user messages."""

    ASK_OPINION = "ask_opinion"
    REQUEST_COMPARISON = "request_comparison"
    CHALLENGE_VIEW = "challenge_view"
    SEEK_CONSENSUS = "seek_consensus"
    GENERAL_QUESTION = "general_question"


# =============================================================================
# Case Input Schemas
# =============================================================================


class DefendantProfile(BaseModel):
    """Profile of the defendant in a case."""

    is_first_offender: bool = True
    age: int | None = None
    occupation: str | None = None


class NarcoticsDetails(BaseModel):
    """Details specific to narcotics cases."""

    substance: str
    weight_grams: float
    intent: NarcoticsIntent = NarcoticsIntent.UNKNOWN


class CorruptionDetails(BaseModel):
    """Details specific to corruption cases."""

    state_loss_idr: float
    position: str | None = None


class StructuredCaseData(BaseModel):
    """Optional structured data that can be provided with case input."""

    defendant_age: int | None = None
    defendant_first_offender: bool | None = None
    substance_type: str | None = None
    weight_grams: float | None = None
    state_loss_idr: float | None = None


class ParsedCaseInput(BaseModel):
    """Parsed and structured case information."""

    case_type: CaseType
    summary: str
    defendant_profile: DefendantProfile | None = None
    key_facts: list[str] = Field(default_factory=list)
    charges: list[str] = Field(default_factory=list)
    narcotics: NarcoticsDetails | None = None
    corruption: CorruptionDetails | None = None


class CaseInput(BaseModel):
    """Complete case input with raw and parsed data."""

    input_type: InputType
    raw_input: str
    parsed_case: ParsedCaseInput


# =============================================================================
# Similar Case Schemas
# =============================================================================


class SimilarCase(BaseModel):
    """A similar case found via semantic search."""

    case_id: str
    case_number: str
    similarity_score: float
    similarity_reason: str
    verdict_summary: str
    sentence_months: int


# =============================================================================
# Message Schemas
# =============================================================================


class UserSender(BaseModel):
    """Sender type for user messages."""

    type: Literal["user"] = "user"


class AgentSender(BaseModel):
    """Sender type for agent messages."""

    type: Literal["agent"] = "agent"
    agent_id: AgentId


class SystemSender(BaseModel):
    """Sender type for system messages."""

    type: Literal["system"] = "system"


# Union type for message senders
MessageSender = UserSender | AgentSender | SystemSender


class DeliberationMessage(BaseModel):
    """A message in a deliberation session."""

    id: str
    session_id: str
    sender: MessageSender
    content: str
    intent: str | None = None
    cited_cases: list[str] = Field(default_factory=list)
    cited_laws: list[str] = Field(default_factory=list)
    timestamp: datetime | None = None


# =============================================================================
# Session Schemas
# =============================================================================


class DeliberationSession(BaseModel):
    """A deliberation session."""

    id: str
    user_id: str | None = None
    status: SessionStatus
    case_input: CaseInput
    similar_cases: list[SimilarCase] = Field(default_factory=list)
    messages: list[DeliberationMessage] = Field(default_factory=list)
    legal_opinion: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime
    concluded_at: datetime | None = None


# =============================================================================
# Legal Opinion Schemas
# =============================================================================


class SentenceRange(BaseModel):
    """A range for sentencing recommendations."""

    minimum: int
    maximum: int
    recommended: int


class VerdictRecommendation(BaseModel):
    """Recommendation for the verdict."""

    decision: VerdictDecision
    confidence: str  # "high", "medium", "low"
    reasoning: str


class SentenceRecommendation(BaseModel):
    """Recommendation for sentencing."""

    imprisonment_months: SentenceRange
    fine_idr: SentenceRange
    additional_penalties: list[str] = Field(default_factory=list)


class ArgumentPoint(BaseModel):
    """A single argument point in the legal opinion."""

    argument: str
    source_agent: AgentId
    supporting_cases: list[str] = Field(default_factory=list)
    strength: str  # "strong", "moderate", "weak"


class LegalArguments(BaseModel):
    """Collected legal arguments from the deliberation."""

    for_conviction: list[ArgumentPoint] = Field(default_factory=list)
    for_leniency: list[ArgumentPoint] = Field(default_factory=list)
    for_severity: list[ArgumentPoint] = Field(default_factory=list)


class CitedPrecedent(BaseModel):
    """A precedent case cited in the opinion."""

    case_id: str
    case_number: str
    relevance: str
    verdict_summary: str
    how_it_applies: str


class ApplicableLaw(BaseModel):
    """An applicable law reference."""

    law_reference: str
    description: str
    how_it_applies: str


class LegalOpinionDraft(BaseModel):
    """Generated legal opinion from a deliberation session."""

    session_id: str
    generated_at: datetime
    case_summary: str
    verdict_recommendation: VerdictRecommendation
    sentence_recommendation: SentenceRecommendation
    legal_arguments: LegalArguments
    cited_precedents: list[CitedPrecedent] = Field(default_factory=list)
    applicable_laws: list[ApplicableLaw] = Field(default_factory=list)
    dissenting_views: list[str] = Field(default_factory=list)


# =============================================================================
# API Request/Response Schemas
# =============================================================================


class CreateSessionRequest(BaseModel):
    """Request to create a new deliberation session."""

    case_summary: str = Field(..., min_length=50, max_length=10000)
    case_type: CaseType | None = None
    structured_data: StructuredCaseData | None = None
    input_type: InputType = InputType.TEXT_SUMMARY


class CreateSessionResponse(BaseModel):
    """Response after creating a session."""

    session_id: str
    parsed_case: ParsedCaseInput
    similar_cases: list[SimilarCase]
    initial_message: DeliberationMessage


class GetSessionResponse(BaseModel):
    """Response for getting a session."""

    session: DeliberationSession


class ListSessionsResponse(BaseModel):
    """Response for listing sessions."""

    sessions: list[DeliberationSession]
    pagination: dict[str, int]


class SendMessageRequest(BaseModel):
    """Request to send a message in a session."""

    content: str = Field(..., min_length=1, max_length=5000)
    target_agent: str | None = None  # AgentId or "all"
    intent: MessageIntent | None = None


class SendMessageResponse(BaseModel):
    """Response after sending a message."""

    user_message: DeliberationMessage
    agent_responses: list[DeliberationMessage]


class ContinueDiscussionRequest(BaseModel):
    """Request to continue the judicial discussion without user input."""

    num_rounds: int = Field(
        default=1,
        ge=1,
        le=3,
        description="Number of discussion rounds (each round = all judges respond)",
    )


class ContinueDiscussionResponse(BaseModel):
    """Response after continuing the discussion."""

    new_messages: list[DeliberationMessage]
    total_messages: int


class GetMessagesResponse(BaseModel):
    """Response for getting messages."""

    messages: list[DeliberationMessage]


class GenerateOpinionRequest(BaseModel):
    """Request to generate a legal opinion."""

    include_dissent: bool = True


class GenerateOpinionResponse(BaseModel):
    """Response with generated legal opinion."""

    opinion: LegalOpinionDraft


# =============================================================================
# Search Schemas
# =============================================================================


class SearchCasesRequest(BaseModel):
    """Request for searching cases."""

    query: str
    limit: int = 10
    semantic_search: bool = True
    filters: StructuredCaseData | None = None


class CaseRecord(BaseModel):
    """A case record from the database."""

    id: str
    case_number: str | None = None
    case_type: CaseType | None = None
    court_name: str | None = None
    court_type: str | None = None
    decision_date: str | None = None
    defendant_name: str | None = None
    defendant_age: int | None = None
    defendant_first_offender: bool | None = None
    indictment: dict[str, Any] | None = None
    narcotics_details: dict[str, Any] | None = None
    corruption_details: dict[str, Any] | None = None
    legal_facts: dict[str, Any] | None = None
    verdict: dict[str, Any] | None = None
    legal_basis: list[str] | None = None
    is_landmark_case: bool = False
    extraction_result: dict[str, Any] | None = None
    summary_en: str | None = None
    summary_id: str | None = None


class SearchCasesResponse(BaseModel):
    """Response for case search."""

    cases: list[CaseRecord]
    total: int


class GetCaseResponse(BaseModel):
    """Response for getting a single case."""

    case: CaseRecord


class CaseStatisticsResponse(BaseModel):
    """Response for case statistics."""

    total_cases: int
    sentence_distribution: dict[str, Any]
    verdict_distribution: dict[str, Any]


# =============================================================================
# Streaming Schemas
# =============================================================================


class StreamEventType(str, Enum):
    """Types of streaming events."""

    AGENT_START = "agent_start"
    CHUNK = "chunk"
    AGENT_COMPLETE = "agent_complete"
    AGENT_ERROR = "agent_error"
    USER_MESSAGE = "user_message"
    DELIBERATION_COMPLETE = "deliberation_complete"


class StreamEventData(BaseModel):
    """Data payload for streaming events (SSE)."""

    event_type: StreamEventType
    agent_id: AgentId | None = None
    content: str = ""
    message_id: str | None = None
    full_content: str | None = None  # Only on agent_complete

    class Config:
        """Pydantic config."""

        use_enum_values = True


class StreamMessageRequest(BaseModel):
    """Request for streaming message processing."""

    content: str = Field(..., min_length=1, max_length=5000)
    target_agent: str | None = None  # AgentId or "all"


class StreamContinueRequest(BaseModel):
    """Request for streaming continued discussion."""

    num_rounds: int = Field(
        default=1,
        ge=1,
        le=3,
        description="Number of discussion rounds (each round = all judges respond)",
    )
