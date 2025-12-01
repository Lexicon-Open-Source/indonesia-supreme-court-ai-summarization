"""
Base Judge Agent class for the Virtual Judicial Council.

Provides the foundation for specialized judicial AI agents with:
- Consistent response formatting
- Citation extraction
- Context window management
- Message history handling
"""

import logging
import re
from abc import ABC, abstractmethod
from uuid import uuid4

from litellm import acompletion
from tenacity import retry, stop_after_attempt, wait_exponential

from settings import get_settings
from src.council.schemas import (
    AgentId,
    AgentSender,
    DeliberationMessage,
    ParsedCaseInput,
    SimilarCase,
)

logger = logging.getLogger(__name__)


class BaseJudgeAgent(ABC):
    """
    Abstract base class for judicial AI agents.

    Each agent represents a distinct judicial philosophy:
    - Strict: Literal interpretation of law
    - Humanist: Focus on rehabilitation and circumstances
    - Historian: Historical precedent and evolution of law

    Subclasses must implement:
    - agent_id: Unique identifier
    - agent_name: Display name
    - system_prompt: Defines the agent's judicial philosophy
    """

    def __init__(self):
        """Initialize the agent with settings."""
        settings = get_settings()
        self.model = settings.extraction_model
        self.max_context_messages = 20
        logger.info(f"Initialized agent: {self.agent_name}")

    @property
    @abstractmethod
    def agent_id(self) -> AgentId:
        """Unique identifier for this agent."""
        ...

    @property
    @abstractmethod
    def agent_name(self) -> str:
        """Display name for this agent."""
        ...

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """System prompt defining the agent's judicial philosophy."""
        ...

    def get_base_system_prompt(self) -> str:
        """
        Get the complete system prompt including base instructions.

        Combines the agent-specific philosophy with common formatting rules.
        """
        return f"""You are a judicial AI assistant participating in a deliberation council.

{self.system_prompt}

RESPONSE GUIDELINES:
1. Keep responses focused and concise (200-400 words typically)
2. Reference specific legal articles when relevant (e.g., "Article 127 UU Narkotika")
3. Cite similar cases when applicable using case numbers
4. Acknowledge but respectfully disagree with other judges when your philosophy differs
5. Use formal but accessible language
6. Structure longer responses with clear sections

CITATION FORMAT:
- When citing cases: "In case [CASE_NUMBER], the court held that..."
- When citing laws: "Under Article X of [LAW_NAME]..."
- Always explain how citations apply to the current case

RESPONSE FORMAT:
Provide your response as a thoughtful judicial opinion. Include:
- Your position on the legal question
- Legal reasoning with citations
- How this aligns with your judicial philosophy
- Any points of agreement or disagreement with other judges
"""

    def _extract_citations(self, content: str) -> tuple[list[str], list[str]]:
        """
        Extract case and law citations from response content.

        Returns:
            Tuple of (case_citations, law_citations)
        """
        case_citations = []
        law_citations = []

        # Case number patterns (Indonesian court format)
        case_patterns = [
            r"\d+\s*/\s*Pid\.Sus\s*/\s*\d{4}\s*/\s*\w+",  # 123/Pid.Sus/2024/PN XYZ
            r"\d+\s*K\s*/\s*Pid\.Sus\s*/\s*\d{4}",  # 123 K/Pid.Sus/2024
            r"MA\s+\d+\s*K\s*/\s*\w+\s*/\s*\d{4}",  # MA 123 K/Pid/2024
            r"Putusan\s+(?:Nomor|No\.?)\s*[\d\w/]+",  # Putusan Nomor X
        ]

        for pattern in case_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            case_citations.extend(matches)

        # Law citation patterns
        law_patterns = [
            r"(?:Pasal|Article)\s+\d+[a-z]?\s+(?:UU|Undang-Undang)[\w\s]+",
            r"UU\s+(?:No\.?|Nomor)\s*\d+\s+Tahun\s+\d{4}",
            r"Undang-Undang\s+(?:No\.?|Nomor)\s*\d+\s+Tahun\s+\d{4}",
            r"KUHP\s+(?:Pasal|Article)\s+\d+",
            r"(?:Pasal|Article)\s+\d+[a-z]?\s+KUHP",
        ]

        for pattern in law_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            law_citations.extend(matches)

        # Deduplicate
        return list(set(case_citations)), list(set(law_citations))

    def _build_context(
        self,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        history: list[DeliberationMessage],
        user_message: str | None = None,
    ) -> list[dict[str, str]]:
        """
        Build the message context for the LLM call.

        Args:
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            history: Previous deliberation messages
            user_message: Optional new user message

        Returns:
            List of messages for the LLM
        """
        messages = [
            {"role": "system", "content": self.get_base_system_prompt()}
        ]

        # Add case context
        case_context = self._format_case_context(case_input, similar_cases)
        messages.append({"role": "user", "content": case_context})
        messages.append({
            "role": "assistant",
            "content": "I understand the case details. I'm ready to deliberate.",
        })

        # Add conversation history (limited to prevent context overflow)
        recent_history = history[-self.max_context_messages:]
        for msg in recent_history:
            role = self._get_role_for_message(msg)
            content = self._format_history_message(msg)
            messages.append({"role": role, "content": content})

        # Add new user message if provided
        if user_message:
            messages.append({"role": "user", "content": user_message})

        return messages

    def _format_case_context(
        self,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
    ) -> str:
        """Format case information as context."""
        parts = [
            "=== CASE UNDER DELIBERATION ===",
            f"Type: {case_input.case_type.value}",
            f"Summary: {case_input.summary}",
        ]

        if case_input.defendant_profile:
            dp = case_input.defendant_profile
            status = "first offender" if dp.is_first_offender else "repeat offender"
            parts.append(f"Defendant: {status}")
            if dp.age:
                parts.append(f"Age: {dp.age}")

        if case_input.key_facts:
            parts.append("Key Facts:\n- " + "\n- ".join(case_input.key_facts[:5]))

        if case_input.charges:
            parts.append(f"Charges: {', '.join(case_input.charges[:3])}")

        if case_input.narcotics:
            n = case_input.narcotics
            parts.append(
                f"Narcotics Details: {n.substance}, {n.weight_grams}g, "
                f"intent: {n.intent.value}"
            )

        if case_input.corruption:
            c = case_input.corruption
            parts.append(f"Corruption Details: State loss Rp {c.state_loss_idr:,.0f}")

        # Add similar cases
        if similar_cases:
            parts.append("\n=== SIMILAR PRECEDENT CASES ===")
            for i, case in enumerate(similar_cases[:5], 1):
                parts.append(
                    f"{i}. {case.case_number} (similarity: {case.similarity_score:.2f})"
                )
                parts.append(f"   Verdict: {case.verdict_summary}")
                parts.append(f"   Sentence: {case.sentence_months} months")

        return "\n".join(parts)

    def _get_role_for_message(self, msg: DeliberationMessage) -> str:
        """Determine LLM role for a message."""
        if hasattr(msg.sender, "type"):
            if msg.sender.type == "user":
                return "user"
            elif msg.sender.type == "agent":
                # Other agents' messages are presented as user context
                if msg.sender.agent_id == self.agent_id:
                    return "assistant"
                return "user"
        return "user"

    def _format_history_message(self, msg: DeliberationMessage) -> str:
        """Format a history message with sender context."""
        if hasattr(msg.sender, "type"):
            if msg.sender.type == "user":
                return f"[User asks]: {msg.content}"
            elif msg.sender.type == "agent":
                agent_name = msg.sender.agent_id.value.title()
                if msg.sender.agent_id == self.agent_id:
                    return msg.content  # Own messages without prefix
                return f"[Judge {agent_name} says]: {msg.content}"
            else:
                return f"[System]: {msg.content}"
        return msg.content

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def generate_response(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        history: list[DeliberationMessage],
        user_message: str | None = None,
    ) -> DeliberationMessage:
        """
        Generate a response in the deliberation.

        Args:
            session_id: Current session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            history: Previous deliberation messages
            user_message: Optional new user message to respond to

        Returns:
            DeliberationMessage with the agent's response
        """
        messages = self._build_context(
            case_input=case_input,
            similar_cases=similar_cases,
            history=history,
            user_message=user_message,
        )

        logger.info(f"Agent {self.agent_id.value} generating response")

        try:
            response = await acompletion(
                model=self.model,
                messages=messages,
            )

            content = response.choices[0].message.content

            # Extract citations
            cited_cases, cited_laws = self._extract_citations(content)

            return DeliberationMessage(
                id=str(uuid4()),
                session_id=session_id,
                sender=AgentSender(agent_id=self.agent_id),
                content=content,
                cited_cases=cited_cases,
                cited_laws=cited_laws,
            )

        except Exception as e:
            logger.error(f"Agent {self.agent_id.value} failed to respond: {e}")
            raise

    async def generate_initial_opinion(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
    ) -> DeliberationMessage:
        """
        Generate the agent's initial opinion on a new case.

        Called when a new deliberation session starts.

        Args:
            session_id: New session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference

        Returns:
            DeliberationMessage with initial opinion
        """
        initial_prompt = (
            "Please provide your initial assessment of this case from your "
            f"judicial perspective as a {self.agent_name}. "
            "Include:\n"
            "1. Your preliminary view on the appropriate verdict\n"
            "2. Key legal considerations from your perspective\n"
            "3. Relevant precedents you find applicable\n"
            "4. Any initial concerns or points for further discussion"
        )

        return await self.generate_response(
            session_id=session_id,
            case_input=case_input,
            similar_cases=similar_cases,
            history=[],
            user_message=initial_prompt,
        )
