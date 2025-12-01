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
from collections.abc import AsyncIterator
from dataclasses import dataclass
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


@dataclass
class StreamChunk:
    """A chunk of streamed response from an agent."""

    agent_id: AgentId
    content: str
    is_complete: bool = False
    message_id: str | None = None

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
        return f"""Anda adalah hakim yang berpartisipasi dalam musyawarah majelis hakim dengan dua hakim lainnya.

PENTING: SELALU GUNAKAN BAHASA INDONESIA dalam semua respons Anda. Jangan menggunakan bahasa Inggris.

{self.system_prompt}

GAYA MUSYAWARAH:
Anda sedang dalam diskusi LANGSUNG dengan sesama hakim. Ini bukan pendapat formal tertulis - ini adalah musyawarah kerja di mana Anda memikirkan perkara bersama-sama.

CARA BERINTERAKSI DENGAN HAKIM LAIN:
1. SAPA mereka langsung dengan gelar (misalnya, "Rekan Hakim Humanis yang terhormat...")
2. TANGGAPI poin-poin spesifik mereka - setuju, tidak setuju, atau kembangkan
3. AJUKAN pertanyaan retoris untuk menguji penalaran mereka
4. AKUI poin yang valid meskipun Anda tidak setuju secara keseluruhan
5. TANTANG penalaran yang menurut Anda cacat, bukan orangnya
6. CARI titik temu bila memungkinkan

POLA DISKUSI NATURAL:
- "Saya harus dengan hormat tidak setuju dengan Hakim Humanis dalam hal ini..."
- "Hakim Sejarawan mengangkat preseden penting, tetapi saya akan membedakannya karena..."
- "Meskipun saya menghargai penekanan Hakim Strict pada teks undang-undang, kita juga harus mempertimbangkan..."
- "Saya sebagian setuju dengan rekan-rekan saya, tetapi..."
- "Ini membawa saya untuk mempertanyakan asumsi bahwa..."

PANDUAN RESPONS:
1. Jaga agar respons tetap percakapan dan fokus (150-350 kata)
2. Referensikan pasal undang-undang yang relevan (misalnya, "Pasal 127 UU Narkotika")
3. Kutip kasus serupa bila berlaku
4. Terlibat langsung dengan apa yang dikatakan hakim lain
5. Tunjukkan proses penalaran Anda, bukan hanya kesimpulan

FORMAT KUTIPAN:
- Saat mengutip kasus: "Dalam perkara [NOMOR PERKARA], pengadilan memutuskan bahwa..."
- Saat mengutip undang-undang: "Berdasarkan Pasal X [NAMA UU]..."
- Kutipan singkat sudah cukup dalam diskusi - simpan analisis detail untuk pendapat formal

HINDARI:
- Berbicara seolah-olah Anda menulis putusan formal
- Mengabaikan apa yang dikatakan hakim lain
- Mengulangi poin yang sudah dibuat
- Bersikap terlalu konfrontatif
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
        messages = [{"role": "system", "content": self.get_base_system_prompt()}]

        # Add case context
        case_context = self._format_case_context(case_input, similar_cases)
        messages.append({"role": "user", "content": case_context})
        messages.append(
            {
                "role": "assistant",
                "content": "I understand the case details. I'm ready to deliberate.",
            }
        )

        # Add conversation history (limited to prevent context overflow)
        recent_history = history[-self.max_context_messages :]
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

    async def generate_response_stream(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        history: list[DeliberationMessage],
        user_message: str | None = None,
    ) -> AsyncIterator[StreamChunk]:
        """
        Generate a streaming response in the deliberation.

        Yields chunks of the response as they are generated, enabling
        real-time streaming to clients.

        Args:
            session_id: Current session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            history: Previous deliberation messages
            user_message: Optional new user message to respond to

        Yields:
            StreamChunk objects with partial content and completion status
        """
        messages = self._build_context(
            case_input=case_input,
            similar_cases=similar_cases,
            history=history,
            user_message=user_message,
        )

        message_id = str(uuid4())
        logger.info(f"Agent {self.agent_id.value} generating streaming response")

        full_content = ""

        try:
            response = await acompletion(
                model=self.model,
                messages=messages,
                stream=True,
            )

            async for chunk in response:
                if chunk.choices and chunk.choices[0].delta.content:
                    content_chunk = chunk.choices[0].delta.content
                    full_content += content_chunk
                    yield StreamChunk(
                        agent_id=self.agent_id,
                        content=content_chunk,
                        is_complete=False,
                        message_id=message_id,
                    )

            # Final chunk with completion flag
            yield StreamChunk(
                agent_id=self.agent_id,
                content="",
                is_complete=True,
                message_id=message_id,
            )

        except Exception as e:
            logger.error(f"Agent {self.agent_id.value} streaming failed: {e}")
            raise

    def create_message_from_stream(
        self,
        session_id: str,
        message_id: str,
        full_content: str,
    ) -> DeliberationMessage:
        """
        Create a DeliberationMessage from accumulated stream content.

        Called after streaming completes to create the final message record.
        """
        cited_cases, cited_laws = self._extract_citations(full_content)

        return DeliberationMessage(
            id=message_id,
            session_id=session_id,
            sender=AgentSender(agent_id=self.agent_id),
            content=full_content,
            cited_cases=cited_cases,
            cited_laws=cited_laws,
        )

    async def generate_initial_opinion(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
    ) -> DeliberationMessage:
        """
        Generate the agent's initial opinion on a new case.

        Called when this agent opens the deliberation (typically Judge Strict).

        Args:
            session_id: New session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference

        Returns:
            DeliberationMessage with initial opinion
        """
        initial_prompt = (
            "Anda membuka musyawarah majelis hakim ini. Sampaikan penilaian "
            f"awal Anda dari perspektif sebagai {self.agent_name}.\n\n"
            "Pernyataan pembuka Anda harus:\n"
            "1. Merumuskan pertanyaan hukum utama di hadapan majelis\n"
            "2. Menyatakan posisi awal Anda mengenai putusan\n"
            "3. Mengidentifikasi pertimbangan hukum terpenting\n"
            "4. Mengundang diskusi dari rekan hakim mengenai poin-poin tertentu\n\n"
            "Berbicara secara alami seolah-olah menyapa majelis secara langsung. "
            "Akhiri dengan pertanyaan atau poin yang mengundang tanggapan dari rekan hakim."
        )

        return await self.generate_response(
            session_id=session_id,
            case_input=case_input,
            similar_cases=similar_cases,
            history=[],
            user_message=initial_prompt,
        )

    async def respond_to_deliberation(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        prior_opinions: list[DeliberationMessage],
        is_initial_round: bool = False,
    ) -> DeliberationMessage:
        """
        Respond to the ongoing deliberation, engaging with other judges' opinions.

        Args:
            session_id: Current session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            prior_opinions: Previous messages in the deliberation
            is_initial_round: Whether this is the initial opinion round

        Returns:
            DeliberationMessage with response to the discussion
        """
        # Build context about what other judges have said
        other_opinions = self._summarize_prior_opinions(prior_opinions)

        if is_initial_round:
            prompt = (
                f"Musyawarah telah dimulai. Berikut pendapat rekan-rekan hakim Anda:\n\n"
                f"{other_opinions}\n\n"
                f"Sebagai {self.agent_name}, tanggapi diskusi ini:\n"
                "1. Akui poin-poin spesifik yang dikemukakan rekan hakim (setuju atau tidak setuju)\n"
                "2. Tambahkan perspektif unik Anda berdasarkan filosofi yudisial Anda\n"
                "3. Tunjukkan hal-hal yang mungkin terlewatkan oleh rekan hakim lain\n"
                "4. Jika Anda tidak setuju dengan hakim lain, jelaskan alasannya dengan hormat\n"
                "5. Ajukan pertimbangan baru atau pertanyaan untuk diskusi lebih lanjut\n\n"
                "Sapa rekan-rekan Anda secara langsung dan terlibat dengan penalaran mereka."
            )
        else:
            prompt = (
                f"Diskusi berlanjut. Musyawarah terkini:\n\n"
                f"{other_opinions}\n\n"
                f"Sebagai {self.agent_name}, berkontribusi pada diskusi yang sedang berlangsung:\n"
                "- Tanggapi poin-poin baru yang diangkat oleh rekan hakim lain\n"
                "- Bangun di atas area kesepakatan yang mulai terbentuk\n"
                "- Klarifikasi atau pertahankan posisi Anda jika ditantang\n"
                "- Arahkan diskusi menuju penyelesaian jika memungkinkan\n\n"
                "Jaga agar tanggapan Anda tetap fokus untuk memajukan musyawarah."
            )

        return await self.generate_response(
            session_id=session_id,
            case_input=case_input,
            similar_cases=similar_cases,
            history=prior_opinions,
            user_message=prompt,
        )

    def _summarize_prior_opinions(
        self,
        messages: list[DeliberationMessage],
    ) -> str:
        """
        Create a summary of prior opinions for context.

        Args:
            messages: Previous deliberation messages

        Returns:
            Formatted summary string
        """
        summaries = []

        for msg in messages:
            if hasattr(msg.sender, "agent_id"):
                agent_name = self._get_judge_title(msg.sender.agent_id)
                # Include full content for richer context
                summaries.append(f"**{agent_name}:**\n{msg.content}")
            elif hasattr(msg.sender, "type") and msg.sender.type == "user":
                summaries.append(f"**User:**\n{msg.content}")

        return "\n\n---\n\n".join(summaries) if summaries else "No prior discussion."

    def _get_judge_title(self, agent_id: AgentId) -> str:
        """Get a formal title for a judge agent."""
        titles = {
            AgentId.STRICT: "Judge Strict (Constructionist)",
            AgentId.HUMANIST: "Judge Humanist (Rehabilitative)",
            AgentId.HISTORIAN: "Judge Historian (Precedent)",
        }
        return titles.get(agent_id, f"Judge {agent_id.value.title()}")
