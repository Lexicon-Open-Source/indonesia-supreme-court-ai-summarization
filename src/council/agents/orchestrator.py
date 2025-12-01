"""
Agent Orchestrator for the Virtual Judicial Council.

Coordinates the three judge agents to produce coherent deliberations:
- Routes user messages to appropriate agents
- Determines response order based on context
- Manages multi-agent discussions
- Ensures balanced participation
"""

import logging
import random
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import uuid4

from src.council.agents.base import BaseJudgeAgent
from src.council.agents.historian import HistorianAgent
from src.council.agents.humanist import HumanistAgent
from src.council.agents.strict import StrictConstructionistAgent
from src.council.schemas import (
    AgentId,
    DeliberationMessage,
    MessageIntent,
    ParsedCaseInput,
    SimilarCase,
    UserSender,
)


@dataclass
class StreamEvent:
    """Event emitted during streaming deliberation."""

    event_type: str  # "agent_start", "chunk", "agent_complete", "deliberation_complete"
    agent_id: AgentId | None = None
    content: str = ""
    message_id: str | None = None
    full_content: str | None = None  # Only on agent_complete

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    """
    Orchestrates deliberation between the three judge agents.

    Responsibilities:
    - Initialize and manage all three agents
    - Route messages to appropriate agent(s)
    - Determine response order for multi-agent responses
    - Track participation to ensure balanced discussion
    """

    def __init__(self):
        """Initialize the orchestrator with all three agents."""
        self.agents: dict[AgentId, BaseJudgeAgent] = {
            AgentId.STRICT: StrictConstructionistAgent(),
            AgentId.HUMANIST: HumanistAgent(),
            AgentId.HISTORIAN: HistorianAgent(),
        }
        logger.info("Agent orchestrator initialized with 3 agents")

    def get_agent(self, agent_id: AgentId) -> BaseJudgeAgent:
        """Get a specific agent by ID."""
        return self.agents[agent_id]

    def classify_intent(self, message: str) -> MessageIntent:
        """
        Classify the intent of a user message.

        Args:
            message: User's message text

        Returns:
            Classified MessageIntent
        """
        message_lower = message.lower()

        # Opinion seeking patterns
        opinion_patterns = [
            r"what (do you|does the|would)",
            r"how (do you|would you|should)",
            r"your (view|opinion|take|position)",
            r"think about",
            r"assess",
            r"evaluate",
        ]
        for pattern in opinion_patterns:
            if re.search(pattern, message_lower):
                return MessageIntent.ASK_OPINION

        # Comparison patterns
        comparison_patterns = [
            r"compare",
            r"similar (case|situation)",
            r"how (does|do) this compare",
            r"precedent",
            r"previous case",
        ]
        for pattern in comparison_patterns:
            if re.search(pattern, message_lower):
                return MessageIntent.REQUEST_COMPARISON

        # Challenge patterns
        challenge_patterns = [
            r"but",
            r"however",
            r"disagree",
            r"what about",
            r"don't you think",
            r"isn't it",
            r"challenge",
            r"counter",
        ]
        for pattern in challenge_patterns:
            if re.search(pattern, message_lower):
                return MessageIntent.CHALLENGE_VIEW

        # Consensus patterns
        consensus_patterns = [
            r"consensus",
            r"agree",
            r"common ground",
            r"conclusion",
            r"final",
            r"verdict",
            r"decision",
        ]
        for pattern in consensus_patterns:
            if re.search(pattern, message_lower):
                return MessageIntent.SEEK_CONSENSUS

        return MessageIntent.GENERAL_QUESTION

    def determine_response_order(
        self,
        message: str,
        target_agent: str | None,
        history: list[DeliberationMessage],
    ) -> list[AgentId]:
        """
        Determine which agents should respond and in what order.

        Args:
            message: User's message
            target_agent: Explicitly targeted agent (if any)
            history: Previous deliberation messages

        Returns:
            Ordered list of AgentIds that should respond
        """
        # If specific agent targeted
        if target_agent:
            if target_agent == "all":
                return self._balanced_order(history)
            try:
                agent_id = AgentId(target_agent)
                return [agent_id]
            except ValueError:
                pass

        # Classify intent and determine based on that
        intent = self.classify_intent(message)

        if intent == MessageIntent.REQUEST_COMPARISON:
            # Historian leads for precedent comparisons
            return [AgentId.HISTORIAN, AgentId.STRICT, AgentId.HUMANIST]

        elif intent == MessageIntent.SEEK_CONSENSUS:
            # All agents respond in balanced order
            return self._balanced_order(history)

        elif intent == MessageIntent.CHALLENGE_VIEW:
            # Find last speaking agent and have others respond first
            last_agent = self._get_last_agent(history)
            if last_agent:
                others = [a for a in AgentId if a != last_agent]
                return others + [last_agent]
            return self._balanced_order(history)

        # Default: single agent, balanced selection
        return [self._next_balanced_agent(history)]

    def _balanced_order(self, history: list[DeliberationMessage]) -> list[AgentId]:
        """Get all agents in an order balanced by recent participation."""
        participation = {agent_id: 0 for agent_id in AgentId}

        # Count recent messages (last 10)
        recent = history[-10:] if len(history) > 10 else history
        for msg in recent:
            if hasattr(msg.sender, "agent_id"):
                participation[msg.sender.agent_id] += 1

        # Sort by participation (least first)
        sorted_agents = sorted(participation.keys(), key=lambda a: participation[a])
        return list(sorted_agents)

    def _next_balanced_agent(self, history: list[DeliberationMessage]) -> AgentId:
        """Get the next agent based on balanced participation."""
        return self._balanced_order(history)[0]

    def _get_last_agent(
        self,
        history: list[DeliberationMessage],
    ) -> AgentId | None:
        """Get the ID of the last agent to speak."""
        for msg in reversed(history):
            if hasattr(msg.sender, "agent_id"):
                return msg.sender.agent_id
        return None

    async def generate_initial_opinions(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
    ) -> list[DeliberationMessage]:
        """
        Generate initial opinions from all agents for a new session.

        Agents respond sequentially to create a natural discussion flow:
        1. Strict judge opens with legal framework analysis
        2. Humanist judge responds with individual circumstances perspective
        3. Historian judge synthesizes with precedent context

        Each judge sees what previous judges said and can respond to them.

        Args:
            session_id: New session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference

        Returns:
            List of initial opinion messages from all agents
        """
        logger.info(f"Generating initial opinions for session {session_id}")

        messages: list[DeliberationMessage] = []
        deliberation_order = [AgentId.STRICT, AgentId.HUMANIST, AgentId.HISTORIAN]

        # Generate opinions sequentially so each judge can respond to previous ones
        for i, agent_id in enumerate(deliberation_order):
            agent = self.agents[agent_id]

            try:
                if i == 0:
                    # First judge opens the deliberation
                    response = await agent.generate_initial_opinion(
                        session_id=session_id,
                        case_input=case_input,
                        similar_cases=similar_cases,
                    )
                else:
                    # Subsequent judges respond to the discussion so far
                    response = await agent.respond_to_deliberation(
                        session_id=session_id,
                        case_input=case_input,
                        similar_cases=similar_cases,
                        prior_opinions=messages,
                        is_initial_round=True,
                    )

                messages.append(response)

            except Exception as e:
                logger.error(f"Agent {agent_id.value} failed to generate opinion: {e}")
                continue

        return messages

    async def generate_random_initial_opinion(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
    ) -> DeliberationMessage:
        """
        Generate an initial opinion from a randomly selected judge.

        This provides variety in who opens the deliberation and makes
        session creation faster by only calling one LLM.

        Args:
            session_id: New session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference

        Returns:
            Initial opinion message from a randomly selected judge
        """
        # Randomly select one judge to open the deliberation
        agent_id = random.choice(list(AgentId))
        agent = self.agents[agent_id]

        logger.info(
            f"Generating random initial opinion for session {session_id} "
            f"from {agent_id.value}"
        )

        response = await agent.generate_initial_opinion(
            session_id=session_id,
            case_input=case_input,
            similar_cases=similar_cases,
        )

        return response

    async def continue_discussion(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        history: list[DeliberationMessage],
        num_rounds: int = 1,
    ) -> list[DeliberationMessage]:
        """
        Continue the judicial discussion without user input.

        Allows judges to respond to each other organically, creating
        a more natural deliberation flow where they debate, challenge,
        and build consensus.

        Args:
            session_id: Current session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            history: Full message history so far
            num_rounds: Number of discussion rounds (each round = all judges)

        Returns:
            New messages from the continued discussion
        """
        logger.info(
            f"Continuing discussion for session {session_id}, {num_rounds} round(s)"
        )

        new_messages: list[DeliberationMessage] = []
        current_history = list(history)

        for round_num in range(num_rounds):
            # Determine who should speak next based on balance and recent activity
            speaker_order = self._get_discussion_order(current_history)

            for agent_id in speaker_order:
                agent = self.agents[agent_id]

                try:
                    response = await agent.respond_to_deliberation(
                        session_id=session_id,
                        case_input=case_input,
                        similar_cases=similar_cases,
                        prior_opinions=current_history,
                        is_initial_round=False,
                    )

                    new_messages.append(response)
                    current_history.append(response)

                except Exception as e:
                    logger.error(
                        f"Agent {agent_id.value} failed in discussion round "
                        f"{round_num + 1}: {e}"
                    )
                    continue

        return new_messages

    def _get_discussion_order(
        self,
        history: list[DeliberationMessage],
    ) -> list[AgentId]:
        """
        Determine the order of speakers for the next discussion round.

        Factors:
        - Who spoke least recently
        - Balance of participation
        - Natural conversation flow (responder should go after challenged party)
        """
        # Find the last agent who spoke
        last_speaker = self._get_last_agent(history)

        # Start with the judges who haven't spoken most recently
        participation = {agent_id: 0 for agent_id in AgentId}
        recent = history[-6:] if len(history) > 6 else history

        for msg in recent:
            if hasattr(msg.sender, "agent_id"):
                participation[msg.sender.agent_id] += 1

        # Sort by least participation, but ensure variety
        sorted_agents = sorted(participation.keys(), key=lambda a: participation[a])

        # If last speaker exists, move them to respond last (they can rebut)
        if last_speaker and last_speaker in sorted_agents:
            sorted_agents.remove(last_speaker)
            sorted_agents.append(last_speaker)

        return sorted_agents

    async def process_user_message(
        self,
        session_id: str,
        user_message: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        history: list[DeliberationMessage],
        target_agent: str | None = None,
    ) -> tuple[DeliberationMessage, list[DeliberationMessage]]:
        """
        Process a user message and generate agent responses.

        Args:
            session_id: Current session ID
            user_message: User's message content
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            history: Previous deliberation messages
            target_agent: Specific agent to target (or "all")

        Returns:
            Tuple of (user_message_record, agent_responses)
        """
        # Create user message record
        intent = self.classify_intent(user_message)
        user_msg = DeliberationMessage(
            id=str(uuid4()),
            session_id=session_id,
            sender=UserSender(),
            content=user_message,
            intent=intent.value,
        )

        # Determine response order
        responders = self.determine_response_order(
            message=user_message,
            target_agent=target_agent,
            history=history,
        )

        logger.info(
            f"Processing message for session {session_id}: "
            f"intent={intent.value}, responders={[r.value for r in responders]}"
        )

        # Update history with user message
        updated_history = history + [user_msg]

        # Generate responses from selected agents
        if len(responders) == 1:
            # Single agent response
            response = await self.agents[responders[0]].generate_response(
                session_id=session_id,
                case_input=case_input,
                similar_cases=similar_cases,
                history=updated_history,
                user_message=user_message,
            )
            return user_msg, [response]

        else:
            # Multi-agent responses (sequential to allow for context)
            responses = []
            current_history = updated_history

            for agent_id in responders:
                response = await self.agents[agent_id].generate_response(
                    session_id=session_id,
                    case_input=case_input,
                    similar_cases=similar_cases,
                    history=current_history,
                    user_message=user_message if not responses else None,
                )
                responses.append(response)
                current_history = current_history + [response]

            return user_msg, responses

    # =========================================================================
    # Streaming Methods
    # =========================================================================

    async def generate_initial_opinions_stream(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
    ) -> AsyncIterator[StreamEvent]:
        """
        Generate initial opinions with streaming responses.

        Yields StreamEvents as each judge generates their opinion,
        allowing real-time display of the deliberation.

        Args:
            session_id: New session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference

        Yields:
            StreamEvent objects for each stage of the deliberation
        """
        logger.info(f"Generating streaming initial opinions for session {session_id}")

        deliberation_order = [AgentId.STRICT, AgentId.HUMANIST, AgentId.HISTORIAN]
        accumulated_messages: list[DeliberationMessage] = []

        for i, agent_id in enumerate(deliberation_order):
            agent = self.agents[agent_id]

            # Signal agent is starting
            yield StreamEvent(
                event_type="agent_start",
                agent_id=agent_id,
            )

            full_content = ""
            message_id = ""

            try:
                if i == 0:
                    # First judge opens - use initial opinion prompt
                    prompt = (
                        "Anda membuka musyawarah majelis hakim ini. Sampaikan penilaian "
                        f"awal Anda dari perspektif sebagai {agent.agent_name}.\n\n"
                        "Pernyataan pembuka Anda harus:\n"
                        "1. Merumuskan pertanyaan hukum utama di hadapan majelis\n"
                        "2. Menyatakan posisi awal Anda mengenai putusan\n"
                        "3. Mengidentifikasi pertimbangan hukum terpenting\n"
                        "4. Mengundang diskusi dari rekan hakim mengenai poin-poin tertentu\n\n"
                        "Berbicara secara alami seolah-olah menyapa majelis secara langsung. "
                        "Akhiri dengan pertanyaan atau poin yang mengundang tanggapan."
                    )

                    async for chunk in agent.generate_response_stream(
                        session_id=session_id,
                        case_input=case_input,
                        similar_cases=similar_cases,
                        history=[],
                        user_message=prompt,
                    ):
                        message_id = chunk.message_id or message_id
                        if not chunk.is_complete:
                            full_content += chunk.content
                            yield StreamEvent(
                                event_type="chunk",
                                agent_id=agent_id,
                                content=chunk.content,
                                message_id=message_id,
                            )
                else:
                    # Subsequent judges respond to prior discussion
                    other_opinions = agent._summarize_prior_opinions(accumulated_messages)
                    prompt = (
                        f"Musyawarah telah dimulai. Berikut pendapat rekan-rekan hakim Anda:\n\n"
                        f"{other_opinions}\n\n"
                        f"Sebagai {agent.agent_name}, tanggapi diskusi ini:\n"
                        "1. Akui poin-poin spesifik yang dikemukakan rekan hakim (setuju atau tidak setuju)\n"
                        "2. Tambahkan perspektif unik Anda berdasarkan filosofi yudisial Anda\n"
                        "3. Tunjukkan hal-hal yang mungkin terlewatkan oleh rekan hakim lain\n"
                        "4. Jika Anda tidak setuju dengan hakim lain, jelaskan alasannya dengan hormat\n"
                        "5. Ajukan pertimbangan baru atau pertanyaan untuk diskusi lebih lanjut\n\n"
                        "Sapa rekan-rekan Anda secara langsung dan terlibat dengan penalaran mereka."
                    )

                    async for chunk in agent.generate_response_stream(
                        session_id=session_id,
                        case_input=case_input,
                        similar_cases=similar_cases,
                        history=accumulated_messages,
                        user_message=prompt,
                    ):
                        message_id = chunk.message_id or message_id
                        if not chunk.is_complete:
                            full_content += chunk.content
                            yield StreamEvent(
                                event_type="chunk",
                                agent_id=agent_id,
                                content=chunk.content,
                                message_id=message_id,
                            )

                # Create and accumulate the message
                message = agent.create_message_from_stream(
                    session_id=session_id,
                    message_id=message_id,
                    full_content=full_content,
                )
                accumulated_messages.append(message)

                # Signal agent completion
                yield StreamEvent(
                    event_type="agent_complete",
                    agent_id=agent_id,
                    message_id=message_id,
                    full_content=full_content,
                )

            except Exception as e:
                logger.error(f"Agent {agent_id.value} streaming failed: {e}")
                yield StreamEvent(
                    event_type="agent_error",
                    agent_id=agent_id,
                    content=str(e),
                )

        # Signal deliberation complete
        yield StreamEvent(event_type="deliberation_complete")

    async def process_user_message_stream(
        self,
        session_id: str,
        user_message: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        history: list[DeliberationMessage],
        target_agent: str | None = None,
    ) -> AsyncIterator[StreamEvent]:
        """
        Process a user message and stream agent responses.

        Args:
            session_id: Current session ID
            user_message: User's message content
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            history: Previous deliberation messages
            target_agent: Specific agent to target (or "all")

        Yields:
            StreamEvent objects for the user message and agent responses
        """
        # Create user message
        intent = self.classify_intent(user_message)
        user_msg = DeliberationMessage(
            id=str(uuid4()),
            session_id=session_id,
            sender=UserSender(),
            content=user_message,
            intent=intent.value,
        )

        # Emit user message event
        yield StreamEvent(
            event_type="user_message",
            content=user_message,
            message_id=user_msg.id,
        )

        # Determine response order
        responders = self.determine_response_order(
            message=user_message,
            target_agent=target_agent,
            history=history,
        )

        logger.info(
            f"Streaming response for session {session_id}: "
            f"intent={intent.value}, responders={[r.value for r in responders]}"
        )

        updated_history = history + [user_msg]

        for agent_id in responders:
            agent = self.agents[agent_id]

            yield StreamEvent(
                event_type="agent_start",
                agent_id=agent_id,
            )

            full_content = ""
            message_id = ""

            try:
                async for chunk in agent.generate_response_stream(
                    session_id=session_id,
                    case_input=case_input,
                    similar_cases=similar_cases,
                    history=updated_history,
                    user_message=user_message if agent_id == responders[0] else None,
                ):
                    message_id = chunk.message_id or message_id
                    if not chunk.is_complete:
                        full_content += chunk.content
                        yield StreamEvent(
                            event_type="chunk",
                            agent_id=agent_id,
                            content=chunk.content,
                            message_id=message_id,
                        )

                # Create message and add to history
                message = agent.create_message_from_stream(
                    session_id=session_id,
                    message_id=message_id,
                    full_content=full_content,
                )
                updated_history.append(message)

                yield StreamEvent(
                    event_type="agent_complete",
                    agent_id=agent_id,
                    message_id=message_id,
                    full_content=full_content,
                )

            except Exception as e:
                logger.error(f"Agent {agent_id.value} streaming failed: {e}")
                yield StreamEvent(
                    event_type="agent_error",
                    agent_id=agent_id,
                    content=str(e),
                )

        yield StreamEvent(event_type="deliberation_complete")

    async def continue_discussion_stream(
        self,
        session_id: str,
        case_input: ParsedCaseInput,
        similar_cases: list[SimilarCase],
        history: list[DeliberationMessage],
        num_rounds: int = 1,
        agents_filter: list[AgentId] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        """
        Continue the judicial discussion with streaming responses.

        Args:
            session_id: Current session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference
            history: Full message history so far
            num_rounds: Number of discussion rounds
            agents_filter: Optional list of agents to include. If provided, only
                these agents will speak. If None, all agents speak.

        Yields:
            StreamEvent objects for the continued discussion
        """
        logger.info(
            f"Streaming continued discussion for session {session_id}, "
            f"{num_rounds} round(s)"
        )

        current_history = list(history)

        for round_num in range(num_rounds):
            speaker_order = self._get_discussion_order(current_history)

            # Filter agents if specified
            if agents_filter is not None:
                speaker_order = [
                    agent_id
                    for agent_id in speaker_order
                    if agent_id in agents_filter
                ]

            for agent_id in speaker_order:
                agent = self.agents[agent_id]

                yield StreamEvent(
                    event_type="agent_start",
                    agent_id=agent_id,
                )

                full_content = ""
                message_id = ""

                # Build continuation prompt
                other_opinions = agent._summarize_prior_opinions(current_history[-6:])
                prompt = (
                    f"Diskusi berlanjut. Musyawarah terkini:\n\n"
                    f"{other_opinions}\n\n"
                    f"Sebagai {agent.agent_name}, berkontribusi pada diskusi yang sedang berlangsung:\n"
                    "- Tanggapi poin-poin baru yang diangkat oleh rekan hakim lain\n"
                    "- Bangun di atas area kesepakatan yang mulai terbentuk\n"
                    "- Klarifikasi atau pertahankan posisi Anda jika ditantang\n"
                    "- Arahkan diskusi menuju penyelesaian jika memungkinkan\n\n"
                    "Jaga agar tanggapan Anda tetap fokus untuk memajukan musyawarah."
                )

                try:
                    async for chunk in agent.generate_response_stream(
                        session_id=session_id,
                        case_input=case_input,
                        similar_cases=similar_cases,
                        history=current_history,
                        user_message=prompt,
                    ):
                        message_id = chunk.message_id or message_id
                        if not chunk.is_complete:
                            full_content += chunk.content
                            yield StreamEvent(
                                event_type="chunk",
                                agent_id=agent_id,
                                content=chunk.content,
                                message_id=message_id,
                            )

                    message = agent.create_message_from_stream(
                        session_id=session_id,
                        message_id=message_id,
                        full_content=full_content,
                    )
                    current_history.append(message)

                    yield StreamEvent(
                        event_type="agent_complete",
                        agent_id=agent_id,
                        message_id=message_id,
                        full_content=full_content,
                    )

                except Exception as e:
                    logger.error(
                        f"Agent {agent_id.value} streaming failed in round "
                        f"{round_num + 1}: {e}"
                    )
                    yield StreamEvent(
                        event_type="agent_error",
                        agent_id=agent_id,
                        content=str(e),
                    )

        yield StreamEvent(event_type="deliberation_complete")


# =============================================================================
# Singleton
# =============================================================================

_orchestrator: AgentOrchestrator | None = None


def get_agent_orchestrator() -> AgentOrchestrator:
    """Get or create the agent orchestrator singleton."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = AgentOrchestrator()
    return _orchestrator
