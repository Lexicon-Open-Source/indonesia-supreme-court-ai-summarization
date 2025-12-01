"""
Agent Orchestrator for the Virtual Judicial Council.

Coordinates the three judge agents to produce coherent deliberations:
- Routes user messages to appropriate agents
- Determines response order based on context
- Manages multi-agent discussions
- Ensures balanced participation
"""

import asyncio
import logging
import re
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

        Agents respond in parallel for efficiency.

        Args:
            session_id: New session ID
            case_input: Parsed case information
            similar_cases: Similar cases for reference

        Returns:
            List of initial opinion messages from all agents
        """
        logger.info(f"Generating initial opinions for session {session_id}")

        # Generate in parallel
        tasks = [
            self.agents[agent_id].generate_initial_opinion(
                session_id=session_id,
                case_input=case_input,
                similar_cases=similar_cases,
            )
            for agent_id in [AgentId.STRICT, AgentId.HUMANIST, AgentId.HISTORIAN]
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        messages = []
        for i, result in enumerate(results):
            agent_id = list(AgentId)[i]
            if isinstance(result, Exception):
                logger.error(f"Agent {agent_id.value} failed to generate opinion: {result}")
                continue
            messages.append(result)

        return messages

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
