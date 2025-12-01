"""
AI Judge Agents for the Virtual Judicial Council.

Provides three distinct judicial personas:
- StrictConstructionistAgent: Focuses on literal law interpretation
- HumanistAgent: Emphasizes rehabilitation and individual circumstances
- HistorianAgent: Provides historical context and precedent analysis
"""

from src.council.agents.base import BaseJudgeAgent, StreamChunk
from src.council.agents.historian import HistorianAgent
from src.council.agents.humanist import HumanistAgent
from src.council.agents.orchestrator import AgentOrchestrator, StreamEvent
from src.council.agents.strict import StrictConstructionistAgent

__all__ = [
    "BaseJudgeAgent",
    "StreamChunk",
    "StreamEvent",
    "StrictConstructionistAgent",
    "HumanistAgent",
    "HistorianAgent",
    "AgentOrchestrator",
]
