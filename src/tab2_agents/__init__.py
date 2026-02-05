"""
AutoInsight Agents Package
"""

from .supervisor_agent import AutoInsightAgent, create_agent
from .mcp_tools import ALL_TOOLS
from .prompts import SYSTEM_PROMPT, USER_ROLE_DESCRIPTIONS, EXAMPLE_QUERIES

__all__ = [
    'AutoInsightAgent',
    'create_agent',
    'ALL_TOOLS',
    'SYSTEM_PROMPT',
    'USER_ROLE_DESCRIPTIONS',
    'EXAMPLE_QUERIES'
]