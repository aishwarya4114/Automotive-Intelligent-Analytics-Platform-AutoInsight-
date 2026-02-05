"""
MCP Tool Wrappers for LangGraph Agent
Cleaned up - only tools actually used by the application
"""

import requests
from typing import List, Optional, Dict, Any
from langchain_core.tools import tool
import os

MCP_BASE_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8001")

# ============================================================================
# ACTIVE NEWS TOOLS
# ============================================================================

@tool
def get_breaking_news(hours: int = 24, limit: int = 10) -> str:
    """
    Get breaking automotive news from the last N hours.
    
    Use this tool when the user asks about recent or breaking news.
    
    Args:
        hours: How many hours back to look (default: 24)
        limit: Maximum number of results (default: 10)
        
    Returns:
        JSON string with recent articles or error message
    """
    try:
        payload = {"hours": hours, "limit": limit}
        
        response = requests.post(
            f"{MCP_BASE_URL}/mcp/tool/get_breaking_news",
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        return str(response.json())
    except Exception as e:
        return f'{{"success": false, "error": "{str(e)}"}}'


@tool
def get_news_by_manufacturer(
    manufacturer: str,
    days_back: int = 7,
    limit: int = 10
) -> str:
    """
    Get all news about a specific automotive manufacturer.
    
    Use this tool when the user asks about a specific car company or brand.
    
    Args:
        manufacturer: Name of manufacturer (e.g., "Ford", "Tesla", "GM", "Toyota", "Mercedes")
        days_back: How many days back to search (default: 7)
        limit: Maximum number of results (default: 10)
        
    Returns:
        JSON string with manufacturer-specific articles or error message
    """
    try:
        payload = {
            "manufacturer": manufacturer,
            "days_back": days_back,
            "limit": limit
        }
        
        response = requests.post(
            f"{MCP_BASE_URL}/mcp/tool/get_news_by_manufacturer",
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        return str(response.json())
    except Exception as e:
        return f'{{"success": false, "error": "{str(e)}"}}'


# ============================================================================
# ACTIVE EVENTS TOOLS
# ============================================================================

@tool
def search_upcoming_events(
    event_type: str = "",
    location: str = "",
    days_ahead: int = 90,
    limit: int = 10
) -> str:
    """
    Search for upcoming automotive events.
    
    Use this when the user asks about auto shows, conferences, or industry events.
    
    Args:
        event_type: Type of event (e.g., "auto show", "conference", "congress"). Leave empty for all.
        location: Event location (e.g., "Detroit", "Las Vegas", "Shanghai"). Leave empty for all.
        days_ahead: How many days ahead to search (default: 90)
        limit: Maximum number of results (default: 10)
        
    Returns:
        JSON string with upcoming events or error message
    """
    try:
        payload = {
            "event_type": event_type if event_type else None,
            "location": location if location else None,
            "days_ahead": days_ahead,
            "limit": limit
        }
        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}
        
        response = requests.post(
            f"{MCP_BASE_URL}/mcp/tool/search_upcoming_events",
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        return str(response.json())
    except Exception as e:
        return f'{{"success": false, "error": "{str(e)}"}}'


# ============================================================================
# ACTIVE DASHBOARD TOOLS
# ============================================================================

@tool
def generate_article_dashboard(article_id: int) -> str:
    """
    Generate AI-powered analysis dashboard for a news article.
    
    Use this when the user wants deep analysis of a specific article.
    
    Args:
        article_id: The article ID to analyze
        
    Returns:
        JSON string with AI-generated dashboard or error message
    """
    try:
        payload = {"article_id": article_id}
        
        response = requests.post(
            f"{MCP_BASE_URL}/mcp/tool/generate_article_dashboard",
            json=payload,
            timeout=30  # Longer timeout for LLM call
        )
        response.raise_for_status()
        return str(response.json())
    except Exception as e:
        return f'{{"success": false, "error": "{str(e)}"}}'


@tool
def generate_event_dashboard(event_id: int) -> str:
    """
    Generate AI-powered analysis dashboard for an automotive event.
    
    Use this when the user wants deep analysis of a specific event.
    
    Args:
        event_id: The event ID to analyze
        
    Returns:
        JSON string with AI-generated dashboard or error message
    """
    try:
        payload = {"event_id": event_id}
        
        response = requests.post(
            f"{MCP_BASE_URL}/mcp/tool/generate_event_dashboard",
            json=payload,
            timeout=30  # Longer timeout for LLM call
        )
        response.raise_for_status()
        return str(response.json())
    except Exception as e:
        return f'{{"success": false, "error": "{str(e)}"}}'


# ============================================================================
# TOOL LIST FOR AGENT
# ============================================================================

ACTIVE_TOOLS = [
    get_breaking_news,
    get_news_by_manufacturer,
    search_upcoming_events,
    generate_article_dashboard,
    generate_event_dashboard
]