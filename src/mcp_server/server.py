"""
AutoInsight Tab 2 - MCP Server
Model Context Protocol server exposing automotive news and events tools
"""

from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
from contextlib import asynccontextmanager

from database import get_snowflake_connection
from tools.news_tools import NewsTools
from tools.events_tools import EventsTools
from tools.dashboard_tools import DashboardTools
from models import (
    NewsByManufacturerRequest,
    BreakingNewsRequest,
    SearchEventsRequest
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting AutoInsight MCP Server...")
    yield
    logger.info("🛑 Shutting down AutoInsight MCP Server...")

# Initialize FastAPI app
app = FastAPI(
    title="AutoInsight MCP Server",
    description="Model Context Protocol server for automotive news and events intelligence",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Initialize tool classes
news_tools = NewsTools()
events_tools = EventsTools()
dashboard_tools = DashboardTools()

# ============================================================================
# MCP PROTOCOL ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "server": "AutoInsight MCP Server",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/mcp/tools")
async def list_tools():
    """
    MCP Protocol: List all available tools
    """
    return {
        "tools": [
            {
                "name": "get_breaking_news",
                "description": "Get breaking automotive news from the last N hours",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "hours": {"type": "integer", "default": 24, "description": "Hours to look back"},
                        "limit": {"type": "integer", "default": 10}
                    }
                }
            },
            {
                "name": "get_news_by_manufacturer",
                "description": "Get all recent news about a specific manufacturer",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "manufacturer": {"type": "string", "description": "Manufacturer name (e.g., 'Ford', 'Tesla')"},
                        "days_back": {"type": "integer", "default": 7},
                        "limit": {"type": "integer", "default": 20}
                    },
                    "required": ["manufacturer"]
                }
            },
            {
                "name": "search_upcoming_events",
                "description": "Search upcoming automotive events with filters",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string", "description": "Type of event (auto show, conference, etc.)"},
                        "location": {"type": "string", "description": "Event location"},
                        "days_ahead": {"type": "integer", "default": 90},
                        "limit": {"type": "integer", "default": 20}
                    }
                }
            },
            {
                "name": "generate_article_dashboard",
                "description": "Generate AI-powered analysis dashboard for a news article using Claude",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "article_id": {"type": "integer", "description": "Article ID to analyze"}
                    },
                    "required": ["article_id"]
                }
            },
            {
                "name": "generate_event_dashboard",
                "description": "Generate AI-powered analysis dashboard for an automotive event using Claude",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "event_id": {"type": "integer", "description": "Event ID to analyze"}
                    },
                    "required": ["event_id"]
                }
            }
        ]
    }

# ============================================================================
# TOOL ENDPOINTS - ONLY THE ONES YOU NEED
# ============================================================================

@app.post("/mcp/tool/get_breaking_news")
async def get_breaking_news(request: BreakingNewsRequest):
    """Get breaking news from last N hours"""
    try:
        results = news_tools.get_breaking_news(request.hours, request.limit)
        return {"success": True, "count": len(results), "articles": results}
    except Exception as e:
        logger.error(f"Error in get_breaking_news: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mcp/tool/get_news_by_manufacturer")
async def get_news_by_manufacturer(request: NewsByManufacturerRequest):
    """Get news for specific manufacturer"""
    try:
        results = news_tools.get_news_by_manufacturer(
            request.manufacturer, 
            request.days_back, 
            request.limit
        )
        return {"success": True, "manufacturer": request.manufacturer, "count": len(results), "articles": results}
    except Exception as e:
        logger.error(f"Error in get_news_by_manufacturer: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mcp/tool/search_upcoming_events")
async def search_upcoming_events(request: SearchEventsRequest):
    """Search upcoming automotive events"""
    try:
        results = events_tools.search_events(
            request.event_type, 
            request.location, 
            request.days_ahead, 
            request.limit
        )
        return {"success": True, "count": len(results), "events": results}
    except Exception as e:
        logger.error(f"Error in search_upcoming_events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mcp/tool/generate_article_dashboard")
async def generate_article_dashboard(request: Dict[Any, Any] = Body(...)):
    """
    Generate AI-powered dashboard analysis for a news article
    Uses Claude LLM via the DashboardTools class
    """
    try:
        article_id = request.get('article_id')
        
        if not article_id:
            raise HTTPException(status_code=400, detail="article_id is required")
        
        logger.info(f"Generating dashboard for article_id={article_id}")
        
        dashboard = dashboard_tools.generate_article_dashboard(article_id)
        
        return {
            "success": True,
            **dashboard
        }
        
    except ValueError as e:
        logger.error(f"ValueError in generate_article_dashboard: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in generate_article_dashboard: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/mcp/tool/generate_event_dashboard")
async def generate_event_dashboard(request: Dict[Any, Any] = Body(...)):
    """
    Generate AI-powered dashboard analysis for an automotive event
    Uses Claude LLM via the DashboardTools class
    """
    try:
        event_id = request.get('event_id')
        
        if not event_id:
            raise HTTPException(status_code=400, detail="event_id is required")
        
        logger.info(f"Generating dashboard for event_id={event_id}")
        
        dashboard = dashboard_tools.generate_event_dashboard(event_id)
        
        return {
            "success": True,
            **dashboard
        }
        
    except ValueError as e:
        logger.error(f"ValueError in generate_event_dashboard: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in generate_event_dashboard: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)