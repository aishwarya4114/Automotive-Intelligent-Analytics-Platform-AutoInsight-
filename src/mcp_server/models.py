"""
Pydantic models for MCP Server request/response validation
Cleaned up - only models for active tools
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# ============================================================================
# REQUEST MODELS - ONLY ACTIVE TOOLS
# ============================================================================

class BreakingNewsRequest(BaseModel):
    """Request model for breaking news"""
    hours: int = Field(24, ge=1, le=168, description="Hours to look back")
    limit: int = Field(10, ge=1, le=50, description="Maximum results to return")

class NewsByManufacturerRequest(BaseModel):
    """Request model for manufacturer-specific news"""
    manufacturer: str = Field(..., description="Manufacturer name")
    days_back: int = Field(7, ge=1, le=365, description="Number of days to look back")
    limit: int = Field(20, ge=1, le=100, description="Maximum results to return")

class SearchEventsRequest(BaseModel):
    """Request model for searching events"""
    event_type: Optional[str] = Field(None, description="Type of event")
    location: Optional[str] = Field(None, description="Event location")
    days_ahead: int = Field(90, ge=1, le=730, description="Days ahead to search")
    limit: int = Field(20, ge=1, le=100, description="Maximum results to return")

# Dashboard request models are handled as plain dicts via Body(...)
# No Pydantic models needed since we use Dict[Any, Any] in the endpoint

# ============================================================================
# RESPONSE MODELS (Keep for documentation)
# ============================================================================

class ArticleResponse(BaseModel):
    """Single article response"""
    article_id: int
    title: str
    url: str
    published_date: Optional[str]
    source: Optional[str]
    author: Optional[str]
    summary: Optional[str]
    category: Optional[str]
    sentiment: Optional[str]

class EventResponse(BaseModel):
    """Single event response"""
    event_id: int
    title: str
    event_type: Optional[str]
    date_start: Optional[str]
    date_end: Optional[str]
    location: Optional[str]
    description: Optional[str]
    url: Optional[str]

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    server: str
    version: str
    timestamp: str

class ErrorResponse(BaseModel):
    """Error response"""
    success: bool = False
    error: str
    detail: Optional[str] = None