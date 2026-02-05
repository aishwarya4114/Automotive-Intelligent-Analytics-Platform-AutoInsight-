"""
Events-related MCP tools for AutoInsight Tab 2
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import logging
from database import execute_query

logger = logging.getLogger(__name__)

class EventsTools:
    """Events search and discovery tools"""
    
    def search_events(
        self,
        event_type: Optional[str] = None,
        location: Optional[str] = None,
        days_ahead: int = 90,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Search upcoming automotive events with filters
        """
        sql = """
        SELECT 
            EVENT_ID,
            TITLE,
            EVENT_TYPE,
            DATE_START,
            DATE_END,
            LOCATION,
            DESCRIPTION,
            URL,
            PARTICIPATING_MANUFACTURERS,
            KEY_TOPICS,
            RELEVANCE_SCORE,
            STRATEGIC_IMPORTANCE
        FROM AUTOMOTIVE_EVENTS
        WHERE DATE_START >= CURRENT_DATE()
          AND DATE_START <= DATEADD(day, %s, CURRENT_DATE())
        """
        params = [days_ahead]
        
        # Add event type filter
        if event_type:
            sql += " AND LOWER(EVENT_TYPE) LIKE %s"
            params.append(f"%{event_type.lower()}%")
        
        # Add location filter
        if location:
            sql += " AND LOWER(LOCATION) LIKE %s"
            params.append(f"%{location.lower()}%")
        
        sql += " ORDER BY DATE_START ASC LIMIT %s"
        params.append(limit)
        
        results = execute_query(sql, tuple(params))
        return self._format_events(results)
    
    def get_events_by_manufacturer(
        self,
        manufacturer: str,
        days_ahead: int = 90
    ) -> List[Dict[str, Any]]:
        """Get upcoming events featuring specific manufacturer"""
        sql = """
        SELECT 
            EVENT_ID,
            TITLE,
            EVENT_TYPE,
            DATE_START,
            DATE_END,
            LOCATION,
            DESCRIPTION,
            URL,
            PARTICIPATING_MANUFACTURERS,
            KEY_TOPICS,
            RELEVANCE_SCORE,
            STRATEGIC_IMPORTANCE
        FROM AUTOMOTIVE_EVENTS
        WHERE DATE_START >= CURRENT_DATE()
          AND DATE_START <= DATEADD(day, %s, CURRENT_DATE())
          AND (
              LOWER(TITLE) LIKE %s 
              OR LOWER(DESCRIPTION) LIKE %s
              OR ARRAY_CONTAINS(%s::VARIANT, PARTICIPATING_MANUFACTURERS)
          )
        ORDER BY DATE_START ASC
        """
        search_term = f"%{manufacturer.lower()}%"
        results = execute_query(sql, (days_ahead, search_term, search_term, manufacturer))
        return self._format_events(results)
    
    def get_events_calendar(self, days_ahead: int = 90) -> Dict[str, Any]:
        """Get events calendar view organized by month"""
        sql = """
        SELECT 
            EVENT_ID,
            TITLE,
            EVENT_TYPE,
            DATE_START,
            DATE_END,
            LOCATION,
            DESCRIPTION,
            URL,
            PARTICIPATING_MANUFACTURERS,
            KEY_TOPICS,
            RELEVANCE_SCORE,
            STRATEGIC_IMPORTANCE
        FROM AUTOMOTIVE_EVENTS
        WHERE DATE_START >= CURRENT_DATE()
          AND DATE_START <= DATEADD(day, %s, CURRENT_DATE())
        ORDER BY DATE_START ASC
        """
        results = execute_query(sql, (days_ahead,))
        events = self._format_events(results)
        
        # Organize by month
        calendar = {}
        for event in events:
            date_start = datetime.fromisoformat(event['date_start'])
            month_key = date_start.strftime('%Y-%m')
            
            if month_key not in calendar:
                calendar[month_key] = {
                    "month": date_start.strftime('%B %Y'),
                    "events": []
                }
            
            calendar[month_key]["events"].append(event)
        
        return {
            "period_days": days_ahead,
            "calendar": list(calendar.values())
        }
    
    def get_major_auto_shows(self, days_ahead: int = 365) -> List[Dict[str, Any]]:
        """Get major auto shows coming up"""
        sql = """
        SELECT 
            EVENT_ID,
            TITLE,
            EVENT_TYPE,
            DATE_START,
            DATE_END,
            LOCATION,
            DESCRIPTION,
            URL,
            PARTICIPATING_MANUFACTURERS,
            KEY_TOPICS,
            RELEVANCE_SCORE,
            STRATEGIC_IMPORTANCE
        FROM AUTOMOTIVE_EVENTS
        WHERE DATE_START >= CURRENT_DATE()
          AND DATE_START <= DATEADD(day, %s, CURRENT_DATE())
          AND LOWER(EVENT_TYPE) LIKE '%auto show%'
        ORDER BY DATE_START ASC
        """
        results = execute_query(sql, (days_ahead,))
        return self._format_events(results)
    
    def get_upcoming_conferences(self, days_ahead: int = 180) -> List[Dict[str, Any]]:
        """Get upcoming industry conferences"""
        sql = """
        SELECT 
            EVENT_ID,
            TITLE,
            EVENT_TYPE,
            DATE_START,
            DATE_END,
            LOCATION,
            DESCRIPTION,
            URL,
            PARTICIPATING_MANUFACTURERS,
            KEY_TOPICS,
            RELEVANCE_SCORE,
            STRATEGIC_IMPORTANCE
        FROM AUTOMOTIVE_EVENTS
        WHERE DATE_START >= CURRENT_DATE()
          AND DATE_START <= DATEADD(day, %s, CURRENT_DATE())
          AND (
              LOWER(EVENT_TYPE) LIKE '%conference%'
              OR LOWER(EVENT_TYPE) LIKE '%congress%'
              OR LOWER(EVENT_TYPE) LIKE '%summit%'
          )
        ORDER BY DATE_START ASC
        """
        results = execute_query(sql, (days_ahead,))
        return self._format_events(results)
    
    def _format_events(self, results: List[Dict]) -> List[Dict[str, Any]]:
        """Format event results for API response"""
        formatted = []
        for row in results:
            formatted.append({
                "event_id": row.get('EVENT_ID'),
                "title": row.get('TITLE'),
                "event_type": row.get('EVENT_TYPE'),
                "date_start": row.get('DATE_START').isoformat() if row.get('DATE_START') else None,
                "date_end": row.get('DATE_END').isoformat() if row.get('DATE_END') else None,
                "location": row.get('LOCATION'),
                "description": row.get('DESCRIPTION'),
                "url": row.get('URL'),
                "participating_manufacturers": row.get('PARTICIPATING_MANUFACTURERS'),
                "key_topics": row.get('KEY_TOPICS'),
                "relevance_score": float(row.get('RELEVANCE_SCORE')) if row.get('RELEVANCE_SCORE') else None,
                "strategic_importance": row.get('STRATEGIC_IMPORTANCE')
            })
        return formatted