"""
News-related MCP tools for AutoInsight Tab 2
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
from database import execute_query

logger = logging.getLogger(__name__)

class NewsTools:
    """News search and analysis tools"""
    
    def search_news(
        self,
        query: Optional[str] = None,
        manufacturers: Optional[List[str]] = None,
        category: Optional[str] = None,
        days_back: int = 7,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Search news articles with multiple filters
        """
        sql = """
        SELECT 
            ARTICLE_ID,
            TITLE,
            URL,
            PUBLISHED_DATE,
            SOURCE,
            AUTHOR,
            SUMMARY,
            CATEGORY,
            SENTIMENT,
            IMPORTANCE_SCORE
        FROM NEWS_ARTICLES
        WHERE PUBLISHED_DATE >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
        """
        params = [days_back]
        
        # Add text search if query provided
        if query:
            sql += " AND (LOWER(TITLE) LIKE %s OR LOWER(SUMMARY) LIKE %s)"
            search_term = f"%{query.lower()}%"
            params.extend([search_term, search_term])
        
        # Add manufacturer filter
        if manufacturers:
            # For simplicity, search in title/summary
            # In production, you'd use the MENTIONED_MANUFACTURERS array
            manufacturer_conditions = " OR ".join(
                ["(LOWER(TITLE) LIKE %s OR LOWER(SUMMARY) LIKE %s)"] * len(manufacturers)
            )
            sql += f" AND ({manufacturer_conditions})"
            for mfr in manufacturers:
                search_term = f"%{mfr.lower()}%"
                params.extend([search_term, search_term])
        
        sql += " ORDER BY PUBLISHED_DATE DESC LIMIT %s"
        params.append(limit)
        
        results = execute_query(sql, tuple(params))
        return self._format_articles(results)
    
    def get_breaking_news(self, hours: int = 24, limit: int = 10) -> List[Dict[str, Any]]:
        """Get breaking news from last N hours"""
        sql = """
        SELECT 
            ARTICLE_ID,
            TITLE,
            URL,
            PUBLISHED_DATE,
            SOURCE,
            AUTHOR,
            SUMMARY,
            CATEGORY,
            SENTIMENT,
            IMPORTANCE_SCORE
        FROM NEWS_ARTICLES
        WHERE PUBLISHED_DATE >= DATEADD(hour, -%s, CURRENT_TIMESTAMP())
        ORDER BY PUBLISHED_DATE DESC
        LIMIT %s
        """
        results = execute_query(sql, (hours, limit))
        return self._format_articles(results)
    
    def get_news_by_manufacturer(
        self,
        manufacturer: str,
        days_back: int = 7,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get all news about specific manufacturer"""
        sql = """
        SELECT 
            ARTICLE_ID,
            TITLE,
            URL,
            PUBLISHED_DATE,
            SOURCE,
            AUTHOR,
            SUMMARY,
            CATEGORY,
            SENTIMENT,
            IMPORTANCE_SCORE
        FROM NEWS_ARTICLES
        WHERE PUBLISHED_DATE >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
          AND (LOWER(TITLE) LIKE %s OR LOWER(SUMMARY) LIKE %s)
        ORDER BY PUBLISHED_DATE DESC
        LIMIT %s
        """
        search_term = f"%{manufacturer.lower()}%"
        results = execute_query(sql, (days_back, search_term, search_term, limit))
        return self._format_articles(results)
    
    def analyze_sentiment(
        self,
        manufacturers: List[str],
        days_back: int = 30
    ) -> Dict[str, Any]:
        """Analyze sentiment distribution for manufacturers"""
        analysis = {}
        
        for manufacturer in manufacturers:
            sql = """
            SELECT 
                SENTIMENT,
                COUNT(*) as count,
                AVG(IMPORTANCE_SCORE) as avg_importance
            FROM NEWS_ARTICLES
            WHERE PUBLISHED_DATE >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
              AND (LOWER(TITLE) LIKE %s OR LOWER(SUMMARY) LIKE %s)
            GROUP BY SENTIMENT
            """
            search_term = f"%{manufacturer.lower()}%"
            results = execute_query(sql, (days_back, search_term, search_term))
            
            analysis[manufacturer] = {
                "sentiment_distribution": results,
                "total_articles": sum(r['COUNT'] for r in results) if results else 0,
                "period_days": days_back
            }
        
        return analysis
    
    def generate_briefing(
        self,
        user_role: str,
        focus_manufacturers: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Generate personalized daily briefing"""
        briefing = {
            "role": user_role,
            "generated_at": datetime.now().isoformat(),
            "sections": {}
        }
        
        # Breaking news (last 24 hours)
        briefing["sections"]["breaking_news"] = self.get_breaking_news(hours=24, limit=5)
        
        # Role-specific content
        if user_role == "executive":
            # Focus on high-level strategic news
            briefing["sections"]["strategic_updates"] = self.search_news(
                query="merger acquisition partnership",
                days_back=7,
                limit=5
            )
        elif user_role == "analyst":
            # Focus on market data and trends
            briefing["sections"]["market_analysis"] = self.search_news(
                query="sales market share revenue",
                days_back=7,
                limit=5
            )
        elif user_role == "product_manager":
            # Focus on product launches and technology
            briefing["sections"]["product_news"] = self.search_news(
                query="launch release technology innovation",
                days_back=7,
                limit=5
            )
        
        # Manufacturer-specific updates
        if focus_manufacturers:
            briefing["sections"]["manufacturer_updates"] = {}
            for mfr in focus_manufacturers:
                briefing["sections"]["manufacturer_updates"][mfr] = self.get_news_by_manufacturer(
                    mfr, days_back=7, limit=3
                )
        
        return briefing
    
    def competitive_intelligence(
        self,
        manufacturers: List[str],
        days_back: int = 30
    ) -> Dict[str, Any]:
        """Generate competitive intelligence report"""
        report = {
            "manufacturers": manufacturers,
            "period_days": days_back,
            "generated_at": datetime.now().isoformat(),
            "comparison": {}
        }
        
        for manufacturer in manufacturers:
            articles = self.get_news_by_manufacturer(manufacturer, days_back, limit=50)
            
            report["comparison"][manufacturer] = {
                "total_mentions": len(articles),
                "recent_articles": articles[:5],  # Top 5 most recent
                "sentiment_summary": self._calculate_sentiment_summary(articles)
            }
        
        return report
    
    def _format_articles(self, results: List[Dict]) -> List[Dict[str, Any]]:
        """Format article results for API response"""
        formatted = []
        for row in results:
            formatted.append({
                "article_id": row.get('ARTICLE_ID'),
                "title": row.get('TITLE'),
                "url": row.get('URL'),
                "published_date": row.get('PUBLISHED_DATE').isoformat() if row.get('PUBLISHED_DATE') else None,
                "source": row.get('SOURCE'),
                "author": row.get('AUTHOR'),
                "summary": row.get('SUMMARY'),
                "category": row.get('CATEGORY'),
                "sentiment": row.get('SENTIMENT'),
                "importance_score": float(row.get('IMPORTANCE_SCORE')) if row.get('IMPORTANCE_SCORE') else None
            })
        return formatted
    
    def _calculate_sentiment_summary(self, articles: List[Dict]) -> Dict[str, Any]:
        """Calculate sentiment summary from articles"""
        if not articles:
            return {"positive": 0, "neutral": 0, "negative": 0, "avg_importance": 0}
        
        sentiments = {"positive": 0, "neutral": 0, "negative": 0}
        total_importance = 0
        count = 0
        
        for article in articles:
            sentiment = article.get("sentiment", "").lower()
            if sentiment in sentiments:
                sentiments[sentiment] += 1
            
            importance = article.get("importance_score")
            if importance is not None:
                total_importance += importance
                count += 1
        
        return {
            **sentiments,
            "avg_importance": total_importance / count if count > 0 else 0
        }