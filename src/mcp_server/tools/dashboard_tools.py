"""
Dashboard generation tools using Claude LLM
Generates structured analysis for articles and events on-demand
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
import anthropic
import instructor
import os
import logging
from database import execute_query

logger = logging.getLogger(__name__)


# ============================================================================
# PYDANTIC MODELS FOR STRUCTURED OUTPUT
# ============================================================================

class KeyEntities(BaseModel):
    """Extracted entities from article/event"""
    manufacturers: List[str] = Field(default_factory=list, description="Automotive manufacturers mentioned")
    suppliers: List[str] = Field(default_factory=list, description="Tier 1/2 suppliers mentioned")
    executives: List[str] = Field(default_factory=list, description="Executives with titles (e.g., 'John Smith, CEO')")
    organizations: List[str] = Field(default_factory=list, description="Organizations (NHTSA, EPA, UAW, etc.)")
    regions: List[str] = Field(default_factory=list, description="Geographic regions affected")


class Classification(BaseModel):
    """Article/Event classification"""
    category: str = Field(default="General", description="Primary category")
    sub_category: str = Field(default="", description="Specific sub-classification")
    sentiment: str = Field(default="Neutral", description="Positive | Negative | Neutral | Mixed")
    confidence: float = Field(default=0.7, ge=0.0, le=1.0, description="Classification confidence score")


class StockImpact(BaseModel):
    """Stock market impact assessment"""
    affected_tickers: List[str] = Field(default_factory=list, description="Stock symbols (e.g., TSLA, F, GM)")
    direction: str = Field(default="Neutral", description="Positive | Negative | Mixed | Neutral")
    magnitude: str = Field(default="Minimal", description="Minimal | Moderate | Significant | Major")


class ImpactAssessment(BaseModel):
    """Impact assessment section"""
    severity: int = Field(default=5, ge=1, le=10, description="1-10 severity score")
    time_horizon: str = Field(default="Near-term", description="Immediate | Near-term | Medium-term | Long-term")
    affected_segments: List[str] = Field(default_factory=list, description="Industry segments affected")
    stock_impact: StockImpact = Field(default_factory=StockImpact)


class ImpactFlags(BaseModel):
    """Binary impact indicators"""
    breaking_news: bool = Field(default=False, description="Time-sensitive urgent news")
    recall_related: bool = Field(default=False, description="Vehicle recall mentioned")
    affects_guidance: bool = Field(default=False, description="Impacts earnings/forecasts")
    supply_chain_disruption: bool = Field(default=False, description="Supply chain impact")
    regulatory_action: bool = Field(default=False, description="Government/regulatory involvement")
    labor_action: bool = Field(default=False, description="Union/strike/layoff related")
    merger_acquisition: bool = Field(default=False, description="M&A activity")


class ExecutiveSummary(BaseModel):
    """Executive summary section"""
    headline: str = Field(default="", description="One-line summary (max 100 chars)")
    situation: str = Field(default="", description="2-3 sentence factual overview")
    implications: str = Field(default="", description="Strategic/financial implications")
    key_figures: List[str] = Field(default_factory=list, description="Specific numbers and data points")
    action_items: List[str] = Field(default_factory=list, description="Follow-up items for stakeholders")


class RelatedContext(BaseModel):
    """Historical and competitive context"""
    historical_precedent: str = Field(default="", description="Similar past events and outcomes")
    competitive_dynamics: str = Field(default="", description="Impact on competitive positioning")


class ArticleDashboard(BaseModel):
    """Complete dashboard for news article"""
    key_entities: KeyEntities = Field(default_factory=KeyEntities)
    classification: Classification = Field(default_factory=Classification)
    impact_assessment: ImpactAssessment = Field(default_factory=ImpactAssessment)
    impact_flags: ImpactFlags = Field(default_factory=ImpactFlags)
    executive_summary: ExecutiveSummary = Field(default_factory=ExecutiveSummary)
    related_context: RelatedContext = Field(default_factory=RelatedContext)


class EventProfile(BaseModel):
    """Event classification profile"""
    event_type: str = Field(default="Conference", description="Type of event")
    tier: str = Field(default="Tier 2", description="Tier 1 (Major) | Tier 2 (Regional) | Tier 3 (Niche)")
    primary_audience: str = Field(default="Industry Professionals", description="Target audience")


class StrategicImportance(BaseModel):
    """Strategic importance assessment"""
    relevance_score: float = Field(default=0.5, ge=0.0, le=1.0, description="0-1 relevance score")
    importance_level: str = Field(default="Medium", description="Critical | High | Medium | Low")
    rationale: str = Field(default="", description="Explanation of significance")


class ExpectedCoverage(BaseModel):
    """Expected event coverage"""
    participants: List[str] = Field(default_factory=list, description="Expected participants")
    key_topics: List[str] = Field(default_factory=list, description="Primary themes expected")
    potential_market_movers: List[str] = Field(default_factory=list, description="Potential market-moving announcements")


class AnalystBrief(BaseModel):
    """Analyst briefing section"""
    summary: str = Field(default="", description="3-4 sentence professional overview")
    watch_items: List[str] = Field(default_factory=list, description="Items to monitor")
    historical_context: str = Field(default="", description="Past notable announcements")
    attendance_recommendation: str = Field(default="Optional", description="Recommended | Optional | Monitor Remotely | Skip")


class EventLogistics(BaseModel):
    """Event logistics information"""
    registration_status: str = Field(default="Unknown", description="Open | Closed | Invite Only | Unknown")
    media_coverage: str = Field(default="Unknown", description="Expected media coverage")
    key_dates: List[str] = Field(default_factory=list, description="Important related dates")


class EventDashboard(BaseModel):
    """Complete dashboard for event"""
    key_entities: KeyEntities = Field(default_factory=KeyEntities)
    event_profile: EventProfile = Field(default_factory=EventProfile)
    strategic_importance: StrategicImportance = Field(default_factory=StrategicImportance)
    expected_coverage: ExpectedCoverage = Field(default_factory=ExpectedCoverage)
    analyst_brief: AnalystBrief = Field(default_factory=AnalystBrief)
    logistics: EventLogistics = Field(default_factory=EventLogistics)


# ============================================================================
# PROMPTS - Professional/Institutional Grade
# ============================================================================

ARTICLE_DASHBOARD_PROMPT = """Analyze this news article. Provide professional, institutional-grade analysis.
Use precise terminology. No emojis. No casual language. Be objective and factual.

ARTICLE:
Title: {title}
Source: {source}
Date: {published_date}
Author: {author}
Content: {summary}

=== SECTION INSTRUCTIONS ===

KEY ENTITIES:
- Extract ALL explicitly mentioned companies, people, and locations
- Manufacturers: Include parent companies and subsidiaries separately
- Suppliers: Tier 1/2 automotive suppliers only
- Executives: Full name with title (e.g., "Mary Barra, CEO of General Motors")
- Organizations: Regulatory bodies, unions, industry groups
- Regions: Be specific (e.g., "North America", "EU", "China")

CLASSIFICATION:
- Category options: M&A | Product Launch | Regulatory | Labor | Supply Chain | Financial | Technology | Market Dynamics | Legal | ESG | General
- Sub-category: More specific classification (e.g., "EV Battery Partnership" under Technology)
- Sentiment: Based on business/market impact, not emotional tone
  - Positive: Favorable for company/industry growth, profitability
  - Negative: Unfavorable implications, risks, losses
  - Neutral: Informational, no clear positive/negative impact
  - Mixed: Contains both positive and negative elements
- Confidence: 0.9+ for clear articles, 0.7-0.9 for mixed signals, below 0.7 for ambiguous

IMPACT ASSESSMENT:
- Severity scale:
  - 1-3: Minor news, limited market impact
  - 4-6: Notable development, moderate industry relevance
  - 7-8: Significant news, material market implications
  - 9-10: Critical/breaking news, immediate widespread impact
- Time horizon:
  - Immediate: Impact within days
  - Near-term: 1-3 months
  - Medium-term: 3-12 months
  - Long-term: 1+ years
- Stock tickers: Use standard symbols (TSLA, F, GM, TM, HMC, STLA, RIVN, etc.)

IMPACT FLAGS:
- Set TRUE only if explicitly supported by article content
- Default to FALSE if uncertain
- breaking_news: Requires immediate attention, time-sensitive
- recall_related: Specific recall mentioned or implied
- affects_guidance: Mentions earnings, forecasts, targets, or guidance changes
- supply_chain_disruption: Impacts parts, chips, batteries, logistics
- regulatory_action: Government action, NHTSA, EPA, tariffs, regulations
- labor_action: Union activity, strikes, layoffs, hiring freezes
- merger_acquisition: M&A activity, partnerships, joint ventures, divestitures

EXECUTIVE SUMMARY:
- Headline: Factual, concise, no sensationalism (max 100 characters)
- Situation: What happened? Who is involved? When? (2-3 sentences)
- Implications: Why does this matter? What are the consequences? (2-3 sentences)
- Key figures: Extract specific numbers (e.g., "$2.5B investment", "15% decline", "50,000 units")
- Action items: What should analysts/investors monitor or do next?

RELATED CONTEXT:
- Historical precedent: Reference similar past events and their outcomes if applicable
- Competitive dynamics: How does this affect market positioning among competitors?
- If no relevant context, state "No directly comparable precedent"

Maintain objectivity. No speculation beyond reasonable inference from the text.
Only extract information explicitly stated or clearly implied in the article."""


EVENT_DASHBOARD_PROMPT = """Analyze this industry event. Provide professional, actionable intelligence.
Use precise terminology. No emojis. No promotional language. Be factual.

EVENT:
Name: {title}
Type: {event_type}
Date: {date_start} to {date_end}
Location: {location}
Venue: {venue}
Description: {description}

=== SECTION INSTRUCTIONS ===

KEY ENTITIES:
- Manufacturers: Expected OEM participants based on event type and history
- Suppliers: Expected supplier participants
- Organizations: Hosting organizations, sponsors, industry groups
- Regions: Geographic focus of the event

EVENT PROFILE:
- Event type options: Auto Show | Conference | Earnings Call | Investor Day | Product Reveal | Regulatory Hearing | Trade Fair | Industry Summit | Press Event | Other
- Tier classification:
  - Tier 1 (Major): Global significance (CES, Detroit Auto Show, Geneva Motor Show, IAA)
  - Tier 2 (Regional): Regional or company-specific importance
  - Tier 3 (Niche): Specialized or limited scope events
- Primary audience: Investors | Media | Dealers | Suppliers | Consumers | Regulators | Industry Professionals

STRATEGIC IMPORTANCE:
- Relevance score: 0.8-1.0 for must-watch, 0.5-0.8 for notable, below 0.5 for minor
- Importance level: Based on potential for market-moving announcements
  - Critical: Major product reveals, significant strategic announcements expected
  - High: Important industry updates, notable participants
  - Medium: Standard industry event, routine coverage
  - Low: Niche event, limited broader relevance
- Rationale: 2-3 sentences explaining WHY this event matters (or doesn't)

EXPECTED COVERAGE:
- Participants: List confirmed or historically participating companies
- Key topics: What themes, announcements, or discussions are expected?
- Potential market movers: Specific announcements that could affect stock prices or industry direction

ANALYST BRIEF:
- Summary: Factual 3-4 sentence overview suitable for executive briefing
- Watch items: Specific things analysts should monitor during/after the event
- Historical context: Notable past announcements from this event series if applicable
- Attendance recommendation:
  - Recommended: High strategic value, likely market-moving content
  - Optional: Useful but not essential
  - Monitor Remotely: Follow coverage without attending
  - Skip: Low relevance for current priorities

LOGISTICS:
- Registration status: Open | Closed | Invite Only | Unknown
- Media coverage: Live Streamed | Press Release Expected | Limited Coverage | Unknown
- Key dates: Pre-event briefings, embargo lifts, related deadlines
- If information unavailable, state "Information unavailable"

Focus on actionable intelligence. If data is insufficient, indicate clearly rather than speculating."""


# ============================================================================
# DASHBOARD TOOLS CLASS
# ============================================================================

class DashboardTools:
    """On-demand dashboard generation using Claude LLM"""
    
    def __init__(self):
        """Initialize Claude client with Instructor for structured outputs"""
        self.api_key = os.getenv('ANTHROPIC_API_KEY')
        self.model = os.getenv('CLAUDE_MODEL', 'claude-sonnet-4-20250514')
        
        if not self.api_key:
            logger.warning("ANTHROPIC_API_KEY not set - dashboard generation will fail")
            self.client = None
        else:
            self.client = instructor.from_anthropic(
                anthropic.Anthropic(api_key=self.api_key)
            )
            logger.info(f"DashboardTools initialized with model: {self.model}")
    
    def generate_article_dashboard(self, article_id: int) -> Dict[str, Any]:
        """Generate dashboard for a specific article"""
        if not self.client:
            raise ValueError("Claude client not initialized - check ANTHROPIC_API_KEY")
        
        logger.info(f"Generating dashboard for article_id={article_id}")
        
        sql = """
        SELECT 
            ARTICLE_ID, TITLE, URL, PUBLISHED_DATE, SOURCE, AUTHOR, SUMMARY
        FROM NEWS_ARTICLES
        WHERE ARTICLE_ID = %s
        """
        results = execute_query(sql, (article_id,))
        
        if not results:
            raise ValueError(f"Article {article_id} not found")
        
        row = results[0]
        article_data = {
            'article_id': row.get('ARTICLE_ID'),
            'title': row.get('TITLE', 'No title'),
            'url': row.get('URL', ''),
            'published_date': str(row.get('PUBLISHED_DATE')) if row.get('PUBLISHED_DATE') else 'Unknown',
            'source': row.get('SOURCE', 'Unknown'),
            'author': row.get('AUTHOR', 'Unknown'),
            'summary': row.get('SUMMARY', 'No content available')
        }
        
        prompt = ARTICLE_DASHBOARD_PROMPT.format(
            title=article_data['title'],
            source=article_data['source'],
            published_date=article_data['published_date'],
            author=article_data['author'],
            summary=article_data['summary']
        )
        
        try:
            dashboard = self.client.messages.create(
                model=self.model,
                max_tokens=2500,
                response_model=ArticleDashboard,
                messages=[{"role": "user", "content": prompt}]
            )
            
            logger.info(f"Dashboard generated for article_id={article_id}")
            
            return {
                "article_id": article_data['article_id'],
                "title": article_data['title'],
                "url": article_data['url'],
                "published_date": article_data['published_date'],
                "source": article_data['source'],
                "author": article_data['author'],
                "key_entities": dashboard.key_entities.model_dump(),
                "classification": dashboard.classification.model_dump(),
                "impact_assessment": dashboard.impact_assessment.model_dump(),
                "impact_flags": dashboard.impact_flags.model_dump(),
                "executive_summary": dashboard.executive_summary.model_dump(),
                "related_context": dashboard.related_context.model_dump()
            }
            
        except Exception as e:
            logger.error(f"Error generating dashboard: {e}")
            return self._create_fallback_article_dashboard(article_data, str(e))
    
    def generate_event_dashboard(self, event_id: int) -> Dict[str, Any]:
        """Generate dashboard for a specific event"""
        if not self.client:
            raise ValueError("Claude client not initialized - check ANTHROPIC_API_KEY")
        
        logger.info(f"Generating dashboard for event_id={event_id}")
        
        sql = """
        SELECT 
            EVENT_ID, TITLE, URL, EVENT_TYPE, DESCRIPTION,
            DATE_START, DATE_END, LOCATION, VENUE
        FROM AUTOMOTIVE_EVENTS
        WHERE EVENT_ID = %s
        """
        results = execute_query(sql, (event_id,))
        
        if not results:
            raise ValueError(f"Event {event_id} not found")
        
        row = results[0]
        event_data = {
            'event_id': row.get('EVENT_ID'),
            'title': row.get('TITLE', 'No title'),
            'url': row.get('URL', ''),
            'event_type': row.get('EVENT_TYPE', 'Other'),
            'description': row.get('DESCRIPTION', 'No description'),
            'date_start': str(row.get('DATE_START')) if row.get('DATE_START') else 'TBD',
            'date_end': str(row.get('DATE_END')) if row.get('DATE_END') else 'TBD',
            'location': row.get('LOCATION', 'TBD'),
            'venue': row.get('VENUE', '')
        }
        
        prompt = EVENT_DASHBOARD_PROMPT.format(
            title=event_data['title'],
            event_type=event_data['event_type'],
            date_start=event_data['date_start'],
            date_end=event_data['date_end'],
            location=event_data['location'],
            venue=event_data['venue'],
            description=event_data['description']
        )
        
        try:
            dashboard = self.client.messages.create(
                model=self.model,
                max_tokens=2500,
                response_model=EventDashboard,
                messages=[{"role": "user", "content": prompt}]
            )
            
            logger.info(f"Dashboard generated for event_id={event_id}")
            
            return {
                "event_id": event_data['event_id'],
                "title": event_data['title'],
                "url": event_data['url'],
                "event_type": event_data['event_type'],
                "date_start": event_data['date_start'],
                "date_end": event_data['date_end'],
                "location": event_data['location'],
                "venue": event_data['venue'],
                "key_entities": dashboard.key_entities.model_dump(),
                "event_profile": dashboard.event_profile.model_dump(),
                "strategic_importance": dashboard.strategic_importance.model_dump(),
                "expected_coverage": dashboard.expected_coverage.model_dump(),
                "analyst_brief": dashboard.analyst_brief.model_dump(),
                "logistics": dashboard.logistics.model_dump()
            }
            
        except Exception as e:
            logger.error(f"Error generating event dashboard: {e}")
            return self._create_fallback_event_dashboard(event_data, str(e))
    
    def _create_fallback_article_dashboard(self, article_data: Dict, error: str) -> Dict[str, Any]:
        """Create fallback dashboard when LLM fails"""
        return {
            "article_id": article_data['article_id'],
            "title": article_data['title'],
            "url": article_data['url'],
            "published_date": article_data['published_date'],
            "source": article_data['source'],
            "author": article_data['author'],
            "key_entities": KeyEntities().model_dump(),
            "classification": Classification().model_dump(),
            "impact_assessment": ImpactAssessment().model_dump(),
            "impact_flags": ImpactFlags().model_dump(),
            "executive_summary": {
                "headline": article_data['title'][:100],
                "situation": article_data['summary'] or "No summary available",
                "implications": "",
                "key_figures": [],
                "action_items": []
            },
            "related_context": RelatedContext().model_dump(),
            "error": error
        }
    
    def _create_fallback_event_dashboard(self, event_data: Dict, error: str) -> Dict[str, Any]:
        """Create fallback dashboard when LLM fails"""
        return {
            "event_id": event_data['event_id'],
            "title": event_data['title'],
            "url": event_data['url'],
            "event_type": event_data['event_type'],
            "date_start": event_data['date_start'],
            "date_end": event_data['date_end'],
            "location": event_data['location'],
            "venue": event_data['venue'],
            "key_entities": KeyEntities().model_dump(),
            "event_profile": EventProfile().model_dump(),
            "strategic_importance": StrategicImportance().model_dump(),
            "expected_coverage": ExpectedCoverage().model_dump(),
            "analyst_brief": {
                "summary": event_data['description'] or "No description available",
                "watch_items": [],
                "historical_context": "",
                "attendance_recommendation": "Unknown"
            },
            "logistics": EventLogistics().model_dump(),
            "error": error
        }