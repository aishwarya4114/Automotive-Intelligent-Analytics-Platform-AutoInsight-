"""
AutoInsight Tab 2: Dynamic Web Scraper
- NO hardcoded data
- All news and events scraped dynamically from real sources
- Entity extraction using NLP patterns (not hardcoded lists)
"""

from playwright.sync_api import sync_playwright
from datetime import datetime
import json
import logging
import re
from typing import List, Dict, Optional
import requests
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DynamicAutoNewsScraper:
    """
    Fully dynamic scraper - no hardcoded data.
    Extracts entities using NLP patterns, not predefined lists.
    """
    
    def __init__(self, headless: bool = False, slow_mo: int = 500):
        """
        Initialize scraper.
        
        Args:
            headless: If False, browser window will be visible (default: False for debugging)
            slow_mo: Milliseconds to slow down each action (default: 500ms for visibility)
        """
        self.headless = headless
        self.slow_mo = slow_mo  # Slow motion in milliseconds
        self.base_url = "https://www.autonews.com"
    
    # =========================================================================
    # DYNAMIC ENTITY EXTRACTION (No hardcoded lists)
    # =========================================================================
    
    def extract_manufacturers_dynamic(self, text: str) -> List[str]:
        """
        Extract manufacturer names using NLP patterns, NOT hardcoded lists.
        Looks for:
        - "X Motors", "X Automotive", "X Auto"
        - "X announced", "X said", "X reported", "X unveiled"
        - "CEO of X", "at X", "from X" (where X is capitalized)
        - Known automotive context patterns
        """
        manufacturers = set()
        
        # Pattern 1: "X Motors", "X Automotive", "X Auto Group"
        pattern1 = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:Motors?|Automotive|Auto(?:\s+Group)?)\b'
        matches = re.findall(pattern1, text)
        manufacturers.update(matches)
        
        # Pattern 2: "X announced/said/reported/unveiled/recalled"
        pattern2 = r'\b([A-Z][A-Za-z]+)\s+(?:announced|said|reported|unveiled|recalled|launched|revealed|plans|will|is|has)\b'
        matches = re.findall(pattern2, text)
        # Filter out common non-manufacturer words
        stop_words = {'The', 'This', 'That', 'These', 'They', 'What', 'When', 'Where', 
                      'How', 'Why', 'Who', 'Which', 'Many', 'Some', 'Most', 'New',
                      'First', 'Last', 'Next', 'Other', 'Each', 'Every', 'Both',
                      'Reuters', 'Bloomberg', 'Source', 'Report', 'Study', 'Data'}
        manufacturers.update(m for m in matches if m not in stop_words)
        
        # Pattern 3: "CEO/chief/president of X"
        pattern3 = r'(?:CEO|chief|president|chairman|head)\s+(?:of\s+)?([A-Z][A-Za-z]+)'
        matches = re.findall(pattern3, text, re.IGNORECASE)
        manufacturers.update(m for m in matches if m not in stop_words)
        
        # Pattern 4: "at X" or "from X" in automotive context
        pattern4 = r'(?:at|from|by)\s+([A-Z][A-Za-z]+)(?:\s+(?:dealership|plant|factory|headquarters))?'
        matches = re.findall(pattern4, text)
        manufacturers.update(m for m in matches if m not in stop_words and len(m) > 2)
        
        # Pattern 5: Possessive - "X's new vehicle", "X's CEO"
        pattern5 = r"\b([A-Z][A-Za-z]+)'s\s+(?:new|latest|upcoming|CEO|chief|vehicle|car|truck|SUV|EV)"
        matches = re.findall(pattern5, text)
        manufacturers.update(m for m in matches if m not in stop_words)
        
        # Pattern 6: Two-letter uppercase (common for auto: GM, VW, BYD)
        pattern6 = r'\b([A-Z]{2,3})\b'
        matches = re.findall(pattern6, text)
        # Only include if appears in automotive context
        auto_context = r'\b(?:vehicle|car|truck|SUV|EV|electric|sales|recall|plant|dealer)'
        if re.search(auto_context, text, re.IGNORECASE):
            manufacturers.update(m for m in matches if len(m) <= 3 and m not in {'CEO', 'CFO', 'COO', 'USA', 'THE', 'AND', 'FOR'})
        
        return list(manufacturers)[:10]  # Limit to top 10
    
    def extract_models_dynamic(self, text: str) -> List[str]:
        """
        Extract vehicle model names using patterns, NOT hardcoded lists.
        """
        models = set()
        
        # Pattern 1: Letter(s) + Number (F-150, X5, Model 3, ID.4)
        pattern1 = r'\b([A-Z]{1,3}[-.]?\d{1,4}[A-Za-z]*)\b'
        models.update(re.findall(pattern1, text))
        
        # Pattern 2: "Model X/Y/S/3"
        pattern2 = r'\b(Model\s+[A-Z0-9])\b'
        models.update(re.findall(pattern2, text))
        
        # Pattern 3: Word + Number (Mustang Mach-E, Ioniq 5)
        pattern3 = r'\b([A-Z][a-z]+\s+\d+[A-Za-z]*)\b'
        models.update(re.findall(pattern3, text))
        
        # Pattern 4: Hyphenated models (Mach-E, e-tron)
        pattern4 = r'\b([A-Za-z]+-[A-Za-z0-9]+)\b'
        matches = re.findall(pattern4, text)
        # Filter out common non-model hyphenated words
        models.update(m for m in matches if not m.lower().startswith(('self-', 'all-', 'full-', 'high-', 'low-', 'non-', 'pre-', 'co-')))
        
        return list(models)[:5]
    
    def extract_executives_dynamic(self, text: str) -> List[str]:
        """
        Extract executive names using patterns.
        """
        executives = set()
        
        # Pattern 1: "CEO/Chief X Name"
        pattern1 = r'(?:CEO|Chief\s+\w+\s+Officer|President|Chairman)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
        executives.update(re.findall(pattern1, text))
        
        # Pattern 2: "Name, CEO/title"
        pattern2 = r'([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(?:the\s+)?(?:CEO|chief|president|chairman)'
        executives.update(re.findall(pattern2, text, re.IGNORECASE))
        
        # Pattern 3: "Name said/announced/told"
        pattern3 = r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:said|announced|told|added|noted)'
        matches = re.findall(pattern3, text)
        executives.update(m for m in matches if not m.split()[0] in {'The', 'This', 'That', 'They'})
        
        return list(executives)[:3]
    
    def extract_locations_dynamic(self, text: str) -> List[str]:
        """
        Extract location names using patterns.
        """
        locations = set()
        
        # Pattern 1: "in City" or "in City, State/Country"
        pattern1 = r'in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s+[A-Z]{2,})?)'
        locations.update(re.findall(pattern1, text))
        
        # Pattern 2: "City-based"
        pattern2 = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)-based'
        locations.update(re.findall(pattern2, text))
        
        # Pattern 3: "at/from City"
        pattern3 = r'(?:at|from)\s+([A-Z][a-z]+(?:,\s+[A-Z]{2})?)'
        locations.update(re.findall(pattern3, text))
        
        return list(locations)[:5]
    
    # =========================================================================
    # DYNAMIC CLASSIFICATION (Pattern-based, not hardcoded)
    # =========================================================================
    
    def detect_category_dynamic(self, text: str) -> str:
        """
        Detect category based on keyword patterns.
        These are classification RULES, not hardcoded data.
        """
        text_lower = text.lower()
        
        # Priority-ordered pattern matching
        patterns = [
            ('recalls', ['recall', 'nhtsa', 'safety defect', 'investigation into']),
            ('executive', ['ceo', 'chief executive', 'appoints', 'hires', 'leadership change', 'steps down']),
            ('financial', ['earnings', 'quarterly results', 'profit', 'revenue', 'stock', 'shares', 'investor']),
            ('ev', ['electric vehicle', ' ev ', 'battery', 'charging', 'hybrid', 'electrification', 'zero emission']),
            ('sales', ['sales', 'deliveries', 'sold', 'market share', 'inventory', 'units sold']),
            ('manufacturing', ['plant', 'factory', 'production', 'assembly', 'supplier', 'manufacturing']),
            ('technology', ['software', 'autonomous', 'self-driving', 'artificial intelligence', ' ai ', 'connected']),
        ]
        
        for category, keywords in patterns:
            if any(kw in text_lower for kw in keywords):
                return category
        
        return 'general'
    
    def detect_sentiment_dynamic(self, text: str) -> str:
        """
        Simple sentiment detection based on linguistic patterns.
        """
        text_lower = text.lower()
        
        # Positive indicators (weighted)
        positive_patterns = [
            (3, ['record high', 'breakthrough', 'surge', 'soar', 'boom', 'best ever']),
            (2, ['growth', 'success', 'award', 'expansion', 'partnership', 'profit', 'gain', 'increase']),
            (1, ['new', 'launch', 'innovative', 'improved', 'positive']),
        ]
        
        # Negative indicators (weighted)
        negative_patterns = [
            (3, ['recall', 'death', 'fatal', 'crash', 'fire', 'lawsuit', 'bankruptcy', 'fraud']),
            (2, ['decline', 'drop', 'fall', 'loss', 'layoff', 'cut', 'slump', 'crisis', 'warning']),
            (1, ['issue', 'problem', 'concern', 'delay', 'struggle']),
        ]
        
        pos_score = sum(weight for weight, words in positive_patterns for w in words if w in text_lower)
        neg_score = sum(weight for weight, words in negative_patterns for w in words if w in text_lower)
        
        if neg_score > pos_score + 1:
            return 'negative'
        elif pos_score > neg_score + 1:
            return 'positive'
        return 'neutral'
    
    def calculate_importance_dynamic(self, text: str, category: str, is_breaking: bool = False) -> int:
        """Calculate importance score based on content analysis."""
        score = 5
        text_lower = text.lower()
        
        # Boost for critical categories
        if category in ['recalls', 'executive', 'financial']:
            score += 2
        
        # Boost for urgency keywords
        if any(kw in text_lower for kw in ['breaking', 'urgent', 'major', 'critical', 'exclusive']):
            score += 2
        
        # Boost for high-impact topics
        if any(kw in text_lower for kw in ['bankruptcy', 'merger', 'acquisition', 'death', 'injury']):
            score += 1
        
        if is_breaking:
            score += 1
        
        return min(score, 10)
    
    # =========================================================================
    # NEWS SCRAPING (Dynamic from RSS)
    # =========================================================================
    
    def scrape_news_rss(self, max_articles: int = 100) -> List[Dict]:
        """Scrape news dynamically from RSS feed."""
        logger.info("Fetching news from RSS feed...")
        articles = []
        
        rss_url = 'https://www.autonews.com/arc/outboundfeeds/rss/?outputType=xml'
        
        try:
            response = requests.get(
                rss_url,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"RSS returned status {response.status_code}")
                return articles
            
            root = ET.fromstring(response.content)
            
            for item in root.findall('.//item')[:max_articles]:
                try:
                    # Extract raw data from RSS
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    desc_elem = item.find('description')
                    date_elem = item.find('pubDate')
                    creator_elem = item.find('{http://purl.org/dc/elements/1.1/}creator')
                    
                    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
                    url = link_elem.text.strip() if link_elem is not None and link_elem.text else ""
                    summary = ""
                    if desc_elem is not None and desc_elem.text:
                        summary = re.sub(r'<[^>]+>', '', desc_elem.text).strip()
                    author = creator_elem.text.strip() if creator_elem is not None and creator_elem.text else None
                    
                    # Parse date
                    published_date = datetime.now().isoformat()
                    if date_elem is not None and date_elem.text:
                        try:
                            dt = datetime.strptime(date_elem.text[:25], '%a, %d %b %Y %H:%M:%S')
                            published_date = dt.isoformat()
                        except:
                            pass
                    
                    if not title or not url:
                        continue
                    
                    # Combine text for analysis
                    full_text = f"{title} {summary}"
                    
                    # Dynamic entity extraction
                    manufacturers = self.extract_manufacturers_dynamic(full_text)
                    models = self.extract_models_dynamic(full_text)
                    executives = self.extract_executives_dynamic(full_text)
                    locations = self.extract_locations_dynamic(full_text)
                    
                    # Dynamic classification
                    category = self.detect_category_dynamic(full_text)
                    sentiment = self.detect_sentiment_dynamic(full_text)
                    is_breaking = any(kw in full_text.lower() for kw in ['breaking', 'just in', 'developing'])
                    importance = self.calculate_importance_dynamic(full_text, category, is_breaking)
                    
                    article = {
                        'title': title,
                        'url': url,
                        'published_date': published_date,
                        'source': 'autonews_rss',
                        'author': author,
                        'summary': summary[:500],
                        'category': category,
                        'sentiment': sentiment,
                        'importance_score': importance,
                        'mentioned_manufacturers': manufacturers,
                        'mentioned_models': models,
                        'mentioned_executives': executives,
                        'mentioned_locations': locations,
                        'is_breaking_news': is_breaking,
                        'is_recall_related': category == 'recalls',
                        'is_safety_critical': any(kw in full_text.lower() for kw in ['death', 'injury', 'fire', 'crash']),
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.debug(f"Error parsing article: {e}")
                    continue
            
            logger.info(f"✓ Scraped {len(articles)} articles from RSS")
            
        except Exception as e:
            logger.error(f"Error fetching RSS: {e}")
        
        return articles
    
    # =========================================================================
    # EVENTS SCRAPING (Fully Dynamic from Web Page)
    # =========================================================================
    
    def parse_event_date(self, date_text: str) -> tuple:
        """
        Parse event date from various formats found on the page.
        Returns (start_date, end_date) as ISO strings or None.
        """
        if not date_text:
            return None, None
        
        date_text = date_text.strip()
        
        # Month abbreviation expansion
        month_map = {
            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'June': '06', 'July': '07', 'August': '08', 'September': '09',
            'October': '10', 'November': '11', 'December': '12'
        }
        
        # Clean prefixes
        date_text = re.sub(r'^(?:Media days?|Press days?)\s*', '', date_text, flags=re.IGNORECASE)
        
        # Pattern: "Month DD-DD, YYYY" (e.g., "Jan. 7-10, 2025")
        match = re.search(r'(\w+)\.?\s+(\d{1,2})\s*[-–]\s*(\d{1,2}),?\s*(\d{4})', date_text)
        if match:
            month_str, start_day, end_day, year = match.groups()
            month_key = month_str.replace('.', '')
            if month_key in month_map:
                month = month_map[month_key]
                start_date = f"{year}-{month}-{start_day.zfill(2)}"
                end_date = f"{year}-{month}-{end_day.zfill(2)}"
                return start_date, end_date
        
        # Pattern: "Month DD, YYYY" (single date)
        match = re.search(r'(\w+)\.?\s+(\d{1,2}),?\s*(\d{4})', date_text)
        if match:
            month_str, day, year = match.groups()
            month_key = month_str.replace('.', '')
            if month_key in month_map:
                month = month_map[month_key]
                start_date = f"{year}-{month}-{day.zfill(2)}"
                return start_date, None
        
        # Pattern: Just year "in 2026"
        match = re.search(r'\b(20\d{2})\b', date_text)
        if match:
            return f"{match.group(1)}-01-01", None
        
        return None, None
    
    def extract_location_from_text(self, text: str) -> Optional[str]:
        """Extract location from event text."""
        # Common patterns in event descriptions
        patterns = [
            r'in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s*[A-Z]{2})?)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:Convention Center|Expo|Center)',
            r'held\s+(?:in|at)\s+([A-Z][a-z]+(?:,\s*[A-Z]{2})?)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        
        return None
    
    def detect_event_type(self, text: str) -> str:
        """Detect event type from text content."""
        text_lower = text.lower()
        
        if any(kw in text_lower for kw in ['auto show', 'motor show', 'car show']):
            return 'auto_show'
        elif any(kw in text_lower for kw in ['conference', 'congress', 'summit']):
            return 'conference'
        elif any(kw in text_lower for kw in ['webinar', 'virtual', 'online']):
            return 'webinar'
        elif any(kw in text_lower for kw in ['expo', 'exhibition', 'trade show']):
            return 'trade_show'
        elif any(kw in text_lower for kw in ['forum', 'roundtable', 'workshop']):
            return 'forum'
        
        return 'event'
    
    def detect_event_topics(self, text: str) -> List[str]:
        """Detect event topics from text."""
        text_lower = text.lower()
        topics = []
        
        topic_patterns = [
            ('ev', ['electric', ' ev ', 'battery', 'charging']),
            ('autonomous', ['autonomous', 'self-driving', 'adas']),
            ('technology', ['technology', 'software', 'digital', 'ai ']),
            ('dealership', ['dealer', 'retail']),
            ('manufacturing', ['manufacturing', 'production', 'supply chain']),
            ('executive', ['leadership', 'women', 'executive']),
        ]
        
        for topic, keywords in topic_patterns:
            if any(kw in text_lower for kw in keywords):
                topics.append(topic)
        
        return topics
    
    def scrape_events_dynamic(self) -> List[Dict]:
        """
        Scrape events DYNAMICALLY from AutoNews.com/events page.
        No hardcoded events - everything extracted from the actual page.
        """
        logger.info("Scraping events from AutoNews.com/events...")
        events = []
        
        with sync_playwright() as p:
            # Launch browser - visible if headless=False, with slow motion for debugging
            browser = p.chromium.launch(
                headless=self.headless,
                slow_mo=self.slow_mo  # Slows down each action so you can see what's happening
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1280, 'height': 800}  # Set viewport size
            )
            page = context.new_page()
            
            try:
                print("    🌐 Opening AutoNews events page...")
                page.goto(f"{self.base_url}/events", timeout=60000, wait_until='domcontentloaded')
                print("    ⏳ Waiting for page to load...")
                page.wait_for_timeout(3000)
                
                # Get the page content
                page_content = page.content()
                
                # Find all event blocks - they have h3 with links followed by date info
                # Parse the HTML structure dynamically
                event_blocks = page.query_selector_all('h3')
                
                for block in event_blocks:
                    try:
                        # Get the link inside h3
                        link = block.query_selector('a')
                        if not link:
                            continue
                        
                        title = link.inner_text().strip()
                        href = link.get_attribute('href')
                        
                        if not title or len(title) < 3:
                            continue
                        
                        # Build full URL
                        url = href if href and href.startswith('http') else f"{self.base_url}{href}" if href else ""
                        
                        # Get parent container to extract more info
                        parent = block.query_selector('xpath=ancestor::*[contains(@class, "event") or contains(@class, "card") or self::div or self::article][1]')
                        if not parent:
                            parent = block.query_selector('xpath=..')
                        
                        # Get all text from the block
                        block_text = parent.inner_text() if parent else block.inner_text()
                        
                        # Extract date dynamically
                        start_date, end_date = self.parse_event_date(block_text)
                        
                        # Extract location dynamically
                        location = self.extract_location_from_text(block_text)
                        
                        # Detect event type
                        event_type = self.detect_event_type(f"{title} {block_text}")
                        
                        # Detect topics
                        topics = self.detect_event_topics(f"{title} {block_text}")
                        
                        # Extract any manufacturers mentioned
                        manufacturers = self.extract_manufacturers_dynamic(f"{title} {block_text}")
                        
                        # Check if virtual
                        is_virtual = any(kw in block_text.lower() for kw in ['virtual', 'webinar', 'online'])
                        
                        # Calculate relevance based on extracted data
                        relevance = 0.5
                        if start_date:
                            relevance += 0.2
                        if topics:
                            relevance += 0.1 * len(topics)
                        if location:
                            relevance += 0.1
                        relevance = min(relevance, 1.0)
                        
                        event = {
                            'title': title,
                            'url': url,
                            'event_type': event_type,
                            'description': '',  # Would need to fetch individual pages for full description
                            'date_start': start_date,
                            'date_end': end_date,
                            'date_raw': block_text[:100],  # Store raw text for debugging
                            'location': location,
                            'is_virtual': is_virtual,
                            'key_topics': topics,
                            'participating_manufacturers': manufacturers,
                            'relevance_score': round(relevance, 2),
                            'strategic_importance': 'high' if relevance > 0.7 else 'medium' if relevance > 0.5 else 'low',
                            'source': 'autonews_events',
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        events.append(event)
                        logger.info(f"  ✓ {title} ({start_date or 'date TBD'})")
                        
                    except Exception as e:
                        logger.debug(f"Error parsing event block: {e}")
                        continue
                
                logger.info(f"✓ Scraped {len(events)} events from AutoNews")
                
            except Exception as e:
                logger.error(f"Error scraping events: {e}")
            finally:
                browser.close()
        
        return events
    
    # =========================================================================
    # UTILITY FUNCTIONS
    # =========================================================================
    
    def save_to_json(self, data: List[Dict], filename: str):
        """Save data to JSON file."""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Saved {len(data)} items to {filename}")


def main():
    """Run the dynamic scraper."""
    print("\n" + "="*80)
    print("🚗 AUTOINSIGHT TAB 2: DYNAMIC SCRAPER")
    print("    (No hardcoding - all data scraped from real sources)")
    print("="*80 + "\n")
    
    # headless=False: You'll SEE the browser window
    # slow_mo=500: Each action waits 500ms so you can follow along
    scraper = DynamicAutoNewsScraper(headless=False, slow_mo=500)
    
    # =========================================================================
    # SCRAPE NEWS
    # =========================================================================
    print("📰 SCRAPING NEWS (from RSS feed)")
    print("-" * 80)
    
    articles = scraper.scrape_news_rss(max_articles=100)
    
    if articles:
        print(f"\n✓ Total articles scraped: {len(articles)}")
        
        # Dynamic stats
        print("\n📊 Category Distribution (auto-detected):")
        categories = {}
        for a in articles:
            categories[a['category']] = categories.get(a['category'], 0) + 1
        for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
            print(f"   {cat}: {count}")
        
        print("\n📊 Sentiment Distribution (auto-detected):")
        sentiments = {}
        for a in articles:
            sentiments[a['sentiment']] = sentiments.get(a['sentiment'], 0) + 1
        for sent, count in sentiments.items():
            print(f"   {sent}: {count}")
        
        print("\n📊 Top Manufacturers Mentioned (auto-extracted):")
        mfr_counts = {}
        for a in articles:
            for m in a['mentioned_manufacturers']:
                mfr_counts[m] = mfr_counts.get(m, 0) + 1
        for mfr, count in sorted(mfr_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"   {mfr}: {count}")
        
        # Sample articles
        print("\n📰 Sample Articles:")
        for i, a in enumerate(articles[:3], 1):
            print(f"\n   {i}. {a['title'][:60]}...")
            print(f"      Category: {a['category']} | Sentiment: {a['sentiment']}")
            if a['mentioned_manufacturers']:
                print(f"      Manufacturers: {', '.join(a['mentioned_manufacturers'][:3])}")
        
        scraper.save_to_json(articles, 'autonews_articles.json')
    else:
        print("⚠ No articles scraped")
    
    # =========================================================================
    # SCRAPE EVENTS
    # =========================================================================
    print("\n" + "="*80)
    print("📅 SCRAPING EVENTS (from AutoNews.com/events)")
    print("-" * 80)
    
    events = scraper.scrape_events_dynamic()
    
    if events:
        print(f"\n✓ Total events scraped: {len(events)}")
        
        # Sort by date
        events.sort(key=lambda e: e.get('date_start') or '9999-99-99')
        
        print("\n📊 Event Types (auto-detected):")
        types = {}
        for e in events:
            types[e['event_type']] = types.get(e['event_type'], 0) + 1
        for t, count in sorted(types.items(), key=lambda x: -x[1]):
            print(f"   {t}: {count}")
        
        print("\n📆 Upcoming Events:")
        for e in events[:10]:
            icon = '🔥' if e['strategic_importance'] == 'high' else '📅'
            date_str = e['date_start'] or 'TBD'
            print(f"   {icon} {date_str} - {e['title'][:45]}")
            if e['location']:
                print(f"      📍 {e['location']}")
        
        scraper.save_to_json(events, 'autonews_events.json')
    else:
        print("⚠ No events scraped")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "="*80)
    print("✅ SCRAPING COMPLETE!")
    print("="*80)
    print(f"\n📁 Files created:")
    print(f"   • autonews_articles.json ({len(articles)} news articles)")
    print(f"   • autonews_events.json   ({len(events)} events)")
    print("\n🎯 All data dynamically scraped - no hardcoding!")
    print()


if __name__ == "__main__":
    main()