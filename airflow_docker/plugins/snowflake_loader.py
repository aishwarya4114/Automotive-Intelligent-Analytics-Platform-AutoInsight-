"""
AutoInsight Tab 2: Snowflake Data Loader
Inserts scraped news and events into Snowflake database
"""

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import json
import os
import logging
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Load scraped data into Snowflake"""
    
    def __init__(self):
        """Initialize Snowflake connection using environment variables"""
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'AUTOMOTIVE_AI'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        }
        
        self.conn = None
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            logger.info("Connecting to Snowflake...")
            self.conn = snowflake.connector.connect(**self.connection_params)
            logger.info(f"✓ Connected to {self.connection_params['database']}.{self.connection_params['schema']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("✓ Disconnected from Snowflake")
    
    def _convert_array_to_snowflake(self, arr: List) -> str:
        """Convert Python list to Snowflake ARRAY format"""
        if not arr:
            return "ARRAY_CONSTRUCT()"
        # Escape single quotes and format as array
        escaped = [str(item).replace("'", "''") for item in arr]
        return f"ARRAY_CONSTRUCT({', '.join([repr(x) for x in escaped])})"
    
    def insert_articles(self, articles: List[Dict]) -> int:
        """
        Insert news articles into Snowflake
        Uses MERGE to avoid duplicates (based on URL)
        """
        if not articles:
            logger.warning("No articles to insert")
            return 0
        
        cursor = self.conn.cursor()
        inserted_count = 0
        
        try:
            for article in articles:
                try:
                    # Convert arrays to Snowflake format
                    manufacturers = self._convert_array_to_snowflake(article.get('mentioned_manufacturers', []))
                    models = self._convert_array_to_snowflake(article.get('mentioned_models', []))
                    executives = self._convert_array_to_snowflake(article.get('mentioned_executives', []))
                    locations = self._convert_array_to_snowflake(article.get('mentioned_locations', []))
                    
                    # Parse date
                    pub_date = article.get('published_date', '')
                    if pub_date:
                        try:
                            pub_date = f"'{pub_date}'"
                        except:
                            pub_date = 'NULL'
                    else:
                        pub_date = 'NULL'
                    
                    # Build MERGE statement (upsert - insert or update if exists)
                    sql = f"""
                    MERGE INTO NEWS_ARTICLES AS target
                    USING (
                        SELECT 
                            '{article.get('title', '').replace("'", "''")}' AS title,
                            '{article.get('url', '').replace("'", "''")}' AS url,
                            {pub_date} AS published_date,
                            '{article.get('source', 'unknown')}' AS source,
                            {f"'{article.get('author', '').replace(chr(39), chr(39)+chr(39))}'" if article.get('author') else 'NULL'} AS author,
                            '{article.get('summary', '').replace("'", "''")}' AS summary,
                            '{article.get('category', 'general')}' AS category,
                            '{article.get('sentiment', 'neutral')}' AS sentiment,
                            {article.get('importance_score', 5)} AS importance_score,
                            {manufacturers} AS mentioned_manufacturers,
                            {models} AS mentioned_models,
                            {executives} AS mentioned_executives,
                            {locations} AS mentioned_locations,
                            {str(article.get('is_breaking_news', False)).upper()} AS is_breaking_news,
                            {str(article.get('is_recall_related', False)).upper()} AS is_recall_related,
                            {str(article.get('is_safety_critical', False)).upper()} AS is_safety_critical,
                            CURRENT_TIMESTAMP() AS scraped_at
                    ) AS source
                    ON target.url = source.url
                    WHEN MATCHED THEN UPDATE SET
                        title = source.title,
                        published_date = source.published_date,
                        summary = source.summary,
                        category = source.category,
                        sentiment = source.sentiment,
                        importance_score = source.importance_score,
                        mentioned_manufacturers = source.mentioned_manufacturers,
                        mentioned_models = source.mentioned_models,
                        mentioned_executives = source.mentioned_executives,
                        mentioned_locations = source.mentioned_locations,
                        is_breaking_news = source.is_breaking_news,
                        is_recall_related = source.is_recall_related,
                        is_safety_critical = source.is_safety_critical,
                        scraped_at = source.scraped_at
                    WHEN NOT MATCHED THEN INSERT (
                        title, url, published_date, source, author, summary,
                        category, sentiment, importance_score,
                        mentioned_manufacturers, mentioned_models, mentioned_executives, mentioned_locations,
                        is_breaking_news, is_recall_related, is_safety_critical, scraped_at
                    ) VALUES (
                        source.title, source.url, source.published_date, source.source, source.author, source.summary,
                        source.category, source.sentiment, source.importance_score,
                        source.mentioned_manufacturers, source.mentioned_models, source.mentioned_executives, source.mentioned_locations,
                        source.is_breaking_news, source.is_recall_related, source.is_safety_critical, source.scraped_at
                    );
                    """
                    
                    cursor.execute(sql)
                    inserted_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to insert article '{article.get('title', '')[:50]}': {e}")
                    continue
            
            self.conn.commit()
            logger.info(f"✓ Inserted/Updated {inserted_count} articles in Snowflake")
            
        except Exception as e:
            logger.error(f"Error inserting articles: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
        
        return inserted_count
    
    def insert_events(self, events: List[Dict]) -> int:
        """
        Insert events into Snowflake
        Uses MERGE to avoid duplicates
        """
        if not events:
            logger.warning("No events to insert")
            return 0
        
        cursor = self.conn.cursor()
        inserted_count = 0
        
        try:
            for event in events:
                try:
                    # Convert arrays
                    topics = self._convert_array_to_snowflake(event.get('key_topics', []))
                    manufacturers = self._convert_array_to_snowflake(event.get('participating_manufacturers', []))
                    
                    # Parse dates
                    date_start = f"'{event.get('date_start')}'" if event.get('date_start') else 'NULL'
                    date_end = f"'{event.get('date_end')}'" if event.get('date_end') else 'NULL'
                    
                    sql = f"""
                    MERGE INTO AUTOMOTIVE_EVENTS AS target
                    USING (
                        SELECT
                            '{event.get('title', '').replace("'", "''")}' AS title,
                            '{event.get('url', '').replace("'", "''")}' AS url,
                            '{event.get('event_type', 'event')}' AS event_type,
                            '{event.get('description', '').replace("'", "''")}' AS description,
                            {date_start} AS date_start,
                            {date_end} AS date_end,
                            {f"'{event.get('date_raw', '').replace(chr(39), chr(39)+chr(39))}'" if event.get('date_raw') else 'NULL'} AS date_raw,
                            {f"'{event.get('location', '').replace(chr(39), chr(39)+chr(39))}'" if event.get('location') else 'NULL'} AS location,
                            {f"'{event.get('venue', '').replace(chr(39), chr(39)+chr(39))}'" if event.get('venue') else 'NULL'} AS venue,
                            {str(event.get('is_virtual', False)).upper()} AS is_virtual,
                            {topics} AS key_topics,
                            {manufacturers} AS participating_manufacturers,
                            {event.get('relevance_score', 0.5)} AS relevance_score,
                            '{event.get('strategic_importance', 'medium')}' AS strategic_importance,
                            '{event.get('source', 'unknown')}' AS source,
                            CURRENT_TIMESTAMP() AS scraped_at
                    ) AS source
                    ON target.title = source.title AND 
                       (target.date_start = source.date_start OR (target.date_start IS NULL AND source.date_start IS NULL))
                    WHEN MATCHED THEN UPDATE SET
                        url = source.url,
                        event_type = source.event_type,
                        description = source.description,
                        date_end = source.date_end,
                        date_raw = source.date_raw,
                        location = source.location,
                        venue = source.venue,
                        is_virtual = source.is_virtual,
                        key_topics = source.key_topics,
                        participating_manufacturers = source.participating_manufacturers,
                        relevance_score = source.relevance_score,
                        strategic_importance = source.strategic_importance,
                        scraped_at = source.scraped_at
                    WHEN NOT MATCHED THEN INSERT (
                        title, url, event_type, description,
                        date_start, date_end, date_raw, location, venue, is_virtual,
                        key_topics, participating_manufacturers,
                        relevance_score, strategic_importance, source, scraped_at
                    ) VALUES (
                        source.title, source.url, source.event_type, source.description,
                        source.date_start, source.date_end, source.date_raw, source.location, source.venue, source.is_virtual,
                        source.key_topics, source.participating_manufacturers,
                        source.relevance_score, source.strategic_importance, source.source, source.scraped_at
                    );
                    """
                    
                    cursor.execute(sql)
                    inserted_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to insert event '{event.get('title', '')[:50]}': {e}")
                    continue
            
            self.conn.commit()
            logger.info(f"✓ Inserted/Updated {inserted_count} events in Snowflake")
            
        except Exception as e:
            logger.error(f"Error inserting events: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
        
        return inserted_count
    
    def get_article_count(self) -> int:
        """Get total number of articles in database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM NEWS_ARTICLES")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    
    def get_event_count(self) -> int:
        """Get total number of events in database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM AUTOMOTIVE_EVENTS")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    
    def get_latest_articles(self, limit: int = 5) -> List[Dict]:
        """Get latest articles from database"""
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT title, url, published_date, category, sentiment, importance_score
            FROM NEWS_ARTICLES 
            ORDER BY published_date DESC 
            LIMIT {limit}
        """)
        
        columns = ['title', 'url', 'published_date', 'category', 'sentiment', 'importance_score']
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return results


def load_json_to_snowflake(articles_file: str = None, events_file: str = None):
    """
    Load JSON files into Snowflake
    This is a convenience function to load existing scraped data
    """
    loader = SnowflakeLoader()
    
    if not loader.connect():
        return
    
    try:
        # Load articles
        if articles_file and os.path.exists(articles_file):
            with open(articles_file, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            logger.info(f"Loading {len(articles)} articles from {articles_file}")
            loader.insert_articles(articles)
        
        # Load events
        if events_file and os.path.exists(events_file):
            with open(events_file, 'r', encoding='utf-8') as f:
                events = json.load(f)
            logger.info(f"Loading {len(events)} events from {events_file}")
            loader.insert_events(events)
        
        # Show summary
        print("\n" + "=" * 60)
        print("📊 SNOWFLAKE DATABASE SUMMARY")
        print("=" * 60)
        print(f"   Total articles: {loader.get_article_count()}")
        print(f"   Total events:   {loader.get_event_count()}")
        
        print("\n📰 Latest Articles:")
        for article in loader.get_latest_articles(5):
            print(f"   • {article['title'][:50]}...")
            print(f"     {article['category']} | {article['sentiment']} | {article['published_date']}")
        
    finally:
        loader.disconnect()


if __name__ == "__main__":
    # Load existing JSON files into Snowflake
    load_json_to_snowflake(
        articles_file='autonews_articles_latest.json',
        events_file='autonews_events_latest.json'
    )