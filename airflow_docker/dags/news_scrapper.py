"""
AutoInsight Daily News & Events Scraper DAG
Scrapes AutoNews.com → S3 → Snowflake
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
import logging
import json
import boto3

# ============================================================================
# CONFIGURATION & PATHS
# ============================================================================

PLUGINS_DIR = '/opt/airflow/plugins'
S3_BUCKET = os.getenv('S3_BUCKET_NAME', 'autoinsightfinal')

# S3 paths for news data
S3_NEWS_PREFIX = 'news/raw/'
S3_EVENTS_PREFIX = 'events/raw/'

if PLUGINS_DIR not in sys.path:
    sys.path.insert(0, PLUGINS_DIR)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'autoinsight',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def upload_to_s3(data, s3_key):
    """Upload JSON data to S3"""
    try:
        s3_client = boto3.client('s3')
        
        # Convert data to JSON string
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        
        logger.info(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        logger.info(f"✅ Successfully uploaded to S3")
        return f"s3://{S3_BUCKET}/{s3_key}"
        
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise


def download_from_s3(s3_key):
    """Download JSON data from S3"""
    try:
        s3_client = boto3.client('s3')
        
        logger.info(f"Downloading from s3://{S3_BUCKET}/{s3_key}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        
        data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"✅ Successfully downloaded {len(data)} items from S3")
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to download from S3: {e}")
        raise


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def scrape_news_articles(**context):
    """Task 1: Scrape news articles and upload to S3"""
    logger.info("="*60)
    logger.info("TASK: Scraping News Articles")
    
    try:
        from scrapper import DynamicAutoNewsScraper
        
        scraper = DynamicAutoNewsScraper(headless=True, slow_mo=0)
        
        logger.info("Fetching articles from AutoNews RSS feed...")
        articles = scraper.scrape_news_rss(max_articles=100)
        
        if not articles:
            logger.warning("No articles scraped!")
            context['task_instance'].xcom_push(key='articles_count', value=0)
            return "No articles scraped"
        
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{S3_NEWS_PREFIX}articles_{timestamp}.json"
        
        # Upload to S3
        s3_path = upload_to_s3(articles, s3_key)
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(key='articles_count', value=len(articles))
        context['task_instance'].xcom_push(key='articles_s3_key', value=s3_key)
        context['task_instance'].xcom_push(key='articles_s3_path', value=s3_path)
        
        logger.info(f"✅ Scraped {len(articles)} articles → {s3_path}")
        return f"Successfully scraped {len(articles)} articles"
        
    except Exception as e:
        logger.error(f"Error in scrape_news_articles: {e}")
        raise


def scrape_events(**context):
    """Task 2: Scrape events and upload to S3"""
    logger.info("="*60)
    logger.info("TASK: Scraping Events")
    
    try:
        from scrapper import DynamicAutoNewsScraper
        
        scraper = DynamicAutoNewsScraper(headless=True, slow_mo=0)
        
        logger.info("Fetching events from AutoNews events page...")
        events = scraper.scrape_events_dynamic()
        
        if not events:
            logger.warning("No events scraped!")
            context['task_instance'].xcom_push(key='events_count', value=0)
            return "No events scraped"
        
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{S3_EVENTS_PREFIX}events_{timestamp}.json"
        
        # Upload to S3
        s3_path = upload_to_s3(events, s3_key)
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(key='events_count', value=len(events))
        context['task_instance'].xcom_push(key='events_s3_key', value=s3_key)
        context['task_instance'].xcom_push(key='events_s3_path', value=s3_path)
        
        logger.info(f"✅ Scraped {len(events)} events → {s3_path}")
        return f"Successfully scraped {len(events)} events"
        
    except Exception as e:
        logger.error(f"Error in scrape_events: {e}")
        raise


def load_articles_to_snowflake(**context):
    """Task 3: Load articles from S3 to Snowflake"""
    logger.info("="*60)
    logger.info("TASK: Loading Articles from S3 to Snowflake")
    
    try:
        from snowflake_loader import SnowflakeLoader
        
        # Get S3 key from XCom
        s3_key = context['task_instance'].xcom_pull(
            task_ids='scrape_news_articles', 
            key='articles_s3_key'
        )
        
        if not s3_key:
            logger.warning("No articles S3 key found")
            return "Skipped loading"
        
        # Download from S3
        articles = download_from_s3(s3_key)
        
        if not articles:
            logger.warning("No articles to load")
            return "No articles to load"
        
        # Connect to Snowflake and load
        loader = SnowflakeLoader()
        if not loader.connect():
            raise Exception("Failed to connect to Snowflake")
        
        inserted_count = loader.insert_articles(articles)
        total_in_db = loader.get_article_count()
        loader.disconnect()
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(key='articles_loaded', value=inserted_count)
        context['task_instance'].xcom_push(key='total_articles_in_db', value=total_in_db)
        
        logger.info(f"✅ Loaded {inserted_count} articles to Snowflake")
        return f"Loaded {inserted_count} articles"
        
    except Exception as e:
        logger.error(f"Error in load_articles_to_snowflake: {e}")
        raise


def load_events_to_snowflake(**context):
    """Task 4: Load events from S3 to Snowflake"""
    logger.info("="*60)
    logger.info("TASK: Loading Events from S3 to Snowflake")
    
    try:
        from snowflake_loader import SnowflakeLoader
        
        # Get S3 key from XCom
        s3_key = context['task_instance'].xcom_pull(
            task_ids='scrape_events', 
            key='events_s3_key'
        )
        
        if not s3_key:
            logger.warning("No events S3 key found")
            return "Skipped loading"
        
        # Download from S3
        events = download_from_s3(s3_key)
        
        if not events:
            logger.warning("No events to load")
            return "No events to load"
        
        # Connect to Snowflake and load
        loader = SnowflakeLoader()
        if not loader.connect():
            raise Exception("Failed to connect to Snowflake")
        
        inserted_count = loader.insert_events(events)
        total_in_db = loader.get_event_count()
        loader.disconnect()
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(key='events_loaded', value=inserted_count)
        context['task_instance'].xcom_push(key='total_events_in_db', value=total_in_db)
        
        logger.info(f"✅ Loaded {inserted_count} events to Snowflake")
        return f"Loaded {inserted_count} events"
        
    except Exception as e:
        logger.error(f"Error in load_events_to_snowflake: {e}")
        raise


def send_completion_summary(**context):
    """Task 5: Print summary"""
    
    def get_xcom(task_id, key):
        val = context['task_instance'].xcom_pull(task_ids=task_id, key=key)
        return val if val is not None else 0

    articles_scraped = get_xcom('scrape_news_articles', 'articles_count')
    articles_loaded = get_xcom('load_articles_to_snowflake', 'articles_loaded')
    total_articles = get_xcom('load_articles_to_snowflake', 'total_articles_in_db')
    articles_s3_path = context['task_instance'].xcom_pull(
        task_ids='scrape_news_articles', 
        key='articles_s3_path'
    )
    
    events_scraped = get_xcom('scrape_events', 'events_count')
    events_loaded = get_xcom('load_events_to_snowflake', 'events_loaded')
    total_events = get_xcom('load_events_to_snowflake', 'total_events_in_db')
    events_s3_path = context['task_instance'].xcom_pull(
        task_ids='scrape_events', 
        key='events_s3_path'
    )
    
    summary = f"""
    ============================================
    ✅ AutoInsight News Scraper Completed
    ============================================
    📰 News Articles:
       - Scraped: {articles_scraped}
       - Loaded to Snowflake: {articles_loaded} new/updated
       - Total in DB: {total_articles}
       - S3 Location: {articles_s3_path}
    
    📅 Events:
       - Scraped: {events_scraped}
       - Loaded to Snowflake: {events_loaded} new/updated
       - Total in DB: {total_events}
       - S3 Location: {events_s3_path}
    ============================================
    """
    logger.info(summary)
    print(summary)


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    'autoinsight_daily_scraper',
    default_args=default_args,
    description='Scrapes AutoNews → S3 → Snowflake',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    tags=['autoinsight', 'snowflake', 'news', 's3'],
    max_active_runs=1,
    doc_md="""
    # AutoInsight Daily News & Events Scraper
    
    ## Data Flow
```
    AutoNews.com → Playwright Scraper → S3 (JSON) → Snowflake
```
    
    ## Outputs
    - **S3**: `s3://{S3_BUCKET}/news/raw/articles_*.json`
    - **S3**: `s3://{S3_BUCKET}/events/raw/events_*.json`
    - **Snowflake**: `AUTOMOTIVE_AI.NEWS.ARTICLES` and `AUTOMOTIVE_AI.NEWS.EVENTS`
    
    ## Schedule
    - Runs daily at 6:00 AM UTC
    - Scrapes latest news and events
    - Loads to Snowflake (upsert on URL)
    """
) as dag:

    # Environment check
    check_env = BashOperator(
        task_id='check_environment',
        bash_command='''
            echo "Checking environment..."
            echo "S3_BUCKET_NAME: ${S3_BUCKET_NAME:-NOT SET}"
            echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:+SET}"
            echo "SNOWFLAKE_USER: ${SNOWFLAKE_USER:+SET}"
            
            if [ -z "$S3_BUCKET_NAME" ]; then
                echo "ERROR: S3_BUCKET_NAME not set"
                exit 1
            fi
            
            echo "✅ Environment check passed"
        '''
    )

    # Scrape Tasks
    scrape_news = PythonOperator(
        task_id='scrape_news_articles',
        python_callable=scrape_news_articles,
        execution_timeout=timedelta(minutes=30),
    )

    scrape_events_task = PythonOperator(
        task_id='scrape_events',
        python_callable=scrape_events,
        execution_timeout=timedelta(minutes=30),
    )

    # Load Tasks
    load_articles = PythonOperator(
        task_id='load_articles_to_snowflake',
        python_callable=load_articles_to_snowflake,
        execution_timeout=timedelta(minutes=15),
    )

    load_events = PythonOperator(
        task_id='load_events_to_snowflake',
        python_callable=load_events_to_snowflake,
        execution_timeout=timedelta(minutes=15),
    )

    # Summary
    summary = PythonOperator(
        task_id='completion_summary',
        python_callable=send_completion_summary,
        trigger_rule='all_done',
    )

    # ========================================================================
    # DEPENDENCIES
    # ========================================================================
    
    # Check environment first
    check_env >> [scrape_news, scrape_events_task]
    
    # Load after specific scraping finishes
    scrape_news >> load_articles
    scrape_events_task >> load_events
    
    # Summary runs after both loading tasks finish
    [load_articles, load_events] >> summary