"""
AutoInsight Cleaning & Loading Pipeline - Apache Airflow DAG v2
Cleans raw data from S3 using PySpark and loads into Snowflake

Pipeline Flow:
    Raw S3 Data → PySpark Cleaning → Cleaned S3 → Snowflake Load

Datasets:
    - EPA Fuel Economy
    - FRED Economic Indicators  
    - NHTSA: Recalls, Complaints (with sentiment), Ratings, Investigations
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    'owner': 'autoinsight-team',
    'depends_on_past': False,
    'email': ['autoinsight@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# S3 Configuration - CSV paths for Snowflake loading
S3_BUCKET = os.getenv('S3_BUCKET_NAME', 'autoinsightfinal')

S3_PATHS = {
    'epa': f'epa/cleaned_new/fuel_economy/csv/',
    'fred': f'fred/cleaned_new/economic_indicators/csv/',
    'recalls': f'nhtsa/cleaned_new/recalls/csv/',
    'complaints': f'nhtsa/cleaned_new/complaints/csv/',
    'ratings': f'nhtsa/cleaned_new/ratings/csv/',
    'investigations': f'nhtsa/cleaned_new/investigations/csv/',
}

# Snowflake table mapping
SNOWFLAKE_TABLES = {
    'epa': {'schema': 'EPA', 'table': 'FACT_FUEL_ECONOMY'},
    'fred': {'schema': 'FRED', 'table': 'FACT_ECONOMIC_INDICATORS'},
    'recalls': {'schema': 'NHTSA', 'table': 'FACT_RECALLS'},
    'complaints': {'schema': 'NHTSA', 'table': 'FACT_COMPLAINTS'},
    'ratings': {'schema': 'NHTSA', 'table': 'FACT_RATINGS'},
    'investigations': {'schema': 'NHTSA', 'table': 'FACT_INVESTIGATIONS'},
}


# ============================================================================
# CLEANING TASK FUNCTIONS
# ============================================================================

def run_cleaning_task(module_name: str, task_name: str, **context):
    """
    Generic wrapper to run cleaning scripts
    Imports and executes the main() function from cleaning modules
    """
    print("=" * 70)
    print(f"CLEANING TASK: {task_name}")
    print("=" * 70)
    
    try:
        if module_name == 'clean_epa_fuel_economy':
            from data_cleaning.clean_epa_fuel_economy import main
        elif module_name == 'clean_fred_economic':
            from data_cleaning.clean_fred_economic import main
        elif module_name == 'clean_nhtsa_recalls':
            from data_cleaning.clean_nhtsa_recalls import main
        elif module_name == 'clean_nhtsa_complaints':
            from data_cleaning.clean_nhtsa_complaints import main
        elif module_name == 'clean_nhtsa_ratings':
            from data_cleaning.clean_nhtsa_ratings import main
        elif module_name == 'clean_nhtsa_investigation':
            from data_cleaning.clean_nhtsa_investigation import main
        else:
            raise ValueError(f"Unknown module: {module_name}")
        
        main()
        
        print(f"✅ {task_name} Complete")
        context['task_instance'].xcom_push(key=f'{module_name}_status', value='success')
        return True
        
    except Exception as e:
        print(f"❌ {task_name} Failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def clean_epa_data(**context):
    """Clean EPA Fuel Economy Data using PySpark"""
    return run_cleaning_task('clean_epa_fuel_economy', 'EPA Fuel Economy', **context)


def clean_fred_data(**context):
    """Clean FRED Economic Data using PySpark"""
    return run_cleaning_task('clean_fred_economic', 'FRED Economic Indicators', **context)


def clean_nhtsa_recalls(**context):
    """Clean NHTSA Recalls Data using PySpark"""
    return run_cleaning_task('clean_nhtsa_recalls', 'NHTSA Recalls', **context)


def clean_nhtsa_complaints(**context):
    """Clean NHTSA Complaints Data with embedded sentiment analysis"""
    return run_cleaning_task('clean_nhtsa_complaints', 'NHTSA Complaints (with Sentiment)', **context)


def clean_nhtsa_ratings(**context):
    """Clean NHTSA Safety Ratings Data using PySpark"""
    return run_cleaning_task('clean_nhtsa_ratings', 'NHTSA Safety Ratings', **context)


def clean_nhtsa_investigations(**context):
    """Clean NHTSA Investigations Data using PySpark"""
    return run_cleaning_task('clean_nhtsa_investigation', 'NHTSA Investigations', **context)


# ============================================================================
# SNOWFLAKE LOADING FUNCTIONS
# ============================================================================

def get_snowflake_connection():
    """Create Snowflake connection using environment variables"""
    import snowflake.connector
    
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database='AUTOMOTIVE_AI'
    )


def load_csv_to_snowflake(dataset_key: str, **context):
    """
    Generic function to load CSV from S3 to Snowflake
    
    Args:
        dataset_key: Key to lookup S3 path and Snowflake table config
    """
    import pandas as pd
    from io import BytesIO
    import boto3
    from snowflake.connector.pandas_tools import write_pandas
    
    s3_path = S3_PATHS[dataset_key]
    sf_config = SNOWFLAKE_TABLES[dataset_key]
    
    print("=" * 70)
    print(f"SNOWFLAKE LOAD: {dataset_key.upper()}")
    print(f"S3 Path: s3://{S3_BUCKET}/{s3_path}")
    print(f"Target: AUTOMOTIVE_AI.{sf_config['schema']}.{sf_config['table']}")
    print("=" * 70)
    
    try:
        # Connect to S3
        s3_client = boto3.client('s3')
        
        # List CSV files in path
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_path)
        
        if 'Contents' not in response:
            raise Exception(f"No files found at s3://{S3_BUCKET}/{s3_path}")
        
        # Find CSV files (exclude _SUCCESS marker)
        csv_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.csv') and not obj['Key'].endswith('_SUCCESS')
        ]
        
        if not csv_files:
            raise Exception(f"No CSV files found at s3://{S3_BUCKET}/{s3_path}")
        
        # Read the first CSV file
        csv_key = csv_files[0]
        print(f"Reading: s3://{S3_BUCKET}/{csv_key}")
        
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=csv_key)
        
        # Read with error handling for malformed rows
        df = pd.read_csv(
            BytesIO(obj['Body'].read()),
            on_bad_lines='skip',
            encoding='utf-8',
            quoting=1,
            escapechar='\\'
        )
        
        print(f"Loaded {len(df):,} records from S3")
        print(f"Columns: {list(df.columns)}")
        
        # Handle array columns (complaint_embedding) - convert to string for Snowflake
        if 'complaint_embedding' in df.columns:
            df['complaint_embedding'] = df['complaint_embedding'].astype(str)
        
        # Connect to Snowflake
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Set schema
        cursor.execute(f"USE SCHEMA {sf_config['schema']}")
        
        # Load data using write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=sf_config['table'],
            auto_create_table=False,
            overwrite=True,
            quote_identifiers=False
        )
        
        print(f"✅ Loaded {nrows:,} rows into AUTOMOTIVE_AI.{sf_config['schema']}.{sf_config['table']}")
        
        # Verify row count
        cursor.execute(f"SELECT COUNT(*) FROM {sf_config['table']}")
        count = cursor.fetchone()[0]
        print(f"Verification: {count:,} rows in table")
        
        cursor.close()
        conn.close()
        
        context['task_instance'].xcom_push(
            key=f'{dataset_key}_rows_loaded',
            value=nrows
        )
        
        return True
        
    except Exception as e:
        print(f"❌ Snowflake Load Failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def load_epa_to_snowflake(**context):
    """Load cleaned EPA data to Snowflake"""
    return load_csv_to_snowflake('epa', **context)


def load_fred_to_snowflake(**context):
    """Load cleaned FRED data to Snowflake"""
    return load_csv_to_snowflake('fred', **context)


def load_recalls_to_snowflake(**context):
    """Load cleaned NHTSA Recalls to Snowflake"""
    return load_csv_to_snowflake('recalls', **context)


def load_complaints_to_snowflake(**context):
    """Load cleaned NHTSA Complaints (with sentiment) to Snowflake"""
    return load_csv_to_snowflake('complaints', **context)


def load_ratings_to_snowflake(**context):
    """Load cleaned NHTSA Ratings to Snowflake"""
    return load_csv_to_snowflake('ratings', **context)


def load_investigations_to_snowflake(**context):
    """Load cleaned NHTSA Investigations to Snowflake"""
    return load_csv_to_snowflake('investigations', **context)


def generate_pipeline_report(**context):
    """Generate summary report of pipeline execution"""
    print("=" * 70)
    print("PIPELINE EXECUTION REPORT")
    print("=" * 70)
    
    ti = context['task_instance']
    
    datasets = ['epa', 'fred', 'recalls', 'complaints', 'ratings', 'investigations']
    
    print("\nRows Loaded to Snowflake:")
    total_rows = 0
    for ds in datasets:
        rows = ti.xcom_pull(key=f'{ds}_rows_loaded', default=0)
        if rows:
            print(f"  {ds.upper()}: {rows:,}")
            total_rows += rows
    
    print(f"\n  TOTAL: {total_rows:,} rows")
    print("\n✅ Pipeline Complete!")
    print("=" * 70)


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id='autoinsight_cleaning_pipeline_v2',
    default_args=default_args,
    description='AutoInsight: Clean raw data with PySpark and load to Snowflake',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['autoinsight', 'cleaning', 'snowflake', 'etl', 'pyspark'],
    max_active_runs=1,
    doc_md="""
    # AutoInsight Data Cleaning & Loading Pipeline v2
    
    ## Overview
    Cleans raw automotive data from S3 using PySpark, then loads to Snowflake.
    
    ## Data Flow
    ```
    S3 Raw → PySpark Clean → S3 Cleaned (CSV/Parquet) → Snowflake
    ```
    
    ## Datasets Processed
    | Dataset | Source | Snowflake Table |
    |---------|--------|-----------------|
    | EPA Fuel Economy | EPA API | AUTOMOTIVE_AI.EPA.FACT_FUEL_ECONOMY |
    | FRED Economic | FRED API | AUTOMOTIVE_AI.FRED.FACT_ECONOMIC_INDICATORS |
    | NHTSA Recalls | NHTSA API | AUTOMOTIVE_AI.NHTSA.FACT_RECALLS |
    | NHTSA Complaints | NHTSA API | AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS |
    | NHTSA Ratings | NHTSA API | AUTOMOTIVE_AI.NHTSA.FACT_RATINGS |
    | NHTSA Investigations | NHTSA API | AUTOMOTIVE_AI.NHTSA.FACT_INVESTIGATIONS |
    
    ## Special Processing
    - **Complaints**: Includes embedded sentiment analysis (RoBERTa-based)
    - **Recalls**: Deduplicated by NHTSA_ID (multiple docs per recall)
    - **All datasets**: Standardized vehicle_id for cross-dataset joins
    
    ## Environment Variables Required
    - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    - S3_BUCKET_NAME
    - SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE
    """
) as dag:
    
    # ========================================================================
    # START/END MARKERS
    # ========================================================================
    
    start = EmptyOperator(task_id='start')
    cleaning_complete = EmptyOperator(task_id='cleaning_complete')
    loading_complete = EmptyOperator(task_id='loading_complete')
    
    # ========================================================================
    # CLEANING TASKS (PySpark)
    # ========================================================================
    
    task_clean_epa = PythonOperator(
        task_id='clean_epa_data',
        python_callable=clean_epa_data,
        execution_timeout=timedelta(hours=1),
    )
    
    task_clean_fred = PythonOperator(
        task_id='clean_fred_data',
        python_callable=clean_fred_data,
        execution_timeout=timedelta(minutes=30),
    )
    
    task_clean_recalls = PythonOperator(
        task_id='clean_nhtsa_recalls',
        python_callable=clean_nhtsa_recalls,
        execution_timeout=timedelta(hours=1),
    )
    
    task_clean_complaints = PythonOperator(
        task_id='clean_nhtsa_complaints',
        python_callable=clean_nhtsa_complaints,
        execution_timeout=timedelta(hours=3),  # Longer for sentiment analysis
    )
    
    task_clean_ratings = PythonOperator(
        task_id='clean_nhtsa_ratings',
        python_callable=clean_nhtsa_ratings,
        execution_timeout=timedelta(hours=1),
    )
    
    task_clean_investigations = PythonOperator(
        task_id='clean_nhtsa_investigations',
        python_callable=clean_nhtsa_investigations,
        execution_timeout=timedelta(hours=1),
    )
    
    # ========================================================================
    # SNOWFLAKE LOADING TASKS
    # ========================================================================
    
    task_load_epa = PythonOperator(
        task_id='load_epa_to_snowflake',
        python_callable=load_epa_to_snowflake,
        execution_timeout=timedelta(minutes=30),
    )
    
    task_load_fred = PythonOperator(
        task_id='load_fred_to_snowflake',
        python_callable=load_fred_to_snowflake,
        execution_timeout=timedelta(minutes=30),
    )
    
    task_load_recalls = PythonOperator(
        task_id='load_recalls_to_snowflake',
        python_callable=load_recalls_to_snowflake,
        execution_timeout=timedelta(minutes=30),
    )
    
    task_load_complaints = PythonOperator(
        task_id='load_complaints_to_snowflake',
        python_callable=load_complaints_to_snowflake,
        execution_timeout=timedelta(hours=1),  # Larger dataset
    )
    
    task_load_ratings = PythonOperator(
        task_id='load_ratings_to_snowflake',
        python_callable=load_ratings_to_snowflake,
        execution_timeout=timedelta(minutes=30),
    )
    
    task_load_investigations = PythonOperator(
        task_id='load_investigations_to_snowflake',
        python_callable=load_investigations_to_snowflake,
        execution_timeout=timedelta(minutes=30),
    )
    
    # ========================================================================
    # REPORTING TASK
    # ========================================================================
    
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_pipeline_report,
        trigger_rule='all_done',  # Run even if some tasks failed
    )
    
    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================
    
    # Phase 1: All cleaning tasks run in parallel after start
    start >> [
        task_clean_epa,
        task_clean_fred,
        task_clean_recalls,
        task_clean_complaints,
        task_clean_ratings,
        task_clean_investigations
    ]
    
    # Phase 2: Each load task depends on its cleaning task
    task_clean_epa >> task_load_epa
    task_clean_fred >> task_load_fred
    task_clean_recalls >> task_load_recalls
    task_clean_complaints >> task_load_complaints
    task_clean_ratings >> task_load_ratings
    task_clean_investigations >> task_load_investigations
    
    # Cleaning complete marker
    [
        task_clean_epa,
        task_clean_fred,
        task_clean_recalls,
        task_clean_complaints,
        task_clean_ratings,
        task_clean_investigations
    ] >> cleaning_complete
    
    # Loading complete marker  
    [
        task_load_epa,
        task_load_fred,
        task_load_recalls,
        task_load_complaints,
        task_load_ratings,
        task_load_investigations
    ] >> loading_complete
    
    # Final report
    loading_complete >> task_report