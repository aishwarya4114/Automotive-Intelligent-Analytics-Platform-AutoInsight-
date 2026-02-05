"""
AutoInsight Data Pipeline - Apache Airflow DAG
Orchestrates EPA, FRED, and NHTSA data collection with S3 upload

Coverage: 2015-2025 (11 years)
Schedule: Daily at 2 AM UTC
Retries: 3 attempts with 5-minute delays
Dependencies: EPA → FRED → NHTSA (sequential)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import sys

# ============================================================================
# DEFAULT DAG ARGUMENTS
# ============================================================================

default_args = {
    'owner': 'autoinsight-team',
    'depends_on_past': False,
    'email': ['autoinsight@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def fetch_epa_data(**context):
    """
    Task 1: Fetch EPA Fuel Economy Data
    Runs the EPA data collection script
    """
    print("="*70)
    print("TASK 1: EPA Fuel Economy Data Collection")
    print("="*70)
    
    try:
        # Import from data_cleaning package (UPDATED PATH)
        from data_cleaning.get_epa_data import main as epa_main
        
        result = epa_main()
        
        if hasattr(result, '__len__'):
            record_count = len(result)
            
            context['task_instance'].xcom_push(
                key='epa_records',
                value=record_count
            )
            
            if hasattr(result, 'make'):
                context['task_instance'].xcom_push(
                    key='epa_manufacturers',
                    value=result['make'].nunique()
                )
            
            print(f"✅ EPA Data Collection Complete: {record_count:,} records")
        else:
            print(f"✅ EPA Data Collection Complete")
        
        return True
        
    except Exception as e:
        print(f"❌ EPA Data Collection Failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def fetch_fred_data(**context):
    """
    Task 2: Fetch FRED Economic Indicators
    Runs the FRED data collection script
    """
    print("="*70)
    print("TASK 2: FRED Economic Data Collection")
    print("="*70)
    
    try:
        # Import from data_cleaning package (UPDATED PATH)
        from data_cleaning.get_fred_data import main as fred_main
        
        result = fred_main()
        
        if hasattr(result, '__len__'):
            context['task_instance'].xcom_push(
                key='fred_records',
                value=len(result)
            )
            print(f"✅ FRED Data Collection Complete: {len(result):,} records")
        else:
            print(f"✅ FRED Data Collection Complete")
        
        return True
        
    except Exception as e:
        print(f"❌ FRED Data Collection Failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def fetch_nhtsa_data(**context):
    """
    Task 3: Fetch NHTSA Safety Data
    Runs the NHTSA bulk data collection script
    """
    print("="*70)
    print("TASK 3: NHTSA Safety Data Collection")
    print("="*70)
    
    try:
        # Ensure NHTSA uses the mounted data directory
        os.environ['NHTSA_OUTPUT_DIR'] = '/opt/airflow/data/nhtsa_bulk'
        
        # Import from data_cleaning package (UPDATED PATH)
        from data_cleaning.get_nhtsa_data import main as nhtsa_main
        
        result = nhtsa_main()
        
        context['task_instance'].xcom_push(
            key='nhtsa_status',
            value='completed'
        )
        
        print(f"✅ NHTSA Data Collection Complete")
        return True
        
    except Exception as e:
        print(f"❌ NHTSA Data Collection Failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def generate_pipeline_summary(**context):
    """
    Task 4: Generate Pipeline Summary Report
    Aggregates results from all tasks
    """
    print("="*70)
    print("TASK 4: Pipeline Summary Generation")
    print("="*70)
    
    ti = context['task_instance']
    
    epa_records = ti.xcom_pull(task_ids='fetch_epa_data', key='epa_records')
    epa_manufacturers = ti.xcom_pull(task_ids='fetch_epa_data', key='epa_manufacturers')
    fred_records = ti.xcom_pull(task_ids='fetch_fred_data', key='fred_records')
    nhtsa_status = ti.xcom_pull(task_ids='fetch_nhtsa_data', key='nhtsa_status')
    
    summary = {
        'pipeline_run_date': datetime.now().isoformat(),
        'execution_date': context['execution_date'].isoformat(),
        'epa_records': epa_records or 0,
        'epa_manufacturers': epa_manufacturers or 0,
        'fred_records': fred_records or 0,
        'nhtsa_status': nhtsa_status or 'unknown',
        'overall_status': 'success'
    }
    
    print("\n" + "="*70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("="*70)
    print(f"Run Date: {summary['pipeline_run_date']}")
    print(f"EPA Records: {summary['epa_records']:,}")
    print(f"EPA Manufacturers: {summary['epa_manufacturers']}")
    print(f"FRED Records: {summary['fred_records']:,}")
    print(f"NHTSA Status: {summary['nhtsa_status']}")
    print(f"Overall Status: {summary['overall_status']}")
    print("="*70)
    
    try:
        import json
        import boto3
        
        S3_BUCKET = os.getenv('S3_BUCKET_NAME')
        
        if S3_BUCKET:
            s3_client = boto3.client('s3')
            
            summary_file = f"/tmp/pipeline_summary_{context['ds']}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            s3_key = f"pipeline_summaries/summary_{context['ds']}.json"
            s3_client.upload_file(summary_file, S3_BUCKET, s3_key)
            
            print(f"✅ Summary uploaded to s3://{S3_BUCKET}/{s3_key}")
    
    except Exception as e:
        print(f"⚠️ Could not upload summary: {e}")
    
    return summary


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id='autoinsight_data_pipeline',
    default_args=default_args,
    description='AutoInsight: EPA, FRED, NHTSA data collection (2015-2025)',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['autoinsight', 'data-collection', 'epa', 'fred', 'nhtsa'],
    max_active_runs=1,
) as dag:
    
    # ... (rest of your DAG definition remains the same)
    
    check_environment = BashOperator(
        task_id='check_environment',
        bash_command='''
            echo "Checking environment variables..."
            echo "S3_BUCKET_NAME: ${S3_BUCKET_NAME:-NOT SET}"
            echo "FRED_API_KEY: ${FRED_API_KEY:+SET}"
            echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:+SET}"
            
            if [ -z "$S3_BUCKET_NAME" ]; then
                echo "ERROR: S3_BUCKET_NAME not set"
                exit 1
            fi
            
            if [ -z "$FRED_API_KEY" ]; then
                echo "ERROR: FRED_API_KEY not set"
                exit 1
            fi
            
            echo "✅ Environment check passed"
        '''
    )
    
    task_epa = PythonOperator(
        task_id='fetch_epa_data',
        python_callable=fetch_epa_data,
        provide_context=True,
    )
    
    task_fred = PythonOperator(
        task_id='fetch_fred_data',
        python_callable=fetch_fred_data,
        provide_context=True,
    )
    
    task_nhtsa = PythonOperator(
        task_id='fetch_nhtsa_data',
        python_callable=fetch_nhtsa_data,
        provide_context=True,
    )
    
    task_summary = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_pipeline_summary,
        provide_context=True,
    )
    
    cleanup = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='''
            echo "Cleaning up temporary files..."
            rm -f /tmp/pipeline_summary_*.json
            echo "✅ Cleanup complete"
        ''',
        trigger_rule='all_done',
    )
    
    # Task dependencies
    check_environment >> task_epa >> task_fred >> task_nhtsa >> task_summary >> cleanup