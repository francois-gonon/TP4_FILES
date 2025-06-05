from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from minio import Minio
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add dags directory to path to import our transformation script
sys.path.append('/opt/airflow/dags')

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'medallion_pipeline_final',
    default_args=default_args,
    description='Complete medallion architecture pipeline for companies with 2022 public contracts',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['data-pipeline', 'medallion', 'contracts', 'analytics', 'final'],
)

# MinIO configuration
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'

def create_minio_client():
    """Create MinIO client"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def check_services(**context):
    """Check if MinIO and other services are accessible"""
    try:
        client = create_minio_client()
        buckets = client.list_buckets()
        logger.info(f"MinIO is accessible. Found {len(buckets)} buckets")
        
        # Check if source data exists
        expected_files = [
            "aws-marchespublics-annee-2022.json",
            "simulated_etablissements_50000.csv",
            "int_courts_naf_rev_2.csv"
        ]
        
        missing_files = []
        for file in expected_files:
            try:
                client.stat_object("landing", file)
                logger.info(f"Found source file: {file}")
            except Exception as e:
                missing_files.append(file)
                logger.warning(f"Missing source file: {file}")
        
        if missing_files:
            raise ValueError(f"Missing source files: {missing_files}")
        
        return {"status": "success", "message": "All services and data are accessible"}
        
    except Exception as e:
        logger.error(f"Service check failed: {str(e)}")
        raise

def run_landing_to_bronze(**context):
    """Execute landing to bronze transformation"""
    try:
        from pandas_transformation_clean import landing_to_bronze_transformation
        result = landing_to_bronze_transformation()
        logger.info(f"Landing to Bronze completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Landing to Bronze failed: {str(e)}")
        raise

def run_bronze_to_silver(**context):
    """Execute bronze to silver transformation"""
    try:
        from pandas_transformation_clean import bronze_to_silver_transformation
        result = bronze_to_silver_transformation()
        logger.info(f"Bronze to Silver completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Bronze to Silver failed: {str(e)}")
        raise

def run_silver_to_gold(**context):
    """Execute silver to gold transformation"""
    try:
        from pandas_transformation_clean import silver_to_gold_transformation
        result = silver_to_gold_transformation()
        logger.info(f"Silver to Gold completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Silver to Gold failed: {str(e)}")
        raise

def validate_gold_results(**context):
    """Validate final results in gold bucket"""
    try:
        client = create_minio_client()
        
        # Check if gold files exist
        expected_files = [
            "companies_2022_contracts_still_active.parquet",
            "analysis_summary.json"
        ]
        
        for file in expected_files:
            try:
                obj_stat = client.stat_object("gold", file)
                logger.info(f"Gold file {file} exists (size: {obj_stat.size} bytes)")
            except Exception as e:
                raise ValueError(f"Missing gold file: {file}")
        
        # Load and validate summary
        import json
        summary_obj = client.get_object("gold", "analysis_summary.json")
        summary_data = json.loads(summary_obj.read().decode('utf-8'))
        
        logger.info("=== FINAL PIPELINE RESULTS ===")
        logger.info(f"Total companies with 2022 contracts: {summary_data.get('total_companies_with_2022_contracts', 0)}")
        logger.info(f"Companies still active today: {summary_data.get('companies_still_active', 0)}")
        logger.info(f"Total contract value: €{summary_data.get('total_contract_value', 0):,.2f}")
        logger.info(f"Average contracts per company: {summary_data.get('average_contracts_per_company', 0):.2f}")
        
        if summary_data.get('companies_still_active', 0) == 0:
            raise ValueError("No companies found with 2022 contracts that are still active")
        
        return {
            "status": "success",
            "summary": summary_data,
            "message": f"Found {summary_data.get('companies_still_active', 0)} companies with 2022 contracts still active today"
        }
        
    except Exception as e:
        logger.error(f"Gold validation failed: {str(e)}")
        raise

def create_buckets(**context):
    """Create required MinIO buckets if they don't exist"""
    try:
        client = create_minio_client()
        buckets = ['landing', 'bronze', 'silver', 'gold']
        
        for bucket in buckets:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
            else:
                logger.info(f"Bucket already exists: {bucket}")
        
        return {"status": "success", "message": "All buckets are ready"}
        
    except Exception as e:
        logger.error(f"Bucket creation failed: {str(e)}")
        raise

# Define tasks
check_services_task = PythonOperator(
    task_id='check_services',
    python_callable=check_services,
    dag=dag,
)

create_buckets_task = PythonOperator(
    task_id='create_buckets',
    python_callable=create_buckets,
    dag=dag,
)

landing_to_bronze_task = PythonOperator(
    task_id='landing_to_bronze',
    python_callable=run_landing_to_bronze,
    dag=dag,
)

bronze_to_silver_task = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=run_bronze_to_silver,
    dag=dag,
)

silver_to_gold_task = PythonOperator(
    task_id='silver_to_gold',
    python_callable=run_silver_to_gold,
    dag=dag,
)

validate_results_task = PythonOperator(
    task_id='validate_gold_results',
    python_callable=validate_gold_results,
    dag=dag,
)

# Set task dependencies
check_services_task >> create_buckets_task >> landing_to_bronze_task
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task >> validate_results_task
