from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import boto3
from minio import Minio
import requests
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    'medallion_pipeline',
    default_args=default_args,
    description='Complete medallion architecture pipeline for companies with 2022 public contracts',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['data-pipeline', 'medallion', 'contracts', 'analytics'],
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
    """Check if all services are running"""
    try:
        # Check MinIO
        minio_client = create_minio_client()
        buckets = minio_client.list_buckets()
        logger.info(f"✅ MinIO is running. Found buckets: {[b.name for b in buckets]}")
        
        # Check Spark
        try:
            response = requests.get('http://spark:8080/json/', timeout=10)
            if response.status_code == 200:
                logger.info("✅ Spark is running")
            else:
                logger.warning(f"⚠️ Spark responded with status: {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Spark check failed: {str(e)}")
        
        logger.info("✅ Service check completed")
        
    except Exception as e:
        logger.error(f"❌ Service check failed: {str(e)}")
        raise

def check_landing_data(**context):
    """
    Check if required data files exist in landing bucket
    """
    try:
        minio_client = create_minio_client()
        
        required_files = [
            'aws-marchespublics-annee-2022.json',
            'simulated_etablissements_50000.csv',
            'int_courts_naf_rev_2.csv'
        ]
        
        existing_files = []
        for obj in minio_client.list_objects('landing'):
            # Extract base filename from object name (removing any path prefixes)
            base_name = obj.object_name.split('/')[-1]
            if base_name in required_files:
                existing_files.append(base_name)
        
        missing_files = set(required_files) - set(existing_files)
        
        if missing_files:
            logger.error(f"❌ Missing required files in landing bucket: {missing_files}")
            raise Exception(f"Missing required files: {missing_files}")
        
        logger.info(f"✅ All required data files found in landing bucket: {existing_files}")
        
        # Store file info for downstream tasks
        context['task_instance'].xcom_push(key='landing_files', value=existing_files)
        
    except Exception as e:
        logger.error(f"❌ Landing data check failed: {str(e)}")
        raise

def validate_data_quality(**context):
    """
    Perform basic data quality checks on landing data
    """
    try:
        minio_client = create_minio_client()
        
        # Check file sizes
        total_size = 0
        file_info = {}
        
        for obj in minio_client.list_objects('landing'):
            stat = minio_client.stat_object('landing', obj.object_name)
            total_size += stat.size
            file_info[obj.object_name] = {
                'size': stat.size,
                'last_modified': stat.last_modified
            }
        
        logger.info(f"✅ Landing data validation completed")
        logger.info(f"Total data size: {total_size / (1024*1024):.2f} MB")
        
        for filename, info in file_info.items():
            logger.info(f"File: {filename}, Size: {info['size'] / (1024*1024):.2f} MB, Modified: {info['last_modified']}")
        
        # Store validation results
        context['task_instance'].xcom_push(key='data_quality_report', value={
            'total_size_mb': total_size / (1024*1024),
            'file_count': len(file_info),
            'file_info': file_info
        })
        
    except Exception as e:
        logger.error(f"❌ Data quality validation failed: {str(e)}")
        raise

def generate_pipeline_summary(**context):
    """
    Generate comprehensive pipeline execution summary
    """
    try:
        minio_client = create_minio_client()
        
        summary = f"""
🎯 MEDALLION PIPELINE EXECUTION SUMMARY
{'='*60}
Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Pipeline: Companies with 2022 Public Contracts Analysis

📊 DATA LAYER STATUS:
"""
        
        # Check each layer
        layers = {
            'landing': 'Raw source data',
            'bronze': 'Ingested and flattened data',
            'silver': 'Cleaned and transformed data',
            'gold': 'Analytics-ready datasets'
        }
        
        for layer, description in layers.items():
            try:
                objects = list(minio_client.list_objects(layer))
                total_size = 0
                
                for obj in objects:
                    stat = minio_client.stat_object(layer, obj.object_name)
                    total_size += stat.size
                
                summary += f"""
  {layer.upper()} Layer - {description}:
    • Objects: {len(objects)}
    • Total Size: {total_size / (1024*1024):.2f} MB
"""
                
                if objects:
                    latest = max(objects, key=lambda x: x.last_modified)
                    summary += f"    • Latest: {latest.object_name}\n"
                    
            except Exception as e:
                summary += f"    • {layer.upper()}: Error - {str(e)}\n"
        
        summary += f"""
🎯 ANALYSIS FOCUS:
  • Objective: Identify companies with 2022 public contracts still active today
  • Data Sources: French public procurement, establishment registry, NAF codes
  • Output: Analytics datasets with company insights and aggregations

✅ PIPELINE COMPLETED SUCCESSFULLY!
{'='*60}
"""
        
        logger.info(summary)
        
        # Store summary in gold layer
        summary_filename = f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        minio_client.put_object('gold', summary_filename, summary.encode('utf-8'), len(summary.encode('utf-8')))
        
        logger.info(f"Pipeline summary saved to gold/{summary_filename}")
        
    except Exception as e:
        logger.error(f"❌ Pipeline summary generation failed: {str(e)}")
        raise

# Task definitions for the complete medallion pipeline

# 1. Check services are running
check_services_task = PythonOperator(
    task_id='check_services',
    python_callable=check_services,
    dag=dag,
)

# 2. Validate landing data exists
check_landing_task = PythonOperator(
    task_id='check_landing_data',
    python_callable=check_landing_data,
    dag=dag,
)

# 3. Data quality validation
validate_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# 4. Landing to Bronze transformation
landing_to_bronze_task = BashOperator(
    task_id='landing_to_bronze_transformation',
    bash_command="""
    docker exec spark-master spark-submit \
        --master spark://spark:7077 \
        --driver-memory 2g \
        --executor-memory 2g \
        /opt/spark/apps/data_transformation.py landing_to_bronze
    """,
    dag=dag,
)

# 5. Bronze to Silver transformation
bronze_to_silver_task = BashOperator(
    task_id='bronze_to_silver_transformation',
    bash_command="""
    docker exec spark-master spark-submit \
        --master spark://spark:7077 \
        --driver-memory 2g \
        --executor-memory 2g \
        /opt/spark/apps/data_transformation.py bronze_to_silver
    """,
    dag=dag,
)

# 6. Silver to Gold transformation
silver_to_gold_task = BashOperator(
    task_id='silver_to_gold_transformation',
    bash_command="""
    docker exec spark-master spark-submit \
        --master spark://spark:7077 \
        --driver-memory 2g \
        --executor-memory 2g \
        /opt/spark/apps/data_transformation.py silver_to_gold
    """,
    dag=dag,
)

# 7. Generate pipeline summary
summary_task = PythonOperator(
    task_id='generate_pipeline_summary',
    python_callable=generate_pipeline_summary,
    dag=dag,
)

# Task dependencies - Complete medallion pipeline flow
check_services_task >> check_landing_task >> validate_quality_task
validate_quality_task >> landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task >> summary_task
