from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3
from minio import Minio
import pandas as pd
import requests
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'medallion_data_pipeline',
    default_args=default_args,
    description='Data pipeline with medallion architecture using Spark, Airflow, and MinIO',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['data-pipeline', 'medallion', 'spark'],
)

# MinIO configuration
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'
MINIO_SECURE = False

# S3 source configuration
SOURCE_BUCKET = 'epf-big-data-processing'
SOURCE_PREFIX = 'tp3-4/sources'

def create_minio_client():
    """Create MinIO client"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def ingest_from_s3(**context):
    """
    Ingest data from external S3 to MinIO landing zone
    """
    try:
        # Create MinIO client
        minio_client = create_minio_client()
        
        # Create S3 client for source data
        s3_client = boto3.client('s3')
        
        # List objects in source bucket
        response = s3_client.list_objects_v2(
            Bucket=SOURCE_BUCKET,
            Prefix=SOURCE_PREFIX
        )
        
        if 'Contents' not in response:
            print(f"No objects found in {SOURCE_BUCKET}/{SOURCE_PREFIX}")
            return
        
        # Download and upload each file
        for obj in response['Contents']:
            key = obj['Key']
            filename = key.split('/')[-1]
            
            if filename:  # Skip directories
                print(f"Processing file: {filename}")
                
                # Download from S3
                s3_client.download_file(SOURCE_BUCKET, key, f'/tmp/{filename}')
                
                # Upload to MinIO landing bucket
                minio_client.fput_object(
                    'landing',
                    filename,
                    f'/tmp/{filename}'
                )
                
                # Clean up temporary file
                os.remove(f'/tmp/{filename}')
                
                print(f"Successfully ingested {filename} to MinIO landing zone")
                
    except Exception as e:
        print(f"Error in data ingestion: {str(e)}")
        raise

def process_to_bronze(**context):
    """
    Process data from landing to bronze layer
    """
    try:
        minio_client = create_minio_client()
        
        # List objects in landing bucket
        objects = minio_client.list_objects('landing')
        
        for obj in objects:
            print(f"Processing {obj.object_name} to bronze layer")
            
            # Get object from landing
            response = minio_client.get_object('landing', obj.object_name)
            
            # For this example, we'll just copy to bronze with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            bronze_key = f"{timestamp}_{obj.object_name}"
            
            # Save to bronze bucket
            minio_client.put_object(
                'bronze',
                bronze_key,
                response,
                length=-1,
                part_size=10*1024*1024
            )
            
            print(f"Successfully processed {obj.object_name} to bronze layer as {bronze_key}")
            
    except Exception as e:
        print(f"Error in bronze processing: {str(e)}")
        raise

def submit_spark_job(**context):
    """
    Submit Spark job for data transformation
    """
    spark_submit_cmd = """
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --class org.apache.spark.examples.SparkPi \
        /opt/bitnami/spark/apps/data_transformation.py
    """
    
    os.system(spark_submit_cmd)

def process_to_silver(**context):
    """
    Process data from bronze to silver layer with Spark
    This would typically involve data cleaning, validation, and standardization
    """
    try:
        minio_client = create_minio_client()
        
        # List objects in bronze bucket
        objects = minio_client.list_objects('bronze')
        
        for obj in objects:
            print(f"Processing {obj.object_name} to silver layer")
            
            # Get object from bronze
            response = minio_client.get_object('bronze', obj.object_name)
            
            # Process data (this is a simplified example)
            # In real scenarios, you would use Spark for complex transformations
            
            silver_key = f"processed_{obj.object_name}"
            
            # Save to silver bucket
            minio_client.put_object(
                'silver',
                silver_key,
                response,
                length=-1,
                part_size=10*1024*1024
            )
            
            print(f"Successfully processed {obj.object_name} to silver layer as {silver_key}")
            
    except Exception as e:
        print(f"Error in silver processing: {str(e)}")
        raise

def process_to_gold(**context):
    """
    Process data from silver to gold layer for analytics
    """
    try:
        minio_client = create_minio_client()
        
        # List objects in silver bucket
        objects = minio_client.list_objects('silver')
        
        for obj in objects:
            print(f"Processing {obj.object_name} to gold layer")
            
            # Get object from silver
            response = minio_client.get_object('silver', obj.object_name)
            
            # Process data for analytics (aggregations, business logic, etc.)
            gold_key = f"analytics_{obj.object_name}"
            
            # Save to gold bucket
            minio_client.put_object(
                'gold',
                gold_key,
                response,
                length=-1,
                part_size=10*1024*1024
            )
            
            print(f"Successfully processed {obj.object_name} to gold layer as {gold_key}")
            
    except Exception as e:
        print(f"Error in gold processing: {str(e)}")
        raise

def data_quality_check(**context):
    """
    Perform data quality checks
    """
    try:
        minio_client = create_minio_client()
        
        # Check each layer
        layers = ['bronze', 'silver', 'gold']
        
        for layer in layers:
            objects = list(minio_client.list_objects(layer))
            object_count = len(objects)
            
            print(f"{layer.capitalize()} layer contains {object_count} objects")
            
            if object_count == 0:
                print(f"WARNING: {layer} layer is empty!")
            
        print("Data quality check completed")
        
    except Exception as e:
        print(f"Error in data quality check: {str(e)}")
        raise

# Task definitions
ingest_task = PythonOperator(
    task_id='ingest_from_s3',
    python_callable=ingest_from_s3,
    dag=dag,
)

bronze_task = PythonOperator(
    task_id='process_to_bronze',
    python_callable=process_to_bronze,
    dag=dag,
)

spark_task = BashOperator(
    task_id='spark_transformation',
    bash_command="""
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/bitnami/spark/apps/data_transformation.py || true
    """,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='process_to_silver',
    python_callable=process_to_silver,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='process_to_gold',
    python_callable=process_to_gold,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Task dependencies (Medallion Architecture Flow)
ingest_task >> bronze_task >> spark_task >> silver_task >> gold_task >> quality_check_task
