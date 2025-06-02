from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Data pipeline with medallion architecture (Bronze, Silver, Gold)',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['data-pipeline', 'medallion', 'simple'],
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

def ingest_sample_data(**context):
    """
    Create sample data and ingest to landing zone
    """
    try:
        minio_client = create_minio_client()
        
        # Create sample CSV data
        sample_data = """id,name,department,salary,date
1,John Doe,Sales,50000,2025-01-01
2,Jane Smith,Marketing,60000,2025-01-02
3,Bob Johnson,IT,70000,2025-01-03
4,Alice Wilson,HR,55000,2025-01-04
5,Charlie Brown,Sales,48000,2025-01-05
6,Diana Prince,IT,75000,2025-01-06
7,Eva Green,Marketing,62000,2025-01-07
8,Frank Miller,HR,53000,2025-01-08"""

        # Save to local file first
        filename = f"sample_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        local_path = f"/tmp/{filename}"
        
        with open(local_path, 'w') as f:
            f.write(sample_data)
        
        # Upload to MinIO landing bucket
        minio_client.fput_object('landing', filename, local_path)
        
        # Clean up
        os.remove(local_path)
        
        logger.info(f"✅ Sample data ingested to landing zone: {filename}")
        
        # Store filename for next tasks
        context['task_instance'].xcom_push(key='filename', value=filename)
        
    except Exception as e:
        logger.error(f"❌ Data ingestion failed: {str(e)}")
        raise

def process_to_bronze(**context):
    """
    Move data from landing to bronze with basic metadata
    """
    try:
        minio_client = create_minio_client()
        
        # Get filename from previous task
        filename = context['task_instance'].xcom_pull(task_ids='ingest_sample_data', key='filename')
        
        if not filename:
            # If no filename from previous task, get the latest file from landing
            objects = list(minio_client.list_objects('landing'))
            if not objects:
                raise Exception("No files found in landing bucket")
            filename = max(objects, key=lambda x: x.last_modified).object_name
        
        logger.info(f"Processing {filename} to bronze layer")
        
        # Get object from landing
        response = minio_client.get_object('landing', filename)
        data = response.read()
        
        # Add metadata (in real scenario, you'd add more sophisticated metadata)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        bronze_filename = f"bronze_{timestamp}_{filename}"
        
        # Upload to bronze bucket
        minio_client.put_object('bronze', bronze_filename, data, len(data))
        
        logger.info(f"✅ Data processed to bronze layer: {bronze_filename}")
        
        # Store bronze filename for next task
        context['task_instance'].xcom_push(key='bronze_filename', value=bronze_filename)
        
    except Exception as e:
        logger.error(f"❌ Bronze processing failed: {str(e)}")
        raise

def simple_spark_processing(**context):
    """
    Simple data processing (simulated Spark job)
    """
    try:
        minio_client = create_minio_client()
        
        # Get bronze filename from previous task
        bronze_filename = context['task_instance'].xcom_pull(task_ids='process_to_bronze', key='bronze_filename')
        
        if not bronze_filename:
            # Get latest bronze file
            objects = list(minio_client.list_objects('bronze'))
            if not objects:
                raise Exception("No files found in bronze bucket")
            bronze_filename = max(objects, key=lambda x: x.last_modified).object_name
        
        logger.info(f"Processing {bronze_filename} with Spark-like transformation")
        
        # Get data from bronze
        response = minio_client.get_object('bronze', bronze_filename)
        data = response.read().decode('utf-8')
        
        # Simple data transformation (in real scenario, this would be done by Spark)
        lines = data.strip().split('\n')
        header = lines[0]
        
        # Add a computed column (salary_category)
        new_header = header + ",salary_category"
        processed_lines = [new_header]
        
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) >= 4:
                try:
                    salary = int(parts[3])
                    if salary < 55000:
                        category = "Low"
                    elif salary < 65000:
                        category = "Medium"
                    else:
                        category = "High"
                    processed_lines.append(line + f",{category}")
                except ValueError:
                    processed_lines.append(line + ",Unknown")
        
        processed_data = '\n'.join(processed_lines)
        
        # Save to silver bucket
        silver_filename = bronze_filename.replace('bronze_', 'silver_')
        minio_client.put_object('silver', silver_filename, processed_data.encode('utf-8'), len(processed_data.encode('utf-8')))
        
        logger.info(f"✅ Data processed to silver layer: {silver_filename}")
        
        # Store silver filename for next task
        context['task_instance'].xcom_push(key='silver_filename', value=silver_filename)
        
    except Exception as e:
        logger.error(f"❌ Spark processing failed: {str(e)}")
        raise

def create_analytics_data(**context):
    """
    Create analytics-ready data in gold layer
    """
    try:
        minio_client = create_minio_client()
        
        # Get silver filename from previous task
        silver_filename = context['task_instance'].xcom_pull(task_ids='simple_spark_processing', key='silver_filename')
        
        if not silver_filename:
            # Get latest silver file
            objects = list(minio_client.list_objects('silver'))
            if not objects:
                raise Exception("No files found in silver bucket")
            silver_filename = max(objects, key=lambda x: x.last_modified).object_name
        
        logger.info(f"Creating analytics data from {silver_filename}")
        
        # Get data from silver
        response = minio_client.get_object('silver', silver_filename)
        data = response.read().decode('utf-8')
        
        # Create analytics summary
        lines = data.strip().split('\n')
        
        # Simple analytics: count by department and salary category
        dept_count = {}
        salary_category_count = {}
        total_salary_by_dept = {}
        
        for line in lines[1:]:  # Skip header
            parts = line.split(',')
            if len(parts) >= 6:
                dept = parts[2]
                salary = int(parts[3]) if parts[3].isdigit() else 0
                category = parts[5]
                
                # Count by department
                dept_count[dept] = dept_count.get(dept, 0) + 1
                
                # Count by salary category
                salary_category_count[category] = salary_category_count.get(category, 0) + 1
                
                # Total salary by department
                total_salary_by_dept[dept] = total_salary_by_dept.get(dept, 0) + salary
        
        # Create analytics report
        analytics_report = f"""Analytics Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
====================================================

Department Summary:
"""
        
        for dept, count in dept_count.items():
            avg_salary = total_salary_by_dept.get(dept, 0) / count if count > 0 else 0
            analytics_report += f"- {dept}: {count} employees, Average Salary: ${avg_salary:,.2f}\n"
        
        analytics_report += f"""
Salary Category Distribution:
"""
        for category, count in salary_category_count.items():
            analytics_report += f"- {category}: {count} employees\n"
        
        analytics_report += f"""
Total Employees: {sum(dept_count.values())}
Total Payroll: ${sum(total_salary_by_dept.values()):,.2f}
"""
        
        # Save to gold bucket
        gold_filename = f"analytics_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        minio_client.put_object('gold', gold_filename, analytics_report.encode('utf-8'), len(analytics_report.encode('utf-8')))
        
        logger.info(f"✅ Analytics data created in gold layer: {gold_filename}")
        logger.info("Analytics Summary:")
        logger.info(analytics_report)
        
    except Exception as e:
        logger.error(f"❌ Analytics creation failed: {str(e)}")
        raise

def pipeline_summary(**context):
    """
    Provide pipeline execution summary
    """
    try:
        minio_client = create_minio_client()
        
        # Count objects in each bucket
        buckets = ['landing', 'bronze', 'silver', 'gold']
        summary = "📊 Pipeline Execution Summary\n" + "="*50 + "\n"
        
        for bucket in buckets:
            try:
                objects = list(minio_client.list_objects(bucket))
                count = len(objects)
                summary += f"{bucket.capitalize()}: {count} objects\n"
                
                if objects:
                    latest = max(objects, key=lambda x: x.last_modified)
                    summary += f"  Latest: {latest.object_name} ({latest.last_modified})\n"
            except Exception as e:
                summary += f"{bucket.capitalize()}: Error - {str(e)}\n"
        
        summary += f"\nPipeline completed at: {datetime.now()}\n"
        summary += "="*50
        
        logger.info(summary)
        
        logger.info("✅ Pipeline execution completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline summary failed: {str(e)}")
        raise

# Task definitions
check_services_task = PythonOperator(
    task_id='check_services',
    python_callable=check_services,
    dag=dag,
)

ingest_task = PythonOperator(
    task_id='ingest_sample_data',
    python_callable=ingest_sample_data,
    dag=dag,
)

bronze_task = PythonOperator(
    task_id='process_to_bronze',
    python_callable=process_to_bronze,
    dag=dag,
)

spark_task = PythonOperator(
    task_id='simple_spark_processing',
    python_callable=simple_spark_processing,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='create_analytics_data',
    python_callable=create_analytics_data,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='pipeline_summary',
    python_callable=pipeline_summary,
    dag=dag,
)

# Task dependencies
check_services_task >> ingest_task >> bronze_task >> spark_task >> gold_task >> summary_task
