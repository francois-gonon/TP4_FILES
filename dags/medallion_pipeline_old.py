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

summary_task = PythonOperator(
    task_id='generate_pipeline_summary',
    python_callable=generate_pipeline_summary,
    dag=dag,
)

# Task dependencies - Complete medallion pipeline flow
check_services_task >> check_landing_task >> validate_quality_task
validate_quality_task >> landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task >> summary_task
