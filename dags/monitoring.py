from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import requests
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
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
dag = DAG(
    'monitoring_pipeline',
    default_args=default_args,
    description='Monitoring and health checks for data pipeline',
    schedule_interval=timedelta(minutes=30),  # Every 30 minutes
    catchup=False,
    tags=['monitoring', 'health-check', 'pipeline'],
)

def create_minio_client():
    """Create MinIO client"""
    return Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )

def check_pipeline_health(**context):
    """Check overall pipeline health"""
    try:
        logger.info("🔍 Starting pipeline health check...")
        
        # Check MinIO
        minio_client = create_minio_client()
        buckets = list(minio_client.list_buckets())
        bucket_names = [bucket.name for bucket in buckets]
        
        expected_buckets = ['landing', 'bronze', 'silver', 'gold']
        missing_buckets = [b for b in expected_buckets if b not in bucket_names]
        
        if missing_buckets:
            logger.warning(f"⚠️ Missing buckets: {missing_buckets}")
        else:
            logger.info("✅ All MinIO buckets are present")
        
        # Check Spark
        try:
            response = requests.get('http://spark:8080/json/', timeout=10)
            if response.status_code == 200:
                logger.info("✅ Spark is healthy")
            else:
                logger.warning(f"⚠️ Spark returned status: {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Spark check failed: {str(e)}")
        
        logger.info("✅ Pipeline health check completed")
        
    except Exception as e:
        logger.error(f"❌ Pipeline health check failed: {str(e)}")
        raise

def check_data_flow(**context):
    """Check data flow through medallion layers"""
    try:
        logger.info("📊 Checking data flow...")
        
        minio_client = create_minio_client()
        
        # Check each layer
        layers = ['landing', 'bronze', 'silver', 'gold']
        data_summary = {}
        
        for layer in layers:
            try:
                objects = list(minio_client.list_objects(layer))
                object_count = len(objects)
                
                if objects:
                    latest_object = max(objects, key=lambda x: x.last_modified)
                    latest_time = latest_object.last_modified
                    total_size = sum(getattr(obj, 'size', 0) for obj in objects)
                else:
                    latest_time = None
                    total_size = 0
                
                data_summary[layer] = {
                    'count': object_count,
                    'latest_time': latest_time,
                    'total_size_kb': round(total_size / 1024, 2) if total_size > 0 else 0
                }
                
                logger.info(f"📁 {layer}: {object_count} objects, {data_summary[layer]['total_size_kb']} KB")
                if latest_time:
                    logger.info(f"   Latest: {latest_time}")
                
            except Exception as e:
                logger.error(f"❌ Error checking {layer}: {str(e)}")
                data_summary[layer] = {'error': str(e)}
        
        # Check for data flow issues
        if data_summary.get('landing', {}).get('count', 0) > 0:
            if data_summary.get('bronze', {}).get('count', 0) == 0:
                logger.warning("⚠️ Data stuck in landing layer")
            elif data_summary.get('silver', {}).get('count', 0) == 0:
                logger.warning("⚠️ Data not reaching silver layer")
            elif data_summary.get('gold', {}).get('count', 0) == 0:
                logger.warning("⚠️ Data not reaching gold layer")
            else:
                logger.info("✅ Data is flowing through all layers")
        else:
            logger.info("ℹ️ No data in pipeline yet")
        
        # Store summary for reporting
        context['task_instance'].xcom_push(key='data_summary', value=data_summary)
        
        logger.info("✅ Data flow check completed")
        
    except Exception as e:
        logger.error(f"❌ Data flow check failed: {str(e)}")
        raise

def generate_simple_report(**context):
    """Generate a simple monitoring report"""
    try:
        logger.info("📋 Generating monitoring report...")
        
        # Get data summary from previous task
        data_summary = context['task_instance'].xcom_pull(
            task_ids='check_data_flow', 
            key='data_summary'
        ) or {}
        
        report = f"""
🔍 SIMPLE PIPELINE MONITORING REPORT
=====================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 Data Layer Summary:
"""
        
        total_objects = 0
        total_size = 0
        
        for layer in ['landing', 'bronze', 'silver', 'gold']:
            layer_data = data_summary.get(layer, {})
            if 'error' in layer_data:
                report += f"❌ {layer.capitalize()}: Error - {layer_data['error']}\n"
            else:
                count = layer_data.get('count', 0)
                size = layer_data.get('total_size_kb', 0)
                latest = layer_data.get('latest_time', 'Never')
                
                report += f"📁 {layer.capitalize()}: {count} objects, {size} KB\n"
                if latest != 'Never':
                    report += f"   Latest update: {latest}\n"
                
                total_objects += count
                total_size += size
        
        report += f"""
📈 Overall Statistics:
- Total objects: {total_objects}
- Total size: {total_size:.2f} KB
- Pipeline status: {'🟢 Active' if total_objects > 0 else '🟡 Idle'}

🎯 Recommendations:
"""
        
        if total_objects == 0:
            report += "- Run the simple_medallion_pipeline DAG to generate test data\n"
        elif data_summary.get('gold', {}).get('count', 0) == 0:
            report += "- Check pipeline execution - no analytics data in gold layer\n"
        else:
            report += "- Pipeline is working normally\n"
        
        report += f"""
=====================================
Next check: {(datetime.now() + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        logger.info(report)
        
        # Store report for external access if needed
        context['task_instance'].xcom_push(key='monitoring_report', value=report)
        
        logger.info("✅ Monitoring report generated")
        
    except Exception as e:
        logger.error(f"❌ Report generation failed: {str(e)}")
        raise

def cleanup_old_data(**context):
    """Optional: Clean up old data to save space"""
    try:
        logger.info("🧹 Checking for old data cleanup...")
        
        minio_client = create_minio_client()
        
        # Define retention policy (keep last 10 objects per bucket)
        retention_count = 10
        
        for bucket in ['landing', 'bronze', 'silver']:  # Don't cleanup gold analytics
            try:
                objects = list(minio_client.list_objects(bucket))
                
                if len(objects) > retention_count:
                    # Sort by modification time, keep newest
                    sorted_objects = sorted(objects, key=lambda x: x.last_modified, reverse=True)
                    objects_to_delete = sorted_objects[retention_count:]
                    
                    logger.info(f"🗑️ Cleaning up {len(objects_to_delete)} old objects from {bucket}")
                    
                    for obj in objects_to_delete:
                        minio_client.remove_object(bucket, obj.object_name)
                        logger.info(f"   Deleted: {obj.object_name}")
                else:
                    logger.info(f"✅ {bucket} bucket within retention limits ({len(objects)}/{retention_count})")
                    
            except Exception as e:
                logger.warning(f"⚠️ Cleanup error in {bucket}: {str(e)}")
        
        logger.info("✅ Cleanup check completed")
        
    except Exception as e:
        logger.warning(f"⚠️ Cleanup failed: {str(e)}")
        # Don't raise - cleanup is optional

# Task definitions
health_check_task = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_pipeline_health,
    dag=dag,
)

data_flow_task = PythonOperator(
    task_id='check_data_flow',
    python_callable=check_data_flow,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_simple_report',
    python_callable=generate_simple_report,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
    trigger_rule='all_done',  # Run even if other tasks fail
)

# Task dependencies
health_check_task >> data_flow_task >> report_task >> cleanup_task
