from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd
import io

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
    'data_quality_checks',
    default_args=default_args,
    description='Comprehensive data quality checks for medallion architecture',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['data-quality', 'monitoring'],
)

def create_minio_client():
    """Create MinIO client"""
    return Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )

def check_data_freshness(**context):
    """Check if data is fresh (updated within expected timeframe)"""
    try:
        minio_client = create_minio_client()
        
        buckets_to_check = ['bronze', 'silver', 'gold']
        freshness_threshold = timedelta(hours=24)  # Data should be updated within 24 hours
        
        for bucket in buckets_to_check:
            try:
                objects = list(minio_client.list_objects(bucket))
                
                if not objects:
                    print(f"⚠️  {bucket} bucket is empty")
                    continue
                
                # Get the most recent object
                latest_object = max(objects, key=lambda x: x.last_modified)
                age = datetime.now(latest_object.last_modified.tzinfo) - latest_object.last_modified
                
                if age > freshness_threshold:
                    print(f"❌ {bucket} data is stale. Latest update: {latest_object.last_modified}")
                    print(f"   Age: {age}")
                else:
                    print(f"✅ {bucket} data is fresh. Latest update: {latest_object.last_modified}")
                    
            except Exception as e:
                print(f"❌ Error checking {bucket} bucket: {str(e)}")
        
    except Exception as e:
        print(f"❌ Data freshness check failed: {str(e)}")
        raise

def check_data_volume(**context):
    """Check data volume across medallion layers"""
    try:
        minio_client = create_minio_client()
        
        buckets = ['landing', 'bronze', 'silver', 'gold']
        bucket_stats = {}
        
        for bucket in buckets:
            try:
                objects = list(minio_client.list_objects(bucket, recursive=True))
                
                total_size = sum(obj.size for obj in objects if obj.size)
                object_count = len(objects)
                
                bucket_stats[bucket] = {
                    'object_count': object_count,
                    'total_size_mb': round(total_size / (1024 * 1024), 2)
                }
                
                print(f"📊 {bucket}: {object_count} objects, {bucket_stats[bucket]['total_size_mb']} MB")
                
            except Exception as e:
                print(f"❌ Error checking {bucket} bucket: {str(e)}")
                bucket_stats[bucket] = {'error': str(e)}
        
        # Data volume validation rules
        if bucket_stats.get('bronze', {}).get('object_count', 0) == 0:
            print("⚠️  WARNING: Bronze layer is empty")
        
        if bucket_stats.get('silver', {}).get('object_count', 0) == 0:
            print("⚠️  WARNING: Silver layer is empty")
        
        # Check if data is flowing through the pipeline
        bronze_count = bucket_stats.get('bronze', {}).get('object_count', 0)
        silver_count = bucket_stats.get('silver', {}).get('object_count', 0)
        gold_count = bucket_stats.get('gold', {}).get('object_count', 0)
        
        if bronze_count > 0 and silver_count == 0:
            print("⚠️  WARNING: Data stuck in bronze layer")
        
        if silver_count > 0 and gold_count == 0:
            print("⚠️  WARNING: Data not reaching gold layer")
        
        print("✅ Data volume check completed")
        
    except Exception as e:
        print(f"❌ Data volume check failed: {str(e)}")
        raise

def check_data_schema(**context):
    """Check data schema consistency"""
    try:
        minio_client = create_minio_client()
        
        # This is a simplified schema check
        # In production, you would have more sophisticated schema validation
        
        buckets_to_check = ['silver', 'gold']
        
        for bucket in buckets_to_check:
            try:
                objects = list(minio_client.list_objects(bucket))
                
                if not objects:
                    print(f"ℹ️  No objects to check schema in {bucket}")
                    continue
                
                # Sample first object for schema check
                first_object = objects[0]
                
                try:
                    # Try to get object and check if it's readable
                    response = minio_client.get_object(bucket, first_object.object_name)
                    data = response.read(1024)  # Read first 1KB
                    
                    if data:
                        print(f"✅ {bucket} objects are readable")
                        
                        # Additional checks can be added here:
                        # - JSON schema validation
                        # - Parquet schema validation
                        # - CSV header validation
                        
                    else:
                        print(f"⚠️  {bucket} objects appear to be empty")
                        
                except Exception as e:
                    print(f"❌ Cannot read object {first_object.object_name} in {bucket}: {str(e)}")
                    
            except Exception as e:
                print(f"❌ Error checking schema in {bucket}: {str(e)}")
        
        print("✅ Schema check completed")
        
    except Exception as e:
        print(f"❌ Schema check failed: {str(e)}")
        raise

def check_data_integrity(**context):
    """Check data integrity and consistency"""
    try:
        minio_client = create_minio_client()
        
        # Check for data corruption, missing files, etc.
        buckets = ['bronze', 'silver', 'gold']
        
        for bucket in buckets:
            try:
                objects = list(minio_client.list_objects(bucket))
                
                corrupt_objects = []
                zero_size_objects = []
                
                for obj in objects:
                    if obj.size == 0:
                        zero_size_objects.append(obj.object_name)
                    
                    # Additional integrity checks can be added here
                    # - Checksum validation
                    # - File format validation
                    # - Data completeness checks
                
                if zero_size_objects:
                    print(f"⚠️  {bucket} has {len(zero_size_objects)} zero-size objects:")
                    for obj_name in zero_size_objects[:5]:  # Show first 5
                        print(f"   - {obj_name}")
                    if len(zero_size_objects) > 5:
                        print(f"   ... and {len(zero_size_objects) - 5} more")
                else:
                    print(f"✅ {bucket} integrity check passed")
                    
            except Exception as e:
                print(f"❌ Error checking integrity in {bucket}: {str(e)}")
        
        print("✅ Data integrity check completed")
        
    except Exception as e:
        print(f"❌ Data integrity check failed: {str(e)}")
        raise

def generate_data_quality_report(**context):
    """Generate comprehensive data quality report"""
    try:
        print("📋 DATA QUALITY REPORT")
        print("=" * 60)
        
        # Get task instances from this DAG run
        dag_run = context['dag_run']
        task_instances = dag_run.get_task_instances()
        
        # Check which data quality checks passed/failed
        quality_checks = [
            'check_data_freshness',
            'check_data_volume', 
            'check_data_schema',
            'check_data_integrity'
        ]
        
        passed_checks = []
        failed_checks = []
        
        for ti in task_instances:
            if ti.task_id in quality_checks:
                if ti.state == 'success':
                    passed_checks.append(ti.task_id)
                elif ti.state == 'failed':
                    failed_checks.append(ti.task_id)
        
        print(f"✅ Passed checks ({len(passed_checks)}): {passed_checks}")
        print(f"❌ Failed checks ({len(failed_checks)}): {failed_checks}")
        
        # Calculate data quality score
        total_checks = len(quality_checks)
        passed_count = len(passed_checks)
        quality_score = (passed_count / total_checks) * 100 if total_checks > 0 else 0
        
        print(f"📊 Data Quality Score: {quality_score:.1f}%")
        
        if quality_score >= 90:
            print("🎯 DATA QUALITY: EXCELLENT")
        elif quality_score >= 75:
            print("✅ DATA QUALITY: GOOD")
        elif quality_score >= 50:
            print("⚠️  DATA QUALITY: NEEDS ATTENTION")
        else:
            print("❌ DATA QUALITY: POOR")
        
        print("=" * 60)
        print(f"📅 Report generated at: {datetime.now()}")
        
        # Store quality metrics (could be sent to monitoring system)
        context['task_instance'].xcom_push(key='quality_score', value=quality_score)
        context['task_instance'].xcom_push(key='passed_checks', value=len(passed_checks))
        context['task_instance'].xcom_push(key='failed_checks', value=len(failed_checks))
        
    except Exception as e:
        print(f"❌ Quality report generation failed: {str(e)}")
        raise

# Task definitions
freshness_task = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

volume_task = PythonOperator(
    task_id='check_data_volume',
    python_callable=check_data_volume,
    dag=dag,
)

schema_task = PythonOperator(
    task_id='check_data_schema',
    python_callable=check_data_schema,
    dag=dag,
)

integrity_task = PythonOperator(
    task_id='check_data_integrity',
    python_callable=check_data_integrity,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_data_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag,
    trigger_rule='all_done',  # Run even if some checks fail
)

# Task dependencies - all quality checks run in parallel, then generate report
[freshness_task, volume_task, schema_task, integrity_task] >> report_task
