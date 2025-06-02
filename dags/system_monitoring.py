from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from minio import Minio
import requests

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
    'system_monitoring',
    default_args=default_args,
    description='Monitor system health and data pipeline status',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['monitoring', 'health-check'],
)

def check_minio_health(**context):
    """Check MinIO service health"""
    try:
        minio_client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        # List buckets to test connectivity
        buckets = minio_client.list_buckets()
        bucket_names = [bucket.name for bucket in buckets]
        
        expected_buckets = ['landing', 'bronze', 'silver', 'gold']
        missing_buckets = [b for b in expected_buckets if b not in bucket_names]
        
        if missing_buckets:
            print(f"WARNING: Missing buckets: {missing_buckets}")
        else:
            print("✅ All MinIO buckets are present")
        
        # Check bucket contents
        for bucket in expected_buckets:
            if bucket in bucket_names:
                objects = list(minio_client.list_objects(bucket))
                print(f"📊 {bucket} bucket contains {len(objects)} objects")
        
        print("✅ MinIO health check passed")
        
    except Exception as e:
        print(f"❌ MinIO health check failed: {str(e)}")
        raise

def check_spark_health(**context):
    """Check Spark cluster health"""
    try:
        # Check Spark Master UI
        response = requests.get('http://spark-master:8080/json/', timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            alive_workers = data.get('aliveworkers', 0)
            cores = data.get('cores', 0)
            memory = data.get('memory', 0)
            
            print(f"✅ Spark Master is healthy")
            print(f"📊 Active workers: {alive_workers}")
            print(f"📊 Total cores: {cores}")
            print(f"📊 Total memory: {memory}")
            
            if alive_workers < 2:
                print("⚠️  WARNING: Less than 2 workers active")
        else:
            raise Exception(f"Spark Master returned status code: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Spark health check failed: {str(e)}")
        raise

def check_airflow_health(**context):
    """Check Airflow components health"""
    try:
        # This runs within Airflow, so if this task runs, basic Airflow is working
        from airflow.models import DagRun, TaskInstance
        from airflow import settings
        
        session = settings.Session()
        
        # Check recent DAG runs
        recent_runs = session.query(DagRun).filter(
            DagRun.start_date >= datetime.now() - timedelta(hours=24)
        ).count()
        
        # Check recent task instances
        recent_tasks = session.query(TaskInstance).filter(
            TaskInstance.start_date >= datetime.now() - timedelta(hours=24)
        ).count()
        
        print(f"✅ Airflow is healthy")
        print(f"📊 DAG runs in last 24h: {recent_runs}")
        print(f"📊 Task instances in last 24h: {recent_tasks}")
        
        session.close()
        
    except Exception as e:
        print(f"❌ Airflow health check failed: {str(e)}")
        raise

def check_data_pipeline_status(**context):
    """Check the status of data pipeline"""
    try:
        from airflow.models import DagRun
        from airflow import settings
        
        session = settings.Session()
        
        # Check medallion pipeline status
        recent_pipeline_runs = session.query(DagRun).filter(
            DagRun.dag_id == 'medallion_data_pipeline',
            DagRun.start_date >= datetime.now() - timedelta(days=1)
        ).all()
        
        if recent_pipeline_runs:
            latest_run = max(recent_pipeline_runs, key=lambda x: x.start_date)
            print(f"✅ Latest pipeline run: {latest_run.state}")
            print(f"📊 Execution date: {latest_run.execution_date}")
            print(f"📊 Start date: {latest_run.start_date}")
            
            if latest_run.state == 'failed':
                print("❌ Latest pipeline run failed!")
            elif latest_run.state == 'success':
                print("✅ Latest pipeline run successful!")
        else:
            print("⚠️  No recent pipeline runs found")
        
        session.close()
        
    except Exception as e:
        print(f"❌ Pipeline status check failed: {str(e)}")
        raise

def generate_health_report(**context):
    """Generate a comprehensive health report"""
    try:
        print("🔍 SYSTEM HEALTH REPORT")
        print("=" * 50)
        
        # Get task instances from this DAG run
        dag_run = context['dag_run']
        task_instances = dag_run.get_task_instances()
        
        # Check if all health checks passed
        health_checks = ['check_minio_health', 'check_spark_health', 'check_airflow_health']
        failed_checks = []
        
        for ti in task_instances:
            if ti.task_id in health_checks and ti.state == 'failed':
                failed_checks.append(ti.task_id)
        
        if failed_checks:
            print(f"❌ Failed health checks: {failed_checks}")
            print("🚨 SYSTEM HEALTH: DEGRADED")
        else:
            print("✅ All health checks passed")
            print("🎯 SYSTEM HEALTH: GOOD")
        
        print("=" * 50)
        print(f"📅 Report generated at: {datetime.now()}")
        
    except Exception as e:
        print(f"❌ Health report generation failed: {str(e)}")
        raise

# Task definitions
minio_health_task = PythonOperator(
    task_id='check_minio_health',
    python_callable=check_minio_health,
    dag=dag,
)

spark_health_task = PythonOperator(
    task_id='check_spark_health',
    python_callable=check_spark_health,
    dag=dag,
)

airflow_health_task = PythonOperator(
    task_id='check_airflow_health',
    python_callable=check_airflow_health,
    dag=dag,
)

pipeline_status_task = PythonOperator(
    task_id='check_data_pipeline_status',
    python_callable=check_data_pipeline_status,
    dag=dag,
)

health_report_task = PythonOperator(
    task_id='generate_health_report',
    python_callable=generate_health_report,
    dag=dag,
    trigger_rule='all_done',  # Run even if some health checks fail
)

# System resource check
system_resources_task = BashOperator(
    task_id='check_system_resources',
    bash_command="""
    echo "🖥️  SYSTEM RESOURCES CHECK"
    echo "=========================="
    echo "Docker containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(minio|airflow|spark|postgres|redis)"
    echo ""
    echo "Container resource usage:"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | head -10
    """,
    dag=dag,
)

# Task dependencies
[minio_health_task, spark_health_task, airflow_health_task, pipeline_status_task, system_resources_task] >> health_report_task
