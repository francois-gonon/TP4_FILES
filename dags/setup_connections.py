from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.utils.db import provide_session
import json

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
    'setup_connections',
    default_args=default_args,
    description='Setup Airflow connections for the data pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['setup', 'connections'],
)

@provide_session
def create_minio_connection(session=None):
    """Create MinIO S3 connection"""
    try:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == 'minio_s3').first()
        if existing_conn:
            session.delete(existing_conn)
            session.commit()
        
        # Create new connection
        new_conn = Connection(
            conn_id='minio_s3',
            conn_type='aws',
            host='minio:9000',
            login='minioadmin',
            password='minioadmin123',
            extra=json.dumps({
                'endpoint_url': 'http://minio:9000',
                'aws_access_key_id': 'minioadmin',
                'aws_secret_access_key': 'minioadmin123'
            })
        )
        
        session.add(new_conn)
        session.commit()
        print("MinIO S3 connection created successfully")
        
    except Exception as e:
        print(f"Error creating MinIO connection: {str(e)}")
        raise

@provide_session
def create_postgres_connection(session=None):
    """Create PostgreSQL connection"""
    try:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == 'postgres_pipeline').first()
        if existing_conn:
            session.delete(existing_conn)
            session.commit()
        
        # Create new connection
        new_conn = Connection(
            conn_id='postgres_pipeline',
            conn_type='postgres',
            host='postgres',
            login='airflow',
            password='airflow',
            schema='airflow',
            port=5432
        )
        
        session.add(new_conn)
        session.commit()
        print("PostgreSQL connection created successfully")
        
    except Exception as e:
        print(f"Error creating PostgreSQL connection: {str(e)}")
        raise

@provide_session
def create_spark_connection(session=None):
    """Create Spark connection"""
    try:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == 'spark_pipeline').first()
        if existing_conn:
            session.delete(existing_conn)
            session.commit()
        
        # Create new connection
        new_conn = Connection(
            conn_id='spark_pipeline',
            conn_type='spark',
            host='spark://spark-master',
            port=7077
        )
        
        session.add(new_conn)
        session.commit()
        print("Spark connection created successfully")
        
    except Exception as e:
        print(f"Error creating Spark connection: {str(e)}")
        raise

@provide_session
def create_aws_s3_connection(session=None):
    """Create AWS S3 connection for source data"""
    try:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == 'aws_s3_source').first()
        if existing_conn:
            session.delete(existing_conn)
            session.commit()
        
        # Create new connection
        new_conn = Connection(
            conn_id='aws_s3_source',
            conn_type='aws',
            extra=json.dumps({
                'region_name': 'us-east-1'
            })
        )
        
        session.add(new_conn)
        session.commit()
        print("AWS S3 source connection created successfully")
        
    except Exception as e:
        print(f"Error creating AWS S3 connection: {str(e)}")
        raise

def test_connections(**context):
    """Test all created connections"""
    from airflow.hooks.S3_hook import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    try:
        print("Testing connections...")
        
        # Test MinIO connection
        try:
            s3_hook = S3Hook(aws_conn_id='minio_s3')
            buckets = s3_hook.list_buckets()
            print(f"MinIO connection successful. Found buckets: {[b['Name'] for b in buckets]}")
        except Exception as e:
            print(f"MinIO connection test failed: {str(e)}")
        
        # Test PostgreSQL connection
        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_pipeline')
            pg_hook.get_conn()
            print("PostgreSQL connection successful")
        except Exception as e:
            print(f"PostgreSQL connection test failed: {str(e)}")
        
        print("Connection testing completed")
        
    except Exception as e:
        print(f"Error testing connections: {str(e)}")
        raise

# Task definitions
create_minio_conn_task = PythonOperator(
    task_id='create_minio_connection',
    python_callable=create_minio_connection,
    dag=dag,
)

create_postgres_conn_task = PythonOperator(
    task_id='create_postgres_connection',
    python_callable=create_postgres_connection,
    dag=dag,
)

create_spark_conn_task = PythonOperator(
    task_id='create_spark_connection',
    python_callable=create_spark_connection,
    dag=dag,
)

create_aws_s3_conn_task = PythonOperator(
    task_id='create_aws_s3_connection',
    python_callable=create_aws_s3_connection,
    dag=dag,
)

test_connections_task = PythonOperator(
    task_id='test_connections',
    python_callable=test_connections,
    dag=dag,
)

# Task dependencies
[create_minio_conn_task, create_postgres_conn_task, create_spark_conn_task, create_aws_s3_conn_task] >> test_connections_task
