#!/usr/bin/env python3
"""
Medallion Architecture Pipeline with Pandas Transformations Only
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import transformation functions
from pandas_transformation import (
    landing_to_bronze_transformation,
    bronze_to_silver_transformation,
    silver_to_gold_transformation
)

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'max_active_runs': 1,
}

# DAG definition (manual trigger only)
dag = DAG(
    'medallion_pandas_transformation',
    default_args=default_args,
    description='🐼 Medallion Architecture with Pandas - Transformation Only',
    schedule_interval=None,
    catchup=False,
    tags=['medallion', 'pandas', 'transformation']
)

# Tasks
landing_task = PythonOperator(
    task_id='landing_to_bronze',
    python_callable=landing_to_bronze_transformation,
    dag=dag
)

bronze_task = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=bronze_to_silver_transformation,
    dag=dag
)

silver_task = PythonOperator(
    task_id='silver_to_gold',
    python_callable=silver_to_gold_transformation,
    dag=dag
)

# Task dependencies
landing_task >> bronze_task >> silver_task
