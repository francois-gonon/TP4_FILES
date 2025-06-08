#!/usr/bin/env python3
"""
Lightweight Medallion Architecture Pipeline with Spark Jobs
Minimal DAG for orchestrating the core data pipeline steps
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 8),
    'retries': 0,
}

# DAG definition
with DAG(
    'medallion_spark_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'spark'],
) as dag:

    # Submit landing to bronze Spark job
    landing_to_bronze = BashOperator(
        task_id='landing_to_bronze',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/airflow/jobs/landing_to_bronze.py'
    )

    # Submit bronze to silver Spark job
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/airflow/jobs/bronze_to_silver.py'
    )

    # Submit silver to gold Spark job
    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/airflow/jobs/silver_to_gold.py'
    )

    # Define simple linear workflow
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
