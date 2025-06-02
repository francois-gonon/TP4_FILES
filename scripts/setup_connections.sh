#!/bin/bash

# Script to set up Airflow connections for MinIO and other services

echo "Setting up Airflow connections..."

# MinIO S3 Connection
docker exec airflow-webserver airflow connections add 'minio_s3' \
    --conn-type 'aws' \
    --conn-host 'minio:9000' \
    --conn-login 'minioadmin' \
    --conn-password 'minioadmin123' \
    --conn-extra '{"endpoint_url": "http://minio:9000", "aws_access_key_id": "minioadmin", "aws_secret_access_key": "minioadmin123"}'

# PostgreSQL Connection
docker exec airflow-webserver airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-schema 'airflow' \
    --conn-port 5432

# Spark Connection
docker exec airflow-webserver airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port 7077

# AWS S3 Connection for source data
docker exec airflow-webserver airflow connections add 'aws_s3_source' \
    --conn-type 'aws' \
    --conn-extra '{"region_name": "us-east-1"}'

echo "Airflow connections setup completed!"

# List all connections to verify
echo "Current Airflow connections:"
docker exec airflow-webserver airflow connections list
