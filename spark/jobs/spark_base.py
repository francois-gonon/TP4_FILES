#!/usr/bin/env python3
"""
Base Spark Job Configuration
Provides common functionality for all Spark jobs in the medallion architecture
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
import os

class SparkJobBase:
    """Base class for all Spark jobs with common configuration and utilities"""
    
    def __init__(self, app_name: str, master_url: str = "spark://spark-master:7077"):
        self.app_name = app_name
        self.master_url = master_url
        self.spark = None
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(self.app_name)
    
    def create_spark_session(self):
        """Create and configure Spark session with MinIO S3 support"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master_url) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info(f"Spark session created successfully for {self.app_name}")
            return self.spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def read_json_data(self, bucket: str, file_name: str):
        """Read JSON data from MinIO bucket"""
        try:
            path = f"s3a://{bucket}/{file_name}"
            self.logger.info(f"Reading JSON data from {path}")
            
            # For complex nested JSON, read as text first then parse
            df = self.spark.read.text(path)
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read JSON data from {path}: {str(e)}")
            # Fallback to local file system
            local_path = f"/opt/bitnami/spark/data/{file_name}"
            if os.path.exists(local_path):
                self.logger.info(f"Fallback: reading from local path {local_path}")
                return self.spark.read.text(local_path)
            raise
    
    def read_csv_data(self, bucket: str, file_name: str, **options):
        """Read CSV data from MinIO bucket"""
        try:
            path = f"s3a://{bucket}/{file_name}"
            self.logger.info(f"Reading CSV data from {path}")
            
            default_options = {
                "header": True,
                "inferSchema": True,
                "sep": ",",
                "quote": '"',
                "escape": '"',
                "multiline": True
            }
            default_options.update(options)
            
            df = self.spark.read.options(**default_options).csv(path)
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CSV data from {path}: {str(e)}")
            raise
    
    def read_parquet_data(self, bucket: str, file_name: str):
        """Read Parquet data from MinIO bucket"""
        try:
            path = f"s3a://{bucket}/{file_name}"
            self.logger.info(f"Reading Parquet data from {path}")
            df = self.spark.read.parquet(path)
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read Parquet data from {path}: {str(e)}")
            raise
    
    def write_parquet_data(self, df, bucket: str, file_name: str, mode: str = "overwrite"):
        """Write DataFrame as Parquet to MinIO bucket"""
        try:
            path = f"s3a://{bucket}/{file_name}"
            self.logger.info(f"Writing Parquet data to {path}")
            
            df.write \
                .mode(mode) \
                .option("compression", "snappy") \
                .parquet(path)
            
            self.logger.info(f"Successfully wrote {df.count()} records to {path}")
            
        except Exception as e:
            self.logger.error(f"Failed to write Parquet data to {path}: {str(e)}")
            raise
    
    def write_json_data(self, df, bucket: str, file_name: str, mode: str = "overwrite"):
        """Write DataFrame as JSON to MinIO bucket"""
        try:
            path = f"s3a://{bucket}/{file_name}"
            self.logger.info(f"Writing JSON data to {path}")
            
            df.write \
                .mode(mode) \
                .option("compression", "gzip") \
                .json(path)
            
            self.logger.info(f"Successfully wrote data to {path}")
            
        except Exception as e:
            self.logger.error(f"Failed to write JSON data to {path}: {str(e)}")
            raise
    
    def log_dataframe_info(self, df, name: str):
        """Log DataFrame information for debugging"""
        try:
            count = df.count()
            self.logger.info(f"DataFrame '{name}': {count} rows")
            self.logger.info(f"Schema for '{name}':")
            df.printSchema()
            
            if count > 0:
                self.logger.info(f"Sample data for '{name}':")
                df.show(5, truncate=False)
                
        except Exception as e:
            self.logger.warning(f"Could not log info for DataFrame '{name}': {str(e)}")
    
    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")

    def run(self):
        """Abstract method to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement the run method")

if __name__ == "__main__":
    # This is a base class, not meant to be run directly
    print("SparkJobBase is a base class. Use specific job implementations.")
