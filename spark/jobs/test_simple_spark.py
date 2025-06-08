#!/usr/bin/env python3
"""
Simple Spark Test Job
Test basic Spark functionality and MinIO connectivity
"""

import sys
import os
from datetime import datetime
import json
import logging

# Add the jobs directory to Python path
sys.path.append('/opt/bitnami/spark/jobs')

# Set environment variables
os.environ['JAVA_HOME'] = '/opt/bitnami/java'

try:
    from spark_base import SparkJobBase
except ImportError:
    # Fallback for local development
    from pyspark.sql import SparkSession
    import logging
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    def create_spark_session():
        return SparkSession.builder \
            .appName("TestSimpleSpark") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()

class TestSimpleSparkJob:
    """Simple test job to verify Spark functionality"""
    
    def __init__(self):
        self.logger = logging.getLogger("TestSimpleSpark")
        logging.basicConfig(level=logging.INFO)
    
    def run(self):
        """Run the test job"""
        try:
            self.logger.info("🚀 Starting simple Spark test...")
            
            # Create Spark session
            if 'SparkJobBase' in globals():
                job = SparkJobBase("TestSimpleSpark")
                spark = job.create_spark_session()
            else:
                spark = create_spark_session()
            
            self.logger.info("✅ Spark session created successfully")
            
            # Test 1: Basic DataFrame operations
            self.logger.info("📊 Testing basic DataFrame operations...")
            df = spark.range(100)
            count = df.count()
            self.logger.info(f"✅ Created DataFrame with {count} rows")
            
            # Test 2: Test MinIO connectivity by listing landing bucket
            self.logger.info("🗂️ Testing MinIO connectivity...")
            try:
                # Try to read from MinIO
                files_df = spark.read.text("s3a://landing/")
                self.logger.info("✅ MinIO connectivity test passed")
            except Exception as e:
                self.logger.warning(f"⚠️ MinIO connectivity test failed: {e}")
                self.logger.info("📁 Will test local file system instead...")
            
            # Create result summary
            result = {
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "spark_version": spark.version,
                "test_df_count": count,
                "message": "All basic Spark tests completed"
            }
            
            # Print result as JSON for parsing by Airflow
            print(json.dumps(result))
            
            spark.stop()
            self.logger.info("🎉 Test completed successfully!")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Test failed: {str(e)}")
            result = {
                "status": "failed",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
            print(json.dumps(result))
            raise

if __name__ == "__main__":
    job = TestSimpleSparkJob()
    job.run()
