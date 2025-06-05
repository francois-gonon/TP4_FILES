#!/usr/bin/env python3

from pyspark.sql import SparkSession
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create simple Spark session"""
    spark = SparkSession.builder \
        .appName("TestSpark") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    return spark

def main():
    try:
        logger.info("Creating Spark session...")
        spark = create_spark_session()
        
        logger.info("Testing basic Spark functionality...")
        # Test basic functionality
        df = spark.range(10)
        count = df.count()
        logger.info(f"Successfully created DataFrame with {count} rows")
        
        # Test file reading
        logger.info("Testing file reading...")
        try:
            # Try to read one of our source files
            json_df = spark.read.json("/opt/bitnami/spark/data/source/aws-marchespublics-annee-2022.json")
            logger.info(f"Successfully read JSON file with {json_df.count()} rows")
        except Exception as e:
            logger.warning(f"Could not read JSON file: {e}")
        
        spark.stop()
        logger.info("Spark test completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in Spark test: {str(e)}")
        raise

if __name__ == "__main__":
    main()
