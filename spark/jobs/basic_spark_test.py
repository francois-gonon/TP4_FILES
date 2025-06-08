#!/usr/bin/env python3
"""
Very Simple Spark Test - No External Dependencies
"""

import logging
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SimpleSparkTest")

def main():
    try:
        logger.info("🚀 Creating basic Spark session...")
        
        # Create a very basic Spark session
        spark = SparkSession.builder \
            .appName("BasicSparkTest") \
            .master("local[1]") \
            .getOrCreate()
        
        logger.info("✅ Spark session created successfully!")
        
        # Create a simple DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)
        
        logger.info("📊 Created DataFrame:")
        df.show()
        
        # Simple transformation
        result = df.filter(df.Age > 28)
        logger.info("📊 Filtered DataFrame (Age > 28):")
        result.show()
        
        logger.info("✅ Basic Spark test completed successfully!")
        
        spark.stop()
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    print(f'{{"status": "{"success" if success else "failed"}"}}')
