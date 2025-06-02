from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Create Spark session with MinIO configuration
    """
    spark = SparkSession.builder \
        .appName("MedallionDataTransformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("INFO")
    
    return spark

def transform_bronze_to_silver(spark):
    """
    Transform data from bronze to silver layer
    """
    try:
        logger.info("Starting bronze to silver transformation")
        
        # Read data from bronze layer (MinIO)
        # Note: This is a sample transformation - adjust based on your actual data
        bronze_path = "s3a://bronze/"
        
        # For demonstration, create a sample DataFrame
        # In real scenarios, you would read from MinIO bronze bucket
        sample_data = [
            (1, "John Doe", "2023-01-01", "Sales", 50000),
            (2, "Jane Smith", "2023-01-02", "Marketing", 60000),
            (3, "Bob Johnson", "2023-01-03", "IT", 70000),
            (4, "Alice Wilson", "2023-01-04", "HR", 55000),
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True)
        ])
        
        df = spark.createDataFrame(sample_data, schema)
        
        # Data transformations for silver layer
        silver_df = df \
            .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
            .withColumn("name_upper", upper(col("name"))) \
            .withColumn("salary_category", 
                       when(col("salary") < 55000, "Low")
                       .when(col("salary") < 65000, "Medium")
                       .otherwise("High")) \
            .withColumn("processed_timestamp", current_timestamp())
        
        # Write to silver layer
        silver_path = "s3a://silver/transformed_data"
        
        logger.info(f"Writing transformed data to {silver_path}")
        
        # For demonstration, show the data
        silver_df.show()
        silver_df.printSchema()
        
        logger.info("Bronze to silver transformation completed successfully")
        
        return silver_df
        
    except Exception as e:
        logger.error(f"Error in bronze to silver transformation: {str(e)}")
        raise

def transform_silver_to_gold(spark, silver_df):
    """
    Transform data from silver to gold layer (analytics ready)
    """
    try:
        logger.info("Starting silver to gold transformation")
        
        # Aggregations and analytics transformations
        gold_df = silver_df \
            .groupBy("department", "salary_category") \
            .agg(
                count("id").alias("employee_count"),
                avg("salary").alias("avg_salary"),
                min("salary").alias("min_salary"),
                max("salary").alias("max_salary")
            ) \
            .withColumn("analysis_date", current_date())
        
        # Write to gold layer
        gold_path = "s3a://gold/analytics_data"
        
        logger.info(f"Writing analytics data to {gold_path}")
        
        # For demonstration, show the data
        gold_df.show()
        gold_df.printSchema()
        
        logger.info("Silver to gold transformation completed successfully")
        
        return gold_df
        
    except Exception as e:
        logger.error(f"Error in silver to gold transformation: {str(e)}")
        raise

def main():
    """
    Main function to execute the data transformation pipeline
    """
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session()
        
        logger.info("Starting Medallion Data Transformation Pipeline")
        
        # Transform bronze to silver
        silver_df = transform_bronze_to_silver(spark)
        
        # Transform silver to gold
        gold_df = transform_silver_to_gold(spark, silver_df)
        
        logger.info("Data transformation pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main pipeline: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
