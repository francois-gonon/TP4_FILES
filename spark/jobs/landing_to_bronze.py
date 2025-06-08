#!/usr/bin/env python3
"""
Landing to Bronze Spark Transformation
Processes raw data from landing bucket and stores in bronze with metadata
"""

import sys
import os
from datetime import datetime
import json

# Add the jobs directory to Python path
sys.path.append('/opt/bitnami/spark/jobs')
from spark_base import SparkJobBase

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class LandingToBronzeJob(SparkJobBase):
    """Spark job for Landing to Bronze transformation"""
    
    def __init__(self):
        super().__init__("LandingToBronze")
    
    def process_marches_json(self):
        """Process the marches publics JSON data"""
        try:
            self.logger.info("Processing marches publics JSON data...")
            
            # Read JSON as text first due to complex nested structure
            json_text_df = self.read_json_data("landing", "aws-marchespublics-annee-2022.json")
            
            # Parse JSON manually due to complex structure
            json_content = json_text_df.collect()[0]['value']
            data = json.loads(json_content)
            
            # Extract marches array
            if 'marches' in data:
                marches_data = data['marches']
            else:
                marches_data = data if isinstance(data, list) else [data]
            
            self.logger.info(f"Found {len(marches_data)} marches records")
            
            # Convert to Spark DataFrame
            marches_df = self.spark.createDataFrame(marches_data)
            
            # Add metadata columns
            marches_df = marches_df \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit("aws-marchespublics-annee-2022.json")) \
                .withColumn("layer", lit("bronze"))
            
            self.log_dataframe_info(marches_df, "marches_publics")
            
            # Write to bronze layer
            self.write_parquet_data(marches_df, "bronze", "marches_publics")
            
            return marches_df.count()
            
        except Exception as e:
            self.logger.error(f"Error processing marches JSON: {str(e)}")
            raise
    
    def process_etablissements_csv(self):
        """Process the etablissements CSV data"""
        try:
            self.logger.info("Processing etablissements CSV data...")
            
            # Read CSV with error handling for malformed lines
            etab_df = self.read_csv_data(
                "landing", 
                "simulated_etablissements_50000.csv",
                header=True,
                inferSchema=True,
                sep=",",
                multiline=True,
                escape='"'
            )
            
            # Add metadata columns
            etab_df = etab_df \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit("simulated_etablissements_50000.csv")) \
                .withColumn("layer", lit("bronze"))
            
            self.log_dataframe_info(etab_df, "etablissements")
            
            # Write to bronze layer
            self.write_parquet_data(etab_df, "bronze", "etablissements")
            
            return etab_df.count()
            
        except Exception as e:
            self.logger.error(f"Error processing etablissements CSV: {str(e)}")
            raise
    
    def process_naf_codes_csv(self):
        """Process the NAF codes CSV data"""
        try:
            self.logger.info("Processing NAF codes CSV data...")
            
            # Try different separators for NAF codes
            try:
                naf_df = self.read_csv_data(
                    "landing", 
                    "int_courts_naf_rev_2.csv",
                    header=True,
                    inferSchema=True,
                    sep=",",
                    multiline=True
                )
            except:
                # Try semicolon separator
                naf_df = self.read_csv_data(
                    "landing", 
                    "int_courts_naf_rev_2.csv",
                    header=True,
                    inferSchema=True,
                    sep=";",
                    multiline=True
                )
            
            # Add metadata columns
            naf_df = naf_df \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit("int_courts_naf_rev_2.csv")) \
                .withColumn("layer", lit("bronze"))
            
            self.log_dataframe_info(naf_df, "naf_codes")
            
            # Write to bronze layer
            self.write_parquet_data(naf_df, "bronze", "codes_naf")
            
            return naf_df.count()
            
        except Exception as e:
            self.logger.error(f"Error processing NAF codes CSV: {str(e)}")
            # Create empty DataFrame as fallback
            schema = StructType([
                StructField("code_naf", StringType(), True),
                StructField("libelle", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), True),
                StructField("source_file", StringType(), True),
                StructField("layer", StringType(), True)
            ])
            empty_df = self.spark.createDataFrame([], schema)
            self.write_parquet_data(empty_df, "bronze", "codes_naf")
            return 0
    
    def run(self):
        """Execute the landing to bronze transformation"""
        try:
            self.logger.info("=== Starting Landing to Bronze Transformation ===")
            
            # Create Spark session
            self.create_spark_session()
            
            # Process each data source
            marches_count = self.process_marches_json()
            etab_count = self.process_etablissements_csv()
            naf_count = self.process_naf_codes_csv()
            
            # Create summary
            summary = {
                "job_name": "LandingToBronze",
                "execution_time": datetime.now().isoformat(),
                "records_processed": {
                    "marches_publics": marches_count,
                    "etablissements": etab_count,
                    "naf_codes": naf_count
                },
                "total_records": marches_count + etab_count + naf_count,
                "status": "success"
            }
            
            self.logger.info(f"=== Landing to Bronze Transformation Completed ===")
            self.logger.info(f"Summary: {summary}")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Landing to Bronze transformation failed: {str(e)}")
            raise
        finally:
            self.cleanup()

def main():
    """Main entry point"""
    try:
        job = LandingToBronzeJob()
        result = job.run()
        print(json.dumps(result, indent=2))
        return 0
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
