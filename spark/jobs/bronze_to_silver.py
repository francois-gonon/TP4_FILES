#!/usr/bin/env python3
"""
Bronze to Silver Spark Transformation
Cleans and validates bronze data, filters for 2022 contracts and active establishments
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

class BronzeToSilverJob(SparkJobBase):
    """Spark job for Bronze to Silver transformation"""
    
    def __init__(self):
        super().__init__("BronzeToSilver")
    
    def clean_marches_data(self):
        """Clean and filter marches data for 2022"""
        try:
            self.logger.info("Cleaning marches data...")
            
            # Read bronze marches data
            marches_df = self.read_parquet_data("bronze", "marches_publics")
            
            self.log_dataframe_info(marches_df, "bronze_marches")
            
            # Parse dates and filter for 2022
            marches_clean = marches_df \
                .withColumn("dateNotification_parsed", 
                           to_date(col("dateNotification"), "yyyy-MM-dd")) \
                .withColumn("year_notification", 
                           year(col("dateNotification_parsed"))) \
                .withColumn("month_notification", 
                           month(col("dateNotification_parsed"))) \
                .filter(col("year_notification") == 2022) \
                .withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("layer", lit("silver"))
            
            # Data quality checks
            total_before = marches_df.count()
            total_after = marches_clean.count()
            
            self.logger.info(f"Marches data: {total_before} -> {total_after} (filtered for 2022)")
            
            self.log_dataframe_info(marches_clean, "cleaned_marches_2022")
            
            # Write to silver layer
            self.write_parquet_data(marches_clean, "silver", "marches_publics_2022")
            
            return total_after
            
        except Exception as e:
            self.logger.error(f"Error cleaning marches data: {str(e)}")
            raise
    
    def clean_etablissements_data(self):
        """Clean and filter etablissements data for active establishments"""
        try:
            self.logger.info("Cleaning etablissements data...")
            
            # Read bronze etablissements data
            etab_df = self.read_parquet_data("bronze", "etablissements")
            
            self.log_dataframe_info(etab_df, "bronze_etablissements")
            
            # Filter for active establishments
            etab_clean = etab_df
            
            # Check if we have the status column
            if "etatAdministratifEtablissement" in etab_df.columns:
                etab_clean = etab_clean.filter(col("etatAdministratifEtablissement") == "A")
            
            # Add processing metadata
            etab_clean = etab_clean \
                .withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("layer", lit("silver"))
            
            # Data quality checks
            total_before = etab_df.count()
            total_after = etab_clean.count()
            
            self.logger.info(f"Etablissements data: {total_before} -> {total_after} (active only)")
            
            self.log_dataframe_info(etab_clean, "cleaned_etablissements_active")
            
            # Write to silver layer
            self.write_parquet_data(etab_clean, "silver", "etablissements_actifs")
            
            return total_after
            
        except Exception as e:
            self.logger.error(f"Error cleaning etablissements data: {str(e)}")
            raise
    
    def clean_naf_codes_data(self):
        """Clean NAF codes reference data"""
        try:
            self.logger.info("Cleaning NAF codes data...")
            
            # Read bronze NAF codes data
            naf_df = self.read_parquet_data("bronze", "codes_naf")
            
            self.log_dataframe_info(naf_df, "bronze_naf_codes")
            
            # Basic cleaning - remove nulls and duplicates
            naf_clean = naf_df \
                .filter(col("code_naf").isNotNull()) \
                .dropDuplicates(["code_naf"]) \
                .withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("layer", lit("silver"))
            
            # Data quality checks
            total_before = naf_df.count()
            total_after = naf_clean.count()
            
            self.logger.info(f"NAF codes data: {total_before} -> {total_after} (cleaned)")
            
            self.log_dataframe_info(naf_clean, "cleaned_naf_codes")
            
            # Write to silver layer
            self.write_parquet_data(naf_clean, "silver", "codes_naf")
            
            return total_after
            
        except Exception as e:
            self.logger.error(f"Error cleaning NAF codes data: {str(e)}")
            # Return 0 if NAF codes fail (not critical)
            return 0
    
    def run_data_quality_checks(self):
        """Run comprehensive data quality checks on silver data"""
        try:
            self.logger.info("Running data quality checks...")
            
            # Check marches data
            marches_df = self.read_parquet_data("silver", "marches_publics_2022")
            marches_count = marches_df.count()
            
            # Check for required fields
            required_marches_fields = ["titulaires", "dateNotification", "year_notification"]
            missing_fields = []
            
            for field in required_marches_fields:
                if field not in marches_df.columns:
                    missing_fields.append(f"marches.{field}")
            
            # Check etablissements data
            etab_df = self.read_parquet_data("silver", "etablissements_actifs")
            etab_count = etab_df.count()
            
            # Check NAF codes data
            try:
                naf_df = self.read_parquet_data("silver", "codes_naf")
                naf_count = naf_df.count()
            except:
                naf_count = 0
            
            quality_report = {
                "silver_data_counts": {
                    "marches_2022": marches_count,
                    "etablissements_actifs": etab_count,
                    "naf_codes": naf_count
                },
                "quality_checks": {
                    "missing_fields": missing_fields,
                    "data_completeness": {
                        "marches_with_titulaires": marches_df.filter(col("titulaires").isNotNull()).count(),
                        "marches_with_dates": marches_df.filter(col("dateNotification").isNotNull()).count()
                    }
                },
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Data quality report: {quality_report}")
            
            return quality_report
            
        except Exception as e:
            self.logger.error(f"Error in data quality checks: {str(e)}")
            raise
    
    def run(self):
        """Execute the bronze to silver transformation"""
        try:
            self.logger.info("=== Starting Bronze to Silver Transformation ===")
            
            # Create Spark session
            self.create_spark_session()
            
            # Process each data source
            marches_count = self.clean_marches_data()
            etab_count = self.clean_etablissements_data()
            naf_count = self.clean_naf_codes_data()
            
            # Run data quality checks
            quality_report = self.run_data_quality_checks()
            
            # Create summary
            summary = {
                "job_name": "BronzeToSilver",
                "execution_time": datetime.now().isoformat(),
                "records_processed": {
                    "marches_2022": marches_count,
                    "etablissements_actifs": etab_count,
                    "naf_codes": naf_count
                },
                "total_records": marches_count + etab_count + naf_count,
                "quality_report": quality_report,
                "status": "success"
            }
            
            self.logger.info(f"=== Bronze to Silver Transformation Completed ===")
            self.logger.info(f"Summary: {summary}")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Bronze to Silver transformation failed: {str(e)}")
            raise
        finally:
            self.cleanup()

def main():
    """Main entry point"""
    try:
        job = BronzeToSilverJob()
        result = job.run()
        print(json.dumps(result, indent=2))
        return 0
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
