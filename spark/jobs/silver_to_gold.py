#!/usr/bin/env python3
"""
Silver to Gold Spark Transformation
Creates analytics-ready dataset of companies with 2022 contracts that are still active
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

class SilverToGoldJob(SparkJobBase):
    """Spark job for Silver to Gold transformation - Analytics layer"""
    
    def __init__(self):
        super().__init__("SilverToGold")
    
    def extract_siret_from_titulaires(self, marches_df):
        """Extract SIRET numbers from the complex titulaires JSON structure"""
        try:
            self.logger.info("Extracting SIRET numbers from titulaires...")
            
            # Parse titulaires JSON and extract SIRET
            marches_with_siret = marches_df \
                .withColumn("titulaires_parsed", from_json(col("titulaires"), ArrayType(
                    StructType([
                        StructField("id", StringType(), True),
                        StructField("nom", StringType(), True),
                        StructField("typeIdentifiant", StringType(), True)
                    ])
                ))) \
                .withColumn("siret_list", 
                           expr("filter(titulaires_parsed, x -> x.typeIdentifiant = 'SIRET')")) \
                .withColumn("siret", 
                           when(size(col("siret_list")) > 0, col("siret_list")[0]["id"])
                           .otherwise(lit(None))) \
                .filter(col("siret").isNotNull()) \
                .drop("titulaires_parsed", "siret_list")
            
            siret_count = marches_with_siret.count()
            self.logger.info(f"Extracted {siret_count} contracts with valid SIRET numbers")
            
            return marches_with_siret
            
        except Exception as e:
            self.logger.error(f"Error extracting SIRET from titulaires: {str(e)}")
            raise
    
    def aggregate_contract_statistics(self, marches_df):
        """Aggregate contract statistics by company (SIRET)"""
        try:
            self.logger.info("Aggregating contract statistics by company...")
            
            # Convert montant to numeric, handling different formats
            marches_with_amount = marches_df \
                .withColumn("montant_numeric", 
                           when(col("montant").isNotNull(), 
                                regexp_replace(col("montant"), "[^0-9.]", "").cast("double"))
                           .otherwise(lit(0.0)))
            
            # Aggregate by SIRET
            contract_stats = marches_with_amount \
                .groupBy("siret") \
                .agg(
                    count("*").alias("nombre_contrats"),
                    sum("montant_numeric").alias("montant_total"),
                    avg("montant_numeric").alias("montant_moyen"),
                    max("montant_numeric").alias("montant_max"),
                    min("dateNotification").alias("premier_contrat"),
                    max("dateNotification").alias("dernier_contrat"),
                    collect_set("acheteur.nom").alias("acheteurs"),
                    countDistinct("acheteur.nom").alias("nombre_acheteurs")
                ) \
                .withColumn("montant_total", round(col("montant_total"), 2)) \
                .withColumn("montant_moyen", round(col("montant_moyen"), 2))
            
            stats_count = contract_stats.count()
            self.logger.info(f"Created contract statistics for {stats_count} unique companies")
            
            self.log_dataframe_info(contract_stats, "contract_statistics")
            
            return contract_stats
            
        except Exception as e:
            self.logger.error(f"Error aggregating contract statistics: {str(e)}")
            raise
    
    def join_with_establishments(self, contract_stats):
        """Join contract statistics with active establishment data"""
        try:
            self.logger.info("Joining contract data with establishment information...")
            
            # Read establishment data
            etab_df = self.read_parquet_data("silver", "etablissements_actifs")
            
            # Find the SIRET column in establishments data
            siret_col = None
            for col_name in etab_df.columns:
                if 'siret' in col_name.lower():
                    siret_col = col_name
                    break
            
            if not siret_col:
                raise ValueError("No SIRET column found in establishments data")
            
            self.logger.info(f"Using SIRET column: {siret_col}")
            
            # Join datasets
            joined_df = contract_stats.join(
                etab_df.select(
                    col(siret_col).alias("siret_etab"),
                    "denominationUniteLegale",
                    "activitePrincipaleEtablissement", 
                    "codeCommuneEtablissement",
                    "codePostalEtablissement",
                    "libelleCommuneEtablissement",
                    "etatAdministratifEtablissement"
                ),
                contract_stats.siret == col("siret_etab"),
                "inner"
            ).drop("siret_etab")
            
            companies_count = joined_df.count()
            self.logger.info(f"Successfully matched {companies_count} companies with 2022 contracts that are still active")
            
            self.log_dataframe_info(joined_df, "companies_with_contracts_and_establishments")
            
            return joined_df
            
        except Exception as e:
            self.logger.error(f"Error joining with establishments: {str(e)}")
            raise
    
    def enrich_with_sector_information(self, companies_df):
        """Enrich with NAF sector classification"""
        try:
            self.logger.info("Enriching with sector information...")
            
            # Read NAF codes data
            try:
                naf_df = self.read_parquet_data("silver", "codes_naf")
                
                # Join with NAF codes
                enriched_df = companies_df.join(
                    naf_df.select("code_naf", "libelle").alias("naf"),
                    companies_df.activitePrincipaleEtablissement == col("naf.code_naf"),
                    "left"
                ).select(
                    companies_df["*"],
                    col("naf.libelle").alias("secteur_activite")
                )
                
                self.logger.info("Successfully enriched with sector information")
                
            except Exception as e:
                self.logger.warning(f"Could not enrich with NAF codes: {str(e)}")
                # Continue without sector enrichment
                enriched_df = companies_df.withColumn("secteur_activite", lit("Non disponible"))
            
            return enriched_df
            
        except Exception as e:
            self.logger.error(f"Error enriching with sector information: {str(e)}")
            raise
    
    def create_final_analytical_dataset(self, enriched_df):
        """Create the final analytical dataset with business insights"""
        try:
            self.logger.info("Creating final analytical dataset...")
            
            # Add analytical columns
            final_df = enriched_df \
                .withColumn("analyse_date", current_timestamp()) \
                .withColumn("a_eu_contrats_2022", lit(True)) \
                .withColumn("est_actuellement_actif", lit(True)) \
                .withColumn("montant_moyen_par_contrat", 
                           round(col("montant_total") / col("nombre_contrats"), 2)) \
                .withColumn("categorie_entreprise",
                           when(col("nombre_contrats") >= 10, "Gros contractant")
                           .when(col("nombre_contrats") >= 5, "Contractant régulier")
                           .otherwise("Contractant occasionnel")) \
                .withColumn("categorie_montant",
                           when(col("montant_total") >= 1000000, "Plus d'1M€")
                           .when(col("montant_total") >= 100000, "100K€ - 1M€")
                           .when(col("montant_total") >= 10000, "10K€ - 100K€")
                           .otherwise("Moins de 10K€"))
            
            # Select and order final columns
            final_columns = [
                "siret",
                "denominationUniteLegale",
                "activitePrincipaleEtablissement",
                "secteur_activite",
                "nombre_contrats",
                "montant_total",
                "montant_moyen",
                "montant_moyen_par_contrat",
                "premier_contrat",
                "dernier_contrat",
                "nombre_acheteurs",
                "categorie_entreprise",
                "categorie_montant",
                "codeCommuneEtablissement",
                "codePostalEtablissement", 
                "libelleCommuneEtablissement",
                "etatAdministratifEtablissement",
                "a_eu_contrats_2022",
                "est_actuellement_actif",
                "analyse_date"
            ]
            
            analytical_df = final_df.select(*[col(c) for c in final_columns if c in final_df.columns])
            
            self.log_dataframe_info(analytical_df, "final_analytical_dataset")
            
            return analytical_df
            
        except Exception as e:
            self.logger.error(f"Error creating final analytical dataset: {str(e)}")
            raise
    
    def create_summary_statistics(self, final_df):
        """Create comprehensive summary statistics"""
        try:
            self.logger.info("Creating summary statistics...")
            
            total_companies = final_df.count()
            
            # Basic statistics
            summary_stats = final_df.agg(
                sum("nombre_contrats").alias("total_contrats"),
                sum("montant_total").alias("montant_total_global"),
                avg("montant_moyen").alias("montant_moyen_global"),
                countDistinct("activitePrincipaleEtablissement").alias("nombre_secteurs"),
                countDistinct("codeCommuneEtablissement").alias("nombre_communes")
            ).collect()[0]
            
            # Category distributions
            category_dist = final_df.groupBy("categorie_entreprise").count().collect()
            montant_dist = final_df.groupBy("categorie_montant").count().collect()
            
            # Top companies by contract value
            top_companies = final_df \
                .orderBy(desc("montant_total")) \
                .select("siret", "denominationUniteLegale", "montant_total", "nombre_contrats") \
                .limit(10) \
                .collect()
            
            summary = {
                "analyse_metadata": {
                    "date_analyse": datetime.now().isoformat(),
                    "job_name": "SilverToGold",
                    "objectif": "Identifier les entreprises ayant eu des contrats publics en 2022 et encore actives"
                },
                "resultats_principaux": {
                    "nombre_entreprises_identifiees": total_companies,
                    "nombre_total_contrats_2022": int(summary_stats["total_contrats"]) if summary_stats["total_contrats"] else 0,
                    "montant_total_contrats": float(summary_stats["montant_total_global"]) if summary_stats["montant_total_global"] else 0.0,
                    "montant_moyen_par_entreprise": float(summary_stats["montant_moyen_global"]) if summary_stats["montant_moyen_global"] else 0.0,
                    "nombre_secteurs_representes": int(summary_stats["nombre_secteurs"]) if summary_stats["nombre_secteurs"] else 0,
                    "nombre_communes_implantees": int(summary_stats["nombre_communes"]) if summary_stats["nombre_communes"] else 0
                },
                "repartition_par_categories": {
                    "par_volume_contrats": {cat["categorie_entreprise"]: cat["count"] for cat in category_dist},
                    "par_montant_contrats": {cat["categorie_montant"]: cat["count"] for cat in montant_dist}
                },
                "top_10_entreprises": [
                    {
                        "siret": company["siret"],
                        "denomination": company["denominationUniteLegale"],
                        "montant_total": float(company["montant_total"]) if company["montant_total"] else 0.0,
                        "nombre_contrats": int(company["nombre_contrats"]) if company["nombre_contrats"] else 0
                    }
                    for company in top_companies
                ]
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error creating summary statistics: {str(e)}")
            raise
    
    def run(self):
        """Execute the silver to gold transformation"""
        try:
            self.logger.info("=== Starting Silver to Gold Transformation ===")
            
            # Create Spark session
            self.create_spark_session()
            
            # Load silver data
            marches_df = self.read_parquet_data("silver", "marches_publics_2022")
            
            # Step 1: Extract SIRET numbers from complex JSON structure
            marches_with_siret = self.extract_siret_from_titulaires(marches_df)
            
            # Step 2: Aggregate contract statistics by company
            contract_stats = self.aggregate_contract_statistics(marches_with_siret)
            
            # Step 3: Join with active establishment data
            companies_with_establishments = self.join_with_establishments(contract_stats)
            
            # Step 4: Enrich with sector information
            enriched_companies = self.enrich_with_sector_information(companies_with_establishments)
            
            # Step 5: Create final analytical dataset
            final_dataset = self.create_final_analytical_dataset(enriched_companies)
            
            # Step 6: Save final dataset
            self.write_parquet_data(final_dataset, "gold", "companies_2022_contracts_still_active")
            
            # Step 7: Create and save summary statistics
            summary_stats = self.create_summary_statistics(final_dataset)
            
            # Save summary as JSON
            summary_df = self.spark.createDataFrame([summary_stats], MapType(StringType(), StringType()))
            self.write_json_data(summary_df, "gold", "analysis_summary")
            
            # Final summary
            execution_summary = {
                "job_name": "SilverToGold",
                "execution_time": datetime.now().isoformat(),
                "companies_identified": final_dataset.count(),
                "summary_statistics": summary_stats,
                "status": "success"
            }
            
            self.logger.info(f"=== Silver to Gold Transformation Completed ===")
            self.logger.info(f"🎉 OBJECTIF ATTEINT: {execution_summary['companies_identified']} entreprises identifiées!")
            
            return execution_summary
            
        except Exception as e:
            self.logger.error(f"Silver to Gold transformation failed: {str(e)}")
            raise
        finally:
            self.cleanup()

def main():
    """Main entry point"""
    try:
        job = SilverToGoldJob()
        result = job.run()
        print(json.dumps(result, indent=2))
        return 0
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
