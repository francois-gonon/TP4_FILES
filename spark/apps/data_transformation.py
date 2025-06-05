from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Create Spark session with local file system configuration
    """
    spark = SparkSession.builder \
        .appName("MedallionDataTransformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("INFO")
    
    return spark

# Define schemas for our data
MARCHES_PUBLICS_SCHEMA = StructType([
    StructField("marches", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("uid", StringType(), True),
        StructField("acheteur", StructType([
            StructField("id", StringType(), True),
            StructField("nom", StringType(), True)
        ]), True),
        StructField("nature", StringType(), True),
        StructField("objet", StringType(), True),
        StructField("codeCPV", StringType(), True),
        StructField("procedure", StringType(), True),
        StructField("lieuExecution", StructType([
            StructField("code", StringType(), True),
            StructField("typeCode", StringType(), True),
            StructField("nom", StringType(), True)
        ]), True),
        StructField("dureeMois", StringType(), True),
        StructField("dateNotification", StringType(), True),
        StructField("datePublicationDonnees", StringType(), True),
        StructField("montant", StringType(), True),
        StructField("formePrix", StringType(), True),
        StructField("titulaires", ArrayType(StructType([
            StructField("typeIdentifiant", StringType(), True),
            StructField("id", StringType(), True),
            StructField("denominationSociale", StringType(), True)
        ])), True),
        StructField("modifications", ArrayType(StructType([
            StructField("objetModification", StringType(), True),
            StructField("dateNotificationModification", StringType(), True),
            StructField("datePublicationDonneesModification", StringType(), True),
            StructField("dureeMois", StringType(), True),
            StructField("montant", StringType(), True)
        ])), True)
    ])), True)
])

ETABLISSEMENTS_SCHEMA = StructType([
    StructField("siren", StringType(), True),
    StructField("nic", StringType(), True),
    StructField("siret", StringType(), True),
    StructField("statutDiffusionEtablissement", StringType(), True),
    StructField("dateCreationEtablissement", StringType(), True),
    StructField("trancheEffectifsEtablissement", StringType(), True),
    StructField("anneeEffectifsEtablissement", StringType(), True),
    StructField("activitePrincipaleRegistreMetiersEtablissement", StringType(), True),
    StructField("dateDernierTraitementEtablissement", StringType(), True),
    StructField("etablissementSiege", StringType(), True),
    StructField("nombrePeriodesEtablissement", StringType(), True),
    StructField("complementAdresseEtablissement", StringType(), True),
    StructField("numeroVoieEtablissement", StringType(), True),
    StructField("indiceRepetitionEtablissement", StringType(), True),
    StructField("dernierNumeroVoieEtablissement", StringType(), True),
    StructField("indiceRepetitionDernierNumeroVoieEtablissement", StringType(), True),
    StructField("typeVoieEtablissement", StringType(), True),
    StructField("libelleVoieEtablissement", StringType(), True),
    StructField("codePostalEtablissement", StringType(), True),
    StructField("libelleCommuneEtablissement", StringType(), True),
    StructField("libelleCommuneEtrangerEtablissement", StringType(), True),
    StructField("distributionSpecialeEtablissement", StringType(), True),
    StructField("codeCommuneEtablissement", StringType(), True),
    StructField("codeCedexEtablissement", StringType(), True),
    StructField("libelleCedexEtablissement", StringType(), True),
    StructField("codePaysEtrangerEtablissement", StringType(), True),
    StructField("libellePaysEtrangerEtablissement", StringType(), True),
    StructField("identifiantAdresseEtablissement", StringType(), True),
    StructField("coordonneeLambertAbscisseEtablissement", StringType(), True),
    StructField("coordonneeLambertOrdonneeEtablissement", StringType(), True),
    StructField("complementAdresse2Etablissement", StringType(), True),
    StructField("numeroVoie2Etablissement", StringType(), True),
    StructField("indiceRepetition2Etablissement", StringType(), True),
    StructField("typeVoie2Etablissement", StringType(), True),
    StructField("libelleVoie2Etablissement", StringType(), True),
    StructField("codePostal2Etablissement", StringType(), True),
    StructField("libelleCommune2Etablissement", StringType(), True),
    StructField("libelleCommuneEtranger2Etablissement", StringType(), True),
    StructField("distributionSpeciale2Etablissement", StringType(), True),
    StructField("codeCommune2Etablissement", StringType(), True),
    StructField("codeCedex2Etablissement", StringType(), True),
    StructField("libelleCedex2Etablissement", StringType(), True),
    StructField("codePaysEtranger2Etablissement", StringType(), True),
    StructField("libellePaysEtranger2Etablissement", StringType(), True),
    StructField("dateDebut", StringType(), True),
    StructField("etatAdministratifEtablissement", StringType(), True),
    StructField("enseigne1Etablissement", StringType(), True),
    StructField("enseigne2Etablissement", StringType(), True),
    StructField("enseigne3Etablissement", StringType(), True),
    StructField("denominationUsuelleEtablissement", StringType(), True),
    StructField("activitePrincipaleEtablissement", StringType(), True),
    StructField("nomenclatureActivitePrincipaleEtablissement", StringType(), True),
    StructField("caractereEmployeurEtablissement", StringType(), True)
])

NAF_SCHEMA = StructType([
    StructField("Code", StringType(), True),
    StructField("Intitulés de la  NAF rév. 2, version finale", StringType(), True)
])

def data_quality_checks(df, table_name, layer):
    """
    Perform data quality checks on DataFrame
    """
    logger.info(f"Starting data quality checks for {table_name} in {layer} layer")
    
    # Basic quality metrics
    total_rows = df.count()
    total_columns = len(df.columns)
    
    logger.info(f"{table_name} - Total rows: {total_rows}")
    logger.info(f"{table_name} - Total columns: {total_columns}")
    
    # Check for null values
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        null_counts[col_name] = {"count": null_count, "percentage": null_percentage}
        if null_percentage > 50:
            logger.warning(f"{table_name} - Column {col_name} has {null_percentage:.2f}% null values")
    
    # Check for duplicates (if SIRET exists)
    if "siret" in df.columns:
        duplicate_count = df.groupBy("siret").count().filter(col("count") > 1).count()
        logger.info(f"{table_name} - Duplicate SIRET count: {duplicate_count}")
    
    # Data freshness check (if date columns exist)
    date_columns = [c for c in df.columns if "date" in c.lower()]
    for date_col in date_columns:
        try:
            max_date = df.select(max(col(date_col))).collect()[0][0]
            min_date = df.select(min(col(date_col))).collect()[0][0]
            logger.info(f"{table_name} - {date_col} range: {min_date} to {max_date}")
        except:
            logger.warning(f"Could not analyze date column {date_col}")
    
    return {
        "table_name": table_name,
        "layer": layer,
        "total_rows": total_rows,
        "total_columns": total_columns,
        "null_counts": null_counts,
        "timestamp": datetime.now().isoformat()
    }

def landing_to_bronze_transformation(spark):
    """
    Move data from landing to bronze layer with basic cleaning
    """
    try:
        logger.info("Starting landing to bronze transformation")
        
        # Read public contracts data
        logger.info("Reading public contracts data from landing")
        marches_df = spark.read.json("s3a://landing/aws-marchespublics-annee-2022.json")
        
        # Explode the marches array to get individual contracts
        marches_exploded = marches_df.select(explode(col("marches")).alias("marche"))
        
        # Flatten the structure
        bronze_marches = marches_exploded.select(
            col("marche.id").alias("marche_id"),
            col("marche.uid").alias("marche_uid"),
            col("marche.acheteur.id").alias("acheteur_id"),
            col("marche.acheteur.nom").alias("acheteur_nom"),
            col("marche.nature").alias("nature"),
            col("marche.objet").alias("objet"),
            col("marche.codeCPV").alias("code_cpv"),
            col("marche.procedure").alias("procedure"),
            col("marche.lieuExecution.code").alias("lieu_code"),
            col("marche.lieuExecution.typeCode").alias("lieu_type_code"),
            col("marche.lieuExecution.nom").alias("lieu_nom"),
            col("marche.dureeMois").alias("duree_mois"),
            col("marche.dateNotification").alias("date_notification"),
            col("marche.datePublicationDonnees").alias("date_publication"),
            col("marche.montant").alias("montant"),
            col("marche.formePrix").alias("forme_prix"),
            col("marche.titulaires").alias("titulaires"),
            col("marche.modifications").alias("modifications")
        ).withColumn("bronze_ingestion_timestamp", current_timestamp())
        
        # Data quality checks
        quality_report_marches = data_quality_checks(bronze_marches, "marches_publics", "bronze")
        
        # Write to bronze layer
        logger.info("Writing public contracts to bronze layer")
        bronze_marches.write \
            .mode("overwrite") \
            .parquet("s3a://bronze/marches_publics")
        
        # Read establishments data
        logger.info("Reading establishments data from landing")
        etablissements_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ",") \
            .csv("s3a://landing/simulated_etablissements_50000.csv")
        
        # Add bronze layer metadata
        bronze_etablissements = etablissements_df.withColumn("bronze_ingestion_timestamp", current_timestamp())
        
        # Data quality checks
        quality_report_etablissements = data_quality_checks(bronze_etablissements, "etablissements", "bronze")
        
        # Write to bronze layer
        logger.info("Writing establishments to bronze layer")
        bronze_etablissements.write \
            .mode("overwrite") \
            .parquet("s3a://bronze/etablissements")
        
        # Read NAF codes data
        logger.info("Reading NAF codes from landing")
        naf_df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .csv("s3a://landing/int_courts_naf_rev_2.csv")
        
        # Clean NAF data
        bronze_naf = naf_df.withColumnRenamed("Code", "code_naf") \
            .withColumnRenamed("Intitulés de la  NAF rév. 2, version finale", "libelle_naf") \
            .withColumn("bronze_ingestion_timestamp", current_timestamp())
        
        # Data quality checks
        quality_report_naf = data_quality_checks(bronze_naf, "codes_naf", "bronze")
        
        # Write to bronze layer
        logger.info("Writing NAF codes to bronze layer")
        bronze_naf.write \
            .mode("overwrite") \
            .parquet("s3a://bronze/codes_naf")
        
        logger.info("Landing to bronze transformation completed successfully")
        
        return {
            "marches_count": bronze_marches.count(),
            "etablissements_count": bronze_etablissements.count(),
            "naf_count": bronze_naf.count(),
            "quality_reports": [quality_report_marches, quality_report_etablissements, quality_report_naf]
        }
        
    except Exception as e:
        logger.error(f"Error in landing to bronze transformation: {str(e)}")
        raise

def bronze_to_silver_transformation(spark):
    """
    Transform data from bronze to silver layer with business logic and validation
    """
    try:
        logger.info("Starting bronze to silver transformation")
        
        # Read bronze data
        marches_df = spark.read.parquet("s3a://bronze/marches_publics")
        etablissements_df = spark.read.parquet("s3a://bronze/etablissements")
        naf_df = spark.read.parquet("s3a://bronze/codes_naf")
        
        # Transform public contracts data
        logger.info("Transforming public contracts data")
        
        # Extract and clean titulaires (contractors)
        marches_with_titulaires = marches_df.select(
            col("marche_id"),
            col("marche_uid"),
            col("acheteur_id"),
            col("acheteur_nom"),
            col("nature"),
            col("objet"),
            col("code_cpv"),
            col("procedure"),
            col("lieu_code"),
            col("lieu_nom"),
            col("duree_mois").cast(IntegerType()).alias("duree_mois"),
            col("date_notification"),
            col("date_publication"),
            col("montant").cast(DoubleType()).alias("montant"),
            col("forme_prix"),
            explode_outer(col("titulaires")).alias("titulaire")
        )
        
        # Extract titulaire details
        silver_marches = marches_with_titulaires.select(
            col("marche_id"),
            col("marche_uid"),
            col("acheteur_id"),
            col("acheteur_nom"),
            col("nature"),
            col("objet"),
            col("code_cpv"),
            col("procedure"),
            col("lieu_code"),
            col("lieu_nom"),
            col("duree_mois"),
            to_timestamp(regexp_replace(col("date_notification"), r"\+\d{2}:\d{2}$", ""), "yyyy-MM-dd'T'HH:mm:ss").alias("date_notification"),
            to_timestamp(regexp_replace(col("date_publication"), r"\+\d{2}:\d{2}$", ""), "yyyy-MM-dd'T'HH:mm:ss").alias("date_publication"),
            col("montant"),
            col("forme_prix"),
            col("titulaire.typeIdentifiant").alias("titulaire_type_id"),
            col("titulaire.id").alias("titulaire_siret"),
            col("titulaire.denominationSociale").alias("titulaire_nom")
        ).filter(col("titulaire_siret").isNotNull()) \
         .withColumn("silver_processing_timestamp", current_timestamp()) \
         .withColumn("year_notification", year(col("date_notification"))) \
         .withColumn("month_notification", month(col("date_notification")))
        
        # Transform establishments data
        logger.info("Transforming establishments data")
        
        # Clean and standardize establishments data
        silver_etablissements = etablissements_df.select(
            col("siren"),
            col("nic"),
            col("siret"),
            col("statutDiffusionEtablissement").alias("statut_diffusion"),
            to_date(col("dateCreationEtablissement"), "yyyy-MM-dd").alias("date_creation"),
            col("trancheEffectifsEtablissement").alias("tranche_effectifs"),
            col("anneeEffectifsEtablissement").alias("annee_effectifs"),
            to_timestamp(col("dateDernierTraitementEtablissement"), "yyyy-MM-dd'T'HH:mm:ss").alias("date_dernier_traitement"),
            when(col("etablissementSiege") == "true", True).otherwise(False).alias("est_siege"),
            col("numeroVoieEtablissement").alias("numero_voie"),
            col("typeVoieEtablissement").alias("type_voie"),
            col("libelleVoieEtablissement").alias("libelle_voie"),
            col("codePostalEtablissement").alias("code_postal"),
            col("libelleCommuneEtablissement").alias("libelle_commune"),
            col("codeCommuneEtablissement").alias("code_commune"),
            to_date(col("dateDebut"), "yyyy-MM-dd").alias("date_debut_activite"),
            col("etatAdministratifEtablissement").alias("etat_administratif"),
            col("enseigne1Etablissement").alias("enseigne"),
            col("denominationUsuelleEtablissement").alias("denomination_usuelle"),
            col("activitePrincipaleEtablissement").alias("activite_principale"),
            col("nomenclatureActivitePrincipaleEtablissement").alias("nomenclature_activite"),
            when(col("caractereEmployeurEtablissement") == "O", True).otherwise(False).alias("est_employeur")
        ).withColumn("silver_processing_timestamp", current_timestamp()) \
         .withColumn("est_actif", when(col("etat_administratif") == "A", True).otherwise(False))
        
        # Clean NAF codes
        logger.info("Transforming NAF codes data")
        silver_naf = naf_df.select(
            col("code_naf"),
            col("libelle_naf")
        ).withColumn("silver_processing_timestamp", current_timestamp())
        
        # Data quality checks
        quality_report_marches = data_quality_checks(silver_marches, "marches_publics", "silver")
        quality_report_etablissements = data_quality_checks(silver_etablissements, "etablissements", "silver")
        quality_report_naf = data_quality_checks(silver_naf, "codes_naf", "silver")
          # Write to silver layer
        logger.info("Writing transformed data to silver layer")
        
        silver_marches.write \
            .mode("overwrite") \
            .partitionBy("year_notification", "month_notification") \
            .parquet("s3a://silver/marches_publics")
        
        silver_etablissements.write \
            .mode("overwrite") \
            .partitionBy("etat_administratif") \
            .parquet("s3a://silver/etablissements")
        
        silver_naf.write \
            .mode("overwrite") \
            .parquet("s3a://silver/codes_naf")
        
        logger.info("Bronze to silver transformation completed successfully")
        
        return {
            "silver_marches_count": silver_marches.count(),
            "silver_etablissements_count": silver_etablissements.count(),
            "silver_naf_count": silver_naf.count(),
            "quality_reports": [quality_report_marches, quality_report_etablissements, quality_report_naf]
        }
    
    except Exception as e:
        logger.error(f"Error in bronze to silver transformation: {str(e)}")
        raise

def silver_to_gold_transformation(spark):
    """
    Transform data from silver to gold layer - Create analytics-ready datasets
    Focus: Companies that had public contracts in 2022 and are still active today
    """
    try:
        logger.info("Starting silver to gold transformation")
        
        # Read silver data
        marches_df = spark.read.parquet("s3a://silver/marches_publics")
        etablissements_df = spark.read.parquet("s3a://silver/etablissements")
        naf_df = spark.read.parquet("s3a://silver/codes_naf")
        
        # Filter contracts from 2022
        logger.info("Filtering contracts from 2022")
        marches_2022 = marches_df.filter(col("year_notification") == 2022)
        
        # Aggregate contract data by company
        logger.info("Aggregating contract data by company")
        contracts_by_company = marches_2022.groupBy("titulaire_siret", "titulaire_nom") \
            .agg(
                count("marche_id").alias("nombre_contrats"),
                sum("montant").alias("montant_total_contrats"),
                avg("montant").alias("montant_moyen_contrat"),
                min("date_notification").alias("premier_contrat_2022"),
                max("date_notification").alias("dernier_contrat_2022"),
                countDistinct("acheteur_id").alias("nombre_acheteurs_distincts"),
                collect_set("nature").alias("types_marches"),
                collect_set("code_cpv").alias("codes_cpv"),
                collect_set("lieu_nom").alias("lieux_execution")
            )
        
        # Filter active establishments (still active today)
        logger.info("Filtering active establishments")
        etablissements_actifs = etablissements_df.filter(col("est_actif") == True)
        
        # Join with NAF codes for sector information
        logger.info("Enriching establishments with sector information")
        etablissements_enrichis = etablissements_actifs.join(
            naf_df,
            etablissements_actifs.activite_principale == naf_df.code_naf,
            "left"
        ).select(
            etablissements_actifs["*"],
            naf_df.libelle_naf.alias("secteur_activite")
        )
        
        # Create the final analytical dataset
        logger.info("Creating final analytical dataset")
        gold_df = contracts_by_company.join(
            etablissements_enrichis,
            contracts_by_company.titulaire_siret == etablissements_enrichis.siret,
            "inner"  # Only companies that exist in both datasets
        ).select(
            # Company identification
            col("titulaire_siret").alias("siret"),
            col("titulaire_nom").alias("nom_entreprise"),
            col("siren"),
            col("denomination_usuelle"),
            col("enseigne"),
            
            # Contract metrics
            col("nombre_contrats"),
            col("montant_total_contrats"),
            col("montant_moyen_contrat"),
            col("premier_contrat_2022"),
            col("dernier_contrat_2022"),
            col("nombre_acheteurs_distincts"),
            col("types_marches"),
            col("codes_cpv"),
            col("lieux_execution"),
            
            # Company details
            col("date_creation"),
            col("secteur_activite"),
            col("activite_principale").alias("code_naf"),
            col("tranche_effectifs"),
            col("est_siege"),
            col("est_employeur"),
            
            # Address information
            col("numero_voie"),
            col("type_voie"),
            col("libelle_voie"),
            col("code_postal"),
            col("libelle_commune"),
            col("code_commune"),
            
            # Status and dates
            col("etat_administratif"),
            col("date_debut_activite"),
            col("date_dernier_traitement")
        ).withColumn(
            # Calculate company age
            "age_entreprise_annees",
            round(datediff(current_date(), col("date_creation")) / 365.25, 1)
        ).withColumn(
            # Categorize contract volume
            "categorie_volume_contrats",
            when(col("nombre_contrats") >= 10, "Gros contractant")
            .when(col("nombre_contrats") >= 5, "Contractant régulier")
            .when(col("nombre_contrats") >= 2, "Contractant occasionnel")
            .otherwise("Contractant unique")
        ).withColumn(
            # Categorize contract value
            "categorie_valeur_contrats",
            when(col("montant_total_contrats") >= 1000000, "Plus de 1M€")
            .when(col("montant_total_contrats") >= 100000, "100K€ - 1M€")
            .when(col("montant_total_contrats") >= 10000, "10K€ - 100K€")
            .otherwise("Moins de 10K€")
        ).withColumn(
            # Add analysis timestamp
            "analyse_timestamp", current_timestamp()
        ).withColumn(
            "analyse_date", current_date()
        )
        
        # Create summary statistics
        logger.info("Generating summary statistics")
        total_companies = gold_df.count()
        total_contracts = gold_df.agg(sum("nombre_contrats")).collect()[0][0]
        total_value = gold_df.agg(sum("montant_total_contrats")).collect()[0][0]
        
        logger.info(f"Gold layer summary:")
        logger.info(f"- Total active companies with 2022 contracts: {total_companies}")
        logger.info(f"- Total contracts: {total_contracts}")
        logger.info(f"- Total contract value: €{total_value:,.2f}")
        
        # Data quality checks
        quality_report = data_quality_checks(gold_df, "analytical_dataset", "gold")
        
        # Write main analytical dataset
        logger.info("Writing main analytical dataset to gold layer")
        gold_df.write \
            .mode("overwrite") \
            .partitionBy("categorie_volume_contrats") \
            .parquet("s3a://gold/active_companies_with_contracts_2022")
        
        # Create and write summary tables
        logger.info("Creating summary tables")
        
        # Summary by sector
        summary_by_sector = gold_df.groupBy("secteur_activite") \
            .agg(
                count("siret").alias("nombre_entreprises"),
                sum("nombre_contrats").alias("total_contrats"),
                sum("montant_total_contrats").alias("valeur_totale"),
                avg("montant_moyen_contrat").alias("montant_moyen_par_secteur")
            ).orderBy(desc("valeur_totale"))
        
        summary_by_sector.write \
            .mode("overwrite") \
            .parquet("s3a://gold/summary_by_sector")
        
        # Summary by region (using first 2 digits of postal code)
        summary_by_region = gold_df.withColumn("region_code", substring(col("code_postal"), 1, 2)) \
            .groupBy("region_code") \
            .agg(
                count("siret").alias("nombre_entreprises"),
                sum("nombre_contrats").alias("total_contrats"),
                sum("montant_total_contrats").alias("valeur_totale")
            ).orderBy(desc("valeur_totale"))
        
        summary_by_region.write \
            .mode("overwrite") \
            .parquet("s3a://gold/summary_by_region")
        
        # Summary by company size (based on employee range)
        summary_by_size = gold_df.groupBy("tranche_effectifs") \
            .agg(
                count("siret").alias("nombre_entreprises"),
                sum("nombre_contrats").alias("total_contrats"),
                sum("montant_total_contrats").alias("valeur_totale"),
                avg("age_entreprise_annees").alias("age_moyen_entreprises")
            ).orderBy(desc("valeur_totale"))
        
        summary_by_size.write \
            .mode("overwrite") \
            .parquet("s3a://gold/summary_by_company_size")
        
        # Top performing companies
        top_companies = gold_df.select(
            col("siret"),
            col("nom_entreprise"),
            col("secteur_activite"),
            col("nombre_contrats"),
            col("montant_total_contrats"),
            col("nombre_acheteurs_distincts"),
            col("libelle_commune"),
            col("categorie_volume_contrats"),
            col("categorie_valeur_contrats")
        ).orderBy(desc("montant_total_contrats")).limit(100)
        
        top_companies.write \
            .mode("overwrite") \
            .parquet("s3a://gold/top_100_companies")
        
        logger.info("Silver to gold transformation completed successfully")
        
        return {
            "gold_companies_count": total_companies,
            "total_contracts": total_contracts,
            "total_value": total_value,
            "quality_report": quality_report
        }
        
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
        
        # Get pipeline stage from command line arguments
        stage = sys.argv[1] if len(sys.argv) > 1 else "all"
        
        if stage in ["landing_to_bronze", "all"]:
            logger.info("Executing landing to bronze transformation")
            landing_result = landing_to_bronze_transformation(spark)
            logger.info(f"Landing to bronze completed: {landing_result}")
        
        if stage in ["bronze_to_silver", "all"]:
            logger.info("Executing bronze to silver transformation")
            silver_result = bronze_to_silver_transformation(spark)
            logger.info(f"Bronze to silver completed: {silver_result}")
        
        if stage in ["silver_to_gold", "all"]:
            logger.info("Executing silver to gold transformation")
            gold_result = silver_to_gold_transformation(spark)
            logger.info(f"Silver to gold completed: {gold_result}")
        
        logger.info("Data transformation pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main pipeline: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
