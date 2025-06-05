#!/usr/bin/env python3
"""
Simplified data transformation pipeline using pandas and MinIO
"""

import pandas as pd
import json
import logging
from minio import Minio
from io import BytesIO, StringIO
import sys
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'

def create_minio_client():
    """Create MinIO client"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def landing_to_bronze_transformation():
    """Transform landing data to bronze layer"""
    try:
        logger.info("Starting landing to bronze transformation...")
        client = create_minio_client()
        
        # Ensure bronze bucket exists
        if not client.bucket_exists("bronze"):
            client.make_bucket("bronze")
        
        # Process JSON file (marches publics)
        logger.info("Processing marches publics JSON file...")
        json_obj = client.get_object("landing", "aws-marchespublics-annee-2022.json")
        json_data = json_obj.read()
          # Parse JSON - handle both JSONL format and standard JSON format
        try:
            # Try parsing as standard JSON first
            data = json.loads(json_data.decode('utf-8'))
            if 'marches' in data:
                marches_data = data['marches']
            else:
                marches_data = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            # Fallback to JSONL format
            marches_data = []
            for line in json_data.decode('utf-8').split('\n'):
                if line.strip():
                    try:
                        marches_data.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        
        marches_df = pd.DataFrame(marches_data)
        logger.info(f"Loaded {len(marches_df)} marches publics records")
        
        # Convert object columns to string to avoid parquet conversion issues
        for col in marches_df.select_dtypes(include=['object']).columns:
            marches_df[col] = marches_df[col].astype(str)
        
        # Save as parquet
        parquet_buffer = BytesIO()
        marches_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        client.put_object(
            "bronze", 
            "marches_publics.parquet",
            parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
          # Process CSV files
        csv_files = [
            ("simulated_etablissements_50000.csv", "etablissements.parquet"),
            ("int_courts_naf_rev_2.csv", "codes_naf.parquet")
        ]
        
        for csv_file, parquet_file in csv_files:
            logger.info(f"Processing {csv_file}...")
            
            try:
                csv_obj = client.get_object("landing", csv_file)
                csv_data = csv_obj.read().decode('utf-8')
                
                # Try different CSV parsing options
                try:
                    df = pd.read_csv(StringIO(csv_data), low_memory=False)
                except pd.errors.ParserError:
                    # Try with different separator or quoting
                    try:
                        df = pd.read_csv(StringIO(csv_data), low_memory=False, sep=';')
                    except pd.errors.ParserError:
                        # Try with error handling
                        df = pd.read_csv(StringIO(csv_data), low_memory=False, error_bad_lines=False, warn_bad_lines=True)
                
                logger.info(f"Loaded {len(df)} records from {csv_file}")
                
                # Convert object columns to string to avoid parquet conversion issues
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str)
                
                # Save as parquet
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                
                client.put_object(
                    "bronze", 
                    parquet_file,
                    parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type='application/octet-stream'
                )
                
            except Exception as e:
                logger.warning(f"Could not process {csv_file}: {str(e)}")
                # Create empty DataFrame as fallback
                df = pd.DataFrame()
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                
                client.put_object(
                    "bronze", 
                    parquet_file,
                    parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type='application/octet-stream'
                )
        
        logger.info("Landing to bronze transformation completed successfully")
        return {
            "marches_count": len(marches_df),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Error in landing to bronze transformation: {str(e)}")
        raise

def bronze_to_silver_transformation():
    """Transform bronze data to silver layer with cleaning and validation"""
    try:
        logger.info("Starting bronze to silver transformation...")
        client = create_minio_client()
        
        # Ensure silver bucket exists
        if not client.bucket_exists("silver"):
            client.make_bucket("silver")
        
        # Load bronze data
        logger.info("Loading bronze data...")
        
        # Load marches publics
        marches_obj = client.get_object("bronze", "marches_publics.parquet")
        marches_df = pd.read_parquet(BytesIO(marches_obj.read()))
        
        # Load etablissements
        etab_obj = client.get_object("bronze", "etablissements.parquet")
        etab_df = pd.read_parquet(BytesIO(etab_obj.read()))
        
        # Load NAF codes
        naf_obj = client.get_object("bronze", "codes_naf.parquet")
        naf_df = pd.read_parquet(BytesIO(naf_obj.read()))
        
        logger.info(f"Loaded data - Marches: {len(marches_df)}, Etablissements: {len(etab_df)}, NAF: {len(naf_df)}")
        
        # Clean and transform marches data
        logger.info("Cleaning marches data...")
        marches_clean = marches_df.copy()
        
        # Parse dates
        if 'dateNotification' in marches_clean.columns:
            marches_clean['dateNotification'] = pd.to_datetime(marches_clean['dateNotification'], errors='coerce')
            marches_clean['year_notification'] = marches_clean['dateNotification'].dt.year
            marches_clean['month_notification'] = marches_clean['dateNotification'].dt.month
        
        # Filter for 2022 contracts
        marches_2022 = marches_clean[marches_clean['year_notification'] == 2022].copy()
        logger.info(f"Found {len(marches_2022)} contracts from 2022")
        
        # Clean etablissements data
        logger.info("Cleaning etablissements data...")
        etab_clean = etab_df.copy()
        
        # Filter for active establishments
        if 'etatAdministratifEtablissement' in etab_clean.columns:
            etab_active = etab_clean[etab_clean['etatAdministratifEtablissement'] == 'A'].copy()
        else:
            etab_active = etab_clean.copy()
        
        logger.info(f"Found {len(etab_active)} active establishments")
        
        # Save silver data
        logger.info("Saving silver data...")
        
        datasets = [
            (marches_2022, "marches_publics_2022.parquet"),
            (etab_active, "etablissements_actifs.parquet"),
            (naf_df, "codes_naf.parquet")
        ]
        
        for df, filename in datasets:
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            client.put_object(
                "silver", 
                filename,
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
        
        logger.info("Bronze to silver transformation completed successfully")
        return {
            "marches_2022_count": len(marches_2022),
            "active_establishments_count": len(etab_active),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Error in bronze to silver transformation: {str(e)}")
        raise

def silver_to_gold_transformation():
    """Transform silver data to gold layer with analytics"""
    try:
        logger.info("Starting silver to gold transformation...")
        client = create_minio_client()
        
        # Ensure gold bucket exists
        if not client.bucket_exists("gold"):
            client.make_bucket("gold")
        
        # Load silver data
        logger.info("Loading silver data...")
        
        marches_obj = client.get_object("silver", "marches_publics_2022.parquet")
        marches_df = pd.read_parquet(BytesIO(marches_obj.read()))
        
        etab_obj = client.get_object("silver", "etablissements_actifs.parquet")
        etab_df = pd.read_parquet(BytesIO(etab_obj.read()))
        
        naf_obj = client.get_object("silver", "codes_naf.parquet")
        naf_df = pd.read_parquet(BytesIO(naf_obj.read()))
        
        logger.info(f"Loaded silver data - Marches: {len(marches_df)}, Etablissements: {len(etab_df)}, NAF: {len(naf_df)}")        # Identify companies with contracts in 2022 that are still active
        logger.info("Creating analytical dataset...")
        
        # Extract SIRET from titulaires field
        logger.info("Extracting SIRET numbers from titulaires field...")
        siret_list = []
        contract_info = []
        
        for idx, row in marches_df.iterrows():
            titulaires = row['titulaires']
            if titulaires and titulaires != 'nan' and str(titulaires) != 'nan':
                try:
                    # Parse titulaires as JSON if it's a string
                    if isinstance(titulaires, str):
                        import json
                        titulaires_data = json.loads(titulaires.replace("'", '"'))
                    else:
                        titulaires_data = eval(str(titulaires)) if isinstance(titulaires, str) else titulaires
                    
                    # Handle both list and single dict cases
                    if isinstance(titulaires_data, list):
                        for titulaire in titulaires_data:
                            if isinstance(titulaire, dict) and 'id' in titulaire:
                                siret_list.append(titulaire['id'])
                                contract_info.append({
                                    'siret': titulaire['id'],
                                    'contract_id': row['id'],
                                    'montant': row.get('montant', 0),
                                    'dateNotification': row.get('dateNotification'),
                                    'objet': row.get('objet', ''),
                                    'denominationSociale': titulaire.get('denominationSociale', '')
                                })
                    elif isinstance(titulaires_data, dict) and 'id' in titulaires_data:
                        siret_list.append(titulaires_data['id'])
                        contract_info.append({
                            'siret': titulaires_data['id'],
                            'contract_id': row['id'],
                            'montant': row.get('montant', 0),
                            'dateNotification': row.get('dateNotification'),
                            'objet': row.get('objet', ''),
                            'denominationSociale': titulaires_data.get('denominationSociale', '')
                        })
                except Exception as e:
                    logger.debug(f"Could not parse titulaires for row {idx}: {e}")
                    continue
        
        logger.info(f"Extracted {len(siret_list)} SIRET numbers from contracts")
        
        if siret_list:            # Create DataFrame from contract info
            contracts_df = pd.DataFrame(contract_info)
            
            # Convert SIRET to string for consistency
            contracts_df['siret'] = contracts_df['siret'].astype(str)
            etab_df['siret'] = etab_df['siret'].astype(str)
            
            # Clean and convert montant to numeric
            contracts_df['montant'] = pd.to_numeric(contracts_df['montant'], errors='coerce').fillna(0)
            
            # Aggregate contracts by company
            contract_stats = contracts_df.groupby('siret').agg({
                'contract_id': 'count',
                'montant': ['sum', 'mean'],
                'denominationSociale': 'first'
            }).reset_index()
            
            # Flatten column names
            contract_stats.columns = ['siret', 'contract_count', 'total_value', 'avg_value', 'company_name']
            
            logger.info(f"Found contracts for {len(contract_stats)} unique companies")
            
            # Join with active establishments
            # Companies that had contracts in 2022 and are still active
            active_contractors = contract_stats.merge(
                etab_df, 
                on='siret', 
                how='inner'
            )
            
            logger.info(f"Found {len(active_contractors)} companies with 2022 contracts that are still active")
            
            # Join with NAF codes for sector information
            if 'activitePrincipaleEtablissement' in active_contractors.columns:
                # Convert NAF code columns to string for joining
                if 'code_naf' in naf_df.columns:
                    active_contractors['activitePrincipaleEtablissement'] = active_contractors['activitePrincipaleEtablissement'].astype(str)
                    naf_df['code_naf'] = naf_df['code_naf'].astype(str)
                    
                    final_dataset = active_contractors.merge(
                        naf_df,
                        left_on='activitePrincipaleEtablissement',
                        right_on='code_naf',
                        how='left'
                    )
                else:
                    final_dataset = active_contractors
            else:
                final_dataset = active_contractors
            
            # Add analysis columns
            final_dataset['analysis_date'] = datetime.now()
            final_dataset['had_2022_contracts'] = True
            final_dataset['is_currently_active'] = True
            
            # Save final analytical dataset
            logger.info("Saving gold analytical dataset...")
            
            parquet_buffer = BytesIO()
            final_dataset.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            client.put_object(
                "gold", 
                "companies_2022_contracts_still_active.parquet",
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            # Create summary statistics
            summary_stats = {
                'total_companies_with_2022_contracts': len(contract_stats),
                'companies_still_active': len(final_dataset),
                'total_contract_value': float(contract_stats['total_value'].sum()),
                'average_contracts_per_company': float(contract_stats['contract_count'].mean()),
                'analysis_date': datetime.now().isoformat()
            }
            
            # Save summary
            summary_json = json.dumps(summary_stats, indent=2)
            client.put_object(
                "gold",
                "analysis_summary.json",
                BytesIO(summary_json.encode()),
                length=len(summary_json.encode()),
                content_type='application/json'
            )
            
            logger.info("Silver to gold transformation completed successfully")
            return {
                "final_companies_count": len(final_dataset),
                "summary_stats": summary_stats,
                "status": "success"
            }
        
        logger.warning("Could not complete analysis due to missing SIRET columns")
        return {"status": "warning", "message": "Missing SIRET columns"}
        
    except Exception as e:
        logger.error(f"Error in silver to gold transformation: {str(e)}")
        raise

def main():
    """Main pipeline execution"""
    if len(sys.argv) < 2:
        logger.error("Please specify transformation stage: landing_to_bronze, bronze_to_silver, or silver_to_gold")
        sys.exit(1)
    
    stage = sys.argv[1]
    
    try:
        if stage == "landing_to_bronze":
            result = landing_to_bronze_transformation()
        elif stage == "bronze_to_silver":
            result = bronze_to_silver_transformation()
        elif stage == "silver_to_gold":
            result = silver_to_gold_transformation()
        else:
            logger.error(f"Unknown stage: {stage}")
            sys.exit(1)
        
        logger.info(f"Pipeline stage {stage} completed successfully")
        logger.info(f"Result: {result}")
        
    except Exception as e:
        logger.error(f"Pipeline stage {stage} failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
