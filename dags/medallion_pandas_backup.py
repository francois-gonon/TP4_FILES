#!/usr/bin/env python3
"""
Medallion Architecture Pipeline with Pandas (Manual Backup)
Manual-only DAG for backup data processing using pandas
"""

from datetime import datetime, timedelta
from textwrap import dedent
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'max_active_runs': 1,
}

# DAG definition
dag = DAG(
    'medallion_pandas_backup',
    default_args=default_args,
    description='🐼 Medallion Architecture with Pandas - Manual Backup Pipeline',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 6, 6),
    catchup=False,
    max_active_runs=1,
    tags=['medallion', 'pandas', 'backup', 'manual'],
    doc_md=dedent("""
    ## 🐼 Medallion Architecture Pipeline with Pandas (Backup)
    
    **Purpose**: Manual backup pipeline using pandas for data processing
    **Trigger**: Manual only (no scheduling)
    
    ### When to use:
    - Spark cluster is unavailable
    - Emergency data processing
    - Development and testing
    - Small dataset processing
    
    ### Architecture:
    - **🥉 Bronze**: Raw data with metadata  
    - **🥈 Silver**: Cleaned and validated data
    - **🥇 Gold**: Analytics-ready business insights
    
    ### Features:
    - Pure pandas implementation
    - Robust CSV parsing with fallbacks
    - Memory-efficient processing
    - Comprehensive error handling
    """),
)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def validate_data_quality(layer: str, **context):
    """Validate data quality for a specific layer"""
    try:
        from minio import Minio
        
        logger.info(f"🔍 Validating data quality for {layer} layer")
        
        # Create MinIO client
        client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        # Check if bucket exists
        bucket_name = layer
        if not client.bucket_exists(bucket_name):
            raise Exception(f"Bucket {bucket_name} does not exist")
        
        # List objects in bucket
        objects = list(client.list_objects(bucket_name))
        if not objects:
            raise Exception(f"No objects found in {bucket_name} bucket")
        
        # Calculate total size
        total_size = sum(obj.size for obj in objects)
        file_count = len(objects)
        
        logger.info(f"✅ {layer} layer validation passed:")
        logger.info(f"   - Files: {file_count}")
        logger.info(f"   - Total size: {total_size:,} bytes")
        
        return {
            'layer': layer,
            'file_count': file_count,
            'total_size': total_size,
            'files': [obj.object_name for obj in objects]
        }
        
    except Exception as e:
        logger.error(f"❌ Data quality validation failed for {layer}: {str(e)}")
        raise

def landing_to_bronze_pandas(**context):
    """Transform landing data to bronze layer using pandas"""
    try:
        import pandas as pd
        from minio import Minio
        from io import BytesIO, StringIO
        
        logger.info("🐼 Starting landing to bronze transformation with pandas")
        
        # Create MinIO client
        client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
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
        
        # Add bronze metadata
        marches_df['bronze_ingestion_timestamp'] = pd.Timestamp.now()
        
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
                    # Try with different separator
                    try:
                        df = pd.read_csv(StringIO(csv_data), low_memory=False, sep=';')
                    except pd.errors.ParserError:
                        # Try with error handling
                        try:
                            df = pd.read_csv(StringIO(csv_data), low_memory=False, on_bad_lines='skip')
                        except:
                            # Final fallback - create empty DataFrame
                            logger.warning(f"Could not parse {csv_file}, creating empty DataFrame")
                            df = pd.DataFrame()
                
                logger.info(f"Loaded {len(df)} records from {csv_file}")
                
                if not df.empty:
                    # Add bronze metadata
                    df['bronze_ingestion_timestamp'] = pd.Timestamp.now()
                    
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
                logger.warning(f"Error processing {csv_file}: {str(e)}")
                # Create empty DataFrame as fallback
                df = pd.DataFrame()
                df['bronze_ingestion_timestamp'] = pd.Timestamp.now()
                
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
        
        logger.info("✅ Landing to bronze transformation completed successfully")
        return {
            "marches_count": len(marches_df),
            "status": "success",
            "processing_method": "pandas"
        }
        
    except Exception as e:
        logger.error(f"❌ Error in landing to bronze transformation: {str(e)}")
        raise

def bronze_to_silver_pandas(**context):
    """Transform bronze data to silver layer with cleaning and validation"""
    try:
        import pandas as pd
        from minio import Minio
        from io import BytesIO
        
        logger.info("🐼 Starting bronze to silver transformation with pandas")
        
        # Create MinIO client
        client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
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
        
        # Add silver metadata
        marches_2022['silver_processing_timestamp'] = pd.Timestamp.now()
        
        # Clean etablissements data
        logger.info("Cleaning etablissements data...")
        etab_clean = etab_df.copy()
        
        # Filter for active establishments
        if 'etatAdministratifEtablissement' in etab_clean.columns:
            etab_active = etab_clean[etab_clean['etatAdministratifEtablissement'] == 'A'].copy()
        else:
            etab_active = etab_clean.copy()
        
        # Add silver metadata
        etab_active['silver_processing_timestamp'] = pd.Timestamp.now()
        etab_active['est_actif'] = True
        
        logger.info(f"Found {len(etab_active)} active establishments")
        
        # Clean NAF codes
        naf_clean = naf_df.copy()
        naf_clean['silver_processing_timestamp'] = pd.Timestamp.now()
        
        # Save silver data
        logger.info("Saving silver data...")
        
        datasets = [
            (marches_2022, "marches_publics_2022.parquet"),
            (etab_active, "etablissements_actifs.parquet"),
            (naf_clean, "codes_naf.parquet")
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
        
        logger.info("✅ Bronze to silver transformation completed successfully")
        return {
            "marches_2022_count": len(marches_2022),
            "active_establishments_count": len(etab_active),
            "status": "success",
            "processing_method": "pandas"
        }
        
    except Exception as e:
        logger.error(f"❌ Error in bronze to silver transformation: {str(e)}")
        raise

def silver_to_gold_pandas(**context):
    """Transform silver data to gold layer with analytics"""
    try:
        import pandas as pd
        from minio import Minio
        from io import BytesIO
        import json
        
        logger.info("🐼 Starting silver to gold transformation with pandas")
        
        # Create MinIO client
        client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
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
        
        logger.info(f"Loaded silver data - Marches: {len(marches_df)}, Etablissements: {len(etab_df)}, NAF: {len(naf_df)}")
        
        # Identify companies with contracts in 2022 that are still active
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
        
        if siret_list:
            # Create DataFrame from contract info
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
            final_dataset['analysis_date'] = pd.Timestamp.now()
            final_dataset['had_2022_contracts'] = True
            final_dataset['is_currently_active'] = True
            final_dataset['gold_processing_timestamp'] = pd.Timestamp.now()
            
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
                'analysis_date': pd.Timestamp.now().isoformat(),
                'processing_method': 'pandas'
            }
            
            # Save summary
            summary_json = json.dumps(summary_stats, indent=2)
            client.put_object(
                "gold",
                "analysis_summary.json",
                BytesIO(summary_json.encode()),
                len(summary_json.encode()),
                content_type='application/json'
            )
            
            logger.info("✅ Silver to gold transformation completed successfully")
            return {
                "final_companies_count": len(final_dataset),
                "summary_stats": summary_stats,
                "status": "success",
                "processing_method": "pandas"
            }
        else:
            logger.warning("No SIRET numbers extracted from contracts")
            return {"status": "warning", "message": "No SIRET numbers found", "processing_method": "pandas"}
        
    except Exception as e:
        logger.error(f"❌ Error in silver to gold transformation: {str(e)}")
        raise

def generate_final_report(**context):
    """Generate comprehensive pipeline execution report"""
    try:
        from minio import Minio
        import json
        from io import BytesIO
        
        logger.info("📊 Generating final pipeline report")
        
        # Create MinIO client
        client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        # Collect statistics from each layer
        layers = ['landing', 'bronze', 'silver', 'gold']
        layer_stats = {}
        
        for layer in layers:
            try:
                if client.bucket_exists(layer):
                    objects = list(client.list_objects(layer))
                    layer_stats[layer] = {
                        'file_count': len(objects),
                        'total_size': sum(obj.size for obj in objects),
                        'files': [obj.object_name for obj in objects]
                    }
                else:
                    layer_stats[layer] = {'error': 'Bucket does not exist'}
            except Exception as e:
                layer_stats[layer] = {'error': str(e)}
        
        # Create comprehensive report
        report = {
            'pipeline_execution': {
                'dag_id': context['dag'].dag_id,
                'execution_date': context['execution_date'].isoformat(),
                'run_id': context['run_id'],
                'status': 'completed',
                'processing_method': 'pandas'
            },
            'layer_statistics': layer_stats,
            'data_flow': {
                'total_processing_time': 'calculated_in_production',
                'records_processed': 'available_in_gold_layer',
                'backup_method': 'pandas_fallback'
            },
            'generated_at': datetime.now().isoformat()
        }
        
        # Save report to gold bucket
        report_json = json.dumps(report, indent=2)
        client.put_object(
            'gold',
            f'pandas_pipeline_report_{context["ds_nodash"]}.json',
            BytesIO(report_json.encode()),
            len(report_json.encode()),
            content_type='application/json'
        )
        
        logger.info("✅ Pipeline report generated successfully")
        logger.info(f"Report summary: {json.dumps(layer_stats, indent=2)}")
        
        return report
        
    except Exception as e:
        logger.error(f"❌ Error generating final report: {str(e)}")
        raise

# =============================================================================
# TASK DEFINITIONS
# =============================================================================

# Start task
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Check data quality in landing layer
check_landing_data = PythonOperator(
    task_id='check_landing_data',
    python_callable=validate_data_quality,
    op_kwargs={'layer': 'landing'},
    dag=dag
)

# Bronze layer processing
with TaskGroup("bronze_layer", dag=dag) as bronze_tasks:
    
    landing_to_bronze = PythonOperator(
        task_id='landing_to_bronze_pandas',
        python_callable=landing_to_bronze_pandas,
        dag=dag
    )

# Bronze validation
with TaskGroup("bronze_validation", dag=dag) as bronze_validation:
    
    validate_bronze = PythonOperator(
        task_id='validate_bronze_data',
        python_callable=validate_data_quality,
        op_kwargs={'layer': 'bronze'},
        dag=dag
    )

# Silver layer processing  
with TaskGroup("silver_layer", dag=dag) as silver_tasks:
    
    bronze_to_silver = PythonOperator(
        task_id='bronze_to_silver_pandas',
        python_callable=bronze_to_silver_pandas,
        dag=dag
    )

# Silver validation
with TaskGroup("silver_validation", dag=dag) as silver_validation:
    
    validate_silver = PythonOperator(
        task_id='validate_silver_data',
        python_callable=validate_data_quality,
        op_kwargs={'layer': 'silver'},
        dag=dag
    )

# Gold layer processing
with TaskGroup("gold_layer", dag=dag) as gold_tasks:
    
    silver_to_gold = PythonOperator(
        task_id='silver_to_gold_pandas',
        python_callable=silver_to_gold_pandas,
        dag=dag
    )

# Gold validation
with TaskGroup("gold_validation", dag=dag) as gold_validation:
    
    validate_gold = PythonOperator(
        task_id='validate_gold_data',
        python_callable=validate_data_quality,
        op_kwargs={'layer': 'gold'},
        dag=dag
    )

# Final reporting
final_report = PythonOperator(
    task_id='generate_final_report',
    python_callable=generate_final_report,
    dag=dag
)

# End task
end_pipeline = DummyOperator(
    task_id='pipeline_completed',
    dag=dag
)

# =============================================================================
# TASK DEPENDENCIES
# =============================================================================

start_pipeline >> check_landing_data >> bronze_tasks >> bronze_validation >> silver_tasks >> silver_validation >> gold_tasks >> gold_validation >> final_report >> end_pipeline
