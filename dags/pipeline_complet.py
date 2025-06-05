#!/usr/bin/env python3
"""
Pipeline de transformation de données simplifiée pour l'architecture médaillon
Objectif : Identifier les entreprises qui ont eu des contrats publics en 2022 et qui sont toujours actives
"""

import pandas as pd
import json
import logging
from minio import Minio
from io import BytesIO, StringIO
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration MinIO
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'

def create_minio_client():
    """Créer un client MinIO"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def landing_to_bronze_transformation():
    """Transformer les données landing vers bronze"""
    try:
        logger.info("🚀 Démarrage de la transformation landing vers bronze...")
        client = create_minio_client()
        
        # S'assurer que le bucket bronze existe
        if not client.bucket_exists("bronze"):
            client.make_bucket("bronze")
            logger.info("✅ Bucket bronze créé")
        
        # Traitement du fichier JSON (marchés publics)
        logger.info("📄 Traitement du fichier marchés publics...")
        json_obj = client.get_object("landing", "aws-marchespublics-annee-2022.json")
        json_data = json_obj.read()
        
        # Parser le JSON ligne par ligne
        marches_data = []
        lines_processed = 0
        for line in json_data.decode('utf-8').split('\n'):
            if line.strip():
                try:
                    marches_data.append(json.loads(line))
                    lines_processed += 1
                    if lines_processed % 10000 == 0:
                        logger.info(f"Traité {lines_processed} lignes...")
                except json.JSONDecodeError:
                    continue
        
        marches_df = pd.DataFrame(marches_data)
        logger.info(f"✅ Chargé {len(marches_df)} enregistrements de marchés publics")
        
        # Sauvegarder en parquet
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
        
        # Traitement des fichiers CSV
        csv_files = [
            ("simulated_etablissements_50000.csv", "etablissements.parquet"),
            ("int_courts_naf_rev_2.csv", "codes_naf.parquet")
        ]
        
        for csv_file, parquet_file in csv_files:
            logger.info(f"📄 Traitement de {csv_file}...")
            
            csv_obj = client.get_object("landing", csv_file)
            csv_data = csv_obj.read().decode('utf-8')
            
            df = pd.read_csv(StringIO(csv_data))
            logger.info(f"✅ Chargé {len(df)} enregistrements depuis {csv_file}")
            
            # Sauvegarder en parquet
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
        
        logger.info("🎉 Transformation landing vers bronze terminée avec succès")
        return {
            "marches_count": len(marches_df),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur dans la transformation landing vers bronze: {str(e)}")
        raise

def bronze_to_silver_transformation():
    """Transformer les données bronze vers silver avec nettoyage"""
    try:
        logger.info("🚀 Démarrage de la transformation bronze vers silver...")
        client = create_minio_client()
        
        # S'assurer que le bucket silver existe
        if not client.bucket_exists("silver"):
            client.make_bucket("silver")
            logger.info("✅ Bucket silver créé")
        
        # Charger les données bronze
        logger.info("📖 Chargement des données bronze...")
        
        # Charger marchés publics
        marches_obj = client.get_object("bronze", "marches_publics.parquet")
        marches_df = pd.read_parquet(BytesIO(marches_obj.read()))
        
        # Charger établissements
        etab_obj = client.get_object("bronze", "etablissements.parquet")
        etab_df = pd.read_parquet(BytesIO(etab_obj.read()))
        
        # Charger codes NAF
        naf_obj = client.get_object("bronze", "codes_naf.parquet")
        naf_df = pd.read_parquet(BytesIO(naf_obj.read()))
        
        logger.info(f"📊 Données chargées - Marchés: {len(marches_df)}, Établissements: {len(etab_df)}, NAF: {len(naf_df)}")
        
        # Nettoyer les données des marchés
        logger.info("🧹 Nettoyage des données des marchés...")
        marches_clean = marches_df.copy()
        
        # Parser les dates si elles existent
        date_cols = [col for col in marches_clean.columns if 'date' in col.lower() or 'Date' in col]
        if date_cols:
            date_col = date_cols[0]  # Prendre la première colonne de date trouvée
            logger.info(f"Utilisation de la colonne de date: {date_col}")
            marches_clean[date_col] = pd.to_datetime(marches_clean[date_col], errors='coerce')
            marches_clean['year_notification'] = marches_clean[date_col].dt.year
            marches_clean['month_notification'] = marches_clean[date_col].dt.month
            
            # Filtrer pour 2022
            marches_2022 = marches_clean[marches_clean['year_notification'] == 2022].copy()
            logger.info(f"🎯 Trouvé {len(marches_2022)} contrats de 2022")
        else:
            logger.warning("⚠️ Aucune colonne de date trouvée, utilisation de toutes les données")
            marches_2022 = marches_clean.copy()
        
        # Nettoyer les données des établissements
        logger.info("🧹 Nettoyage des données des établissements...")
        etab_clean = etab_df.copy()
        
        # Filtrer pour les établissements actifs
        etat_cols = [col for col in etab_clean.columns if 'etat' in col.lower() or 'Etat' in col]
        if etat_cols:
            etat_col = etat_cols[0]
            logger.info(f"Utilisation de la colonne d'état: {etat_col}")
            etab_active = etab_clean[etab_clean[etat_col] == 'A'].copy()
            logger.info(f"🏢 Trouvé {len(etab_active)} établissements actifs")
        else:
            logger.warning("⚠️ Aucune colonne d'état trouvée, utilisation de tous les établissements")
            etab_active = etab_clean.copy()
        
        # Sauvegarder les données silver
        logger.info("💾 Sauvegarde des données silver...")
        
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
        
        logger.info("🎉 Transformation bronze vers silver terminée avec succès")
        return {
            "marches_2022_count": len(marches_2022),
            "active_establishments_count": len(etab_active),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur dans la transformation bronze vers silver: {str(e)}")
        raise

def silver_to_gold_transformation():
    """Transformer les données silver vers gold avec analyses"""
    try:
        logger.info("🚀 Démarrage de la transformation silver vers gold...")
        client = create_minio_client()
        
        # S'assurer que le bucket gold existe
        if not client.bucket_exists("gold"):
            client.make_bucket("gold")
            logger.info("✅ Bucket gold créé")
        
        # Charger les données silver
        logger.info("📖 Chargement des données silver...")
        
        marches_obj = client.get_object("silver", "marches_publics_2022.parquet")
        marches_df = pd.read_parquet(BytesIO(marches_obj.read()))
        
        etab_obj = client.get_object("silver", "etablissements_actifs.parquet")
        etab_df = pd.read_parquet(BytesIO(etab_obj.read()))
        
        naf_obj = client.get_object("silver", "codes_naf.parquet")
        naf_df = pd.read_parquet(BytesIO(naf_obj.read()))
        
        logger.info(f"📊 Données silver chargées - Marchés: {len(marches_df)}, Établissements: {len(etab_df)}, NAF: {len(naf_df)}")
        
        # Identifier les entreprises avec des contrats en 2022 qui sont toujours actives
        logger.info("🔍 Création du dataset analytique...")
        
        # Trouver la colonne SIRET dans les marchés
        siret_cols_marches = [col for col in marches_df.columns if 'siret' in col.lower()]
        if not siret_cols_marches:
            logger.error("❌ Aucune colonne SIRET trouvée dans les données de marchés")
            return {"status": "error", "message": "Aucune colonne SIRET trouvée"}
        
        siret_col_marches = siret_cols_marches[0]
        logger.info(f"🏷️ Utilisation de la colonne SIRET: {siret_col_marches}")
        
        # Trouver la colonne SIRET dans les établissements
        siret_cols_etab = [col for col in etab_df.columns if 'siret' in col.lower()]
        if not siret_cols_etab:
            logger.error("❌ Aucune colonne SIRET trouvée dans les données d'établissements")
            return {"status": "error", "message": "Aucune colonne SIRET trouvée dans établissements"}
        
        siret_col_etab = siret_cols_etab[0]
        logger.info(f"🏷️ Utilisation de la colonne SIRET établissements: {siret_col_etab}")
        
        # Aggreger les contrats par entreprise
        logger.info("📊 Agrégation des contrats par entreprise...")
        
        # Trouver une colonne de valeur
        valeur_cols = [col for col in marches_df.columns if 'valeur' in col.lower() or 'montant' in col.lower()]
        
        if valeur_cols:
            valeur_col = valeur_cols[0]
            logger.info(f"💰 Utilisation de la colonne de valeur: {valeur_col}")
            
            # Convertir en numérique si nécessaire
            marches_df[valeur_col] = pd.to_numeric(marches_df[valeur_col], errors='coerce')
            
            contract_stats = marches_df.groupby(siret_col_marches).agg({
                valeur_col: ['count', 'sum', 'mean']
            }).reset_index()
            
            # Aplatir les noms de colonnes
            contract_stats.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in contract_stats.columns]
            contract_stats = contract_stats.rename(columns={f"{siret_col_marches}_": siret_col_marches})
            
        else:
            logger.info("⚠️ Aucune colonne de valeur trouvée, comptage seulement")
            contract_stats = marches_df.groupby(siret_col_marches).size().reset_index(name='contract_count')
        
        logger.info(f"🏢 Trouvé des contrats pour {len(contract_stats)} entreprises uniques")
        
        # Joindre avec les établissements actifs
        logger.info("🔗 Jointure avec les établissements actifs...")
        
        # Entreprises qui ont eu des contrats en 2022 ET qui sont toujours actives
        active_contractors = contract_stats.merge(
            etab_df, 
            left_on=siret_col_marches, 
            right_on=siret_col_etab, 
            how='inner'
        )
        
        logger.info(f"🎯 RÉSULTAT: {len(active_contractors)} entreprises avec des contrats 2022 qui sont toujours actives")
        
        # Joindre avec les codes NAF pour les informations sectorielles
        naf_cols = [col for col in active_contractors.columns if 'naf' in col.lower() or 'activite' in col.lower()]
        if naf_cols and 'code_naf' in naf_df.columns:
            naf_col = naf_cols[0]
            logger.info(f"🏭 Ajout des informations sectorielles avec la colonne: {naf_col}")
            
            final_dataset = active_contractors.merge(
                naf_df,
                left_on=naf_col,
                right_on='code_naf',
                how='left'
            )
        else:
            final_dataset = active_contractors
        
        # Ajouter des colonnes d'analyse
        final_dataset['analysis_date'] = datetime.now()
        final_dataset['had_2022_contracts'] = True
        final_dataset['is_currently_active'] = True
        
        # Sauvegarder le dataset analytique final
        logger.info("💾 Sauvegarde du dataset analytique final...")
        
        parquet_buffer = BytesIO()
        final_dataset.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        client.put_object(
            "gold", 
            "entreprises_contrats_2022_actives.parquet",
            parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        
        # Créer un résumé en JSON
        summary_stats = {
            'total_entreprises_contrats_2022': len(contract_stats),
            'entreprises_toujours_actives': len(final_dataset),
            'pourcentage_encore_actives': round((len(final_dataset) / len(contract_stats)) * 100, 2) if len(contract_stats) > 0 else 0,
            'date_analyse': datetime.now().isoformat(),
            'colonnes_disponibles': list(final_dataset.columns)
        }
        
        if valeur_cols:
            valeur_total = contract_stats[f'{valeur_col}_sum'].sum() if f'{valeur_col}_sum' in contract_stats.columns else 0
            summary_stats['valeur_totale_contrats'] = float(valeur_total)
        
        # Sauvegarder le résumé
        summary_json = json.dumps(summary_stats, indent=2, ensure_ascii=False)
        client.put_object(
            "gold",
            "resume_analyse.json",
            BytesIO(summary_json.encode('utf-8')),
            length=len(summary_json.encode('utf-8')),
            content_type='application/json'
        )
        
        logger.info("🎉 Transformation silver vers gold terminée avec succès")
        logger.info(f"📈 Statistiques: {summary_stats}")
        
        return {
            "entreprises_finales_count": len(final_dataset),
            "summary_stats": summary_stats,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur dans la transformation silver vers gold: {str(e)}")
        raise

def run_complete_pipeline():
    """Exécuter le pipeline complet"""
    try:
        logger.info("🚀 DÉMARRAGE DU PIPELINE COMPLET")
        logger.info("="*60)
        
        # Étape 1: Landing vers Bronze
        logger.info("ÉTAPE 1: Landing vers Bronze")
        result1 = landing_to_bronze_transformation()
        logger.info(f"Résultat étape 1: {result1}")
        
        # Étape 2: Bronze vers Silver
        logger.info("\nÉTAPE 2: Bronze vers Silver")
        result2 = bronze_to_silver_transformation()
        logger.info(f"Résultat étape 2: {result2}")
        
        # Étape 3: Silver vers Gold
        logger.info("\nÉTAPE 3: Silver vers Gold")
        result3 = silver_to_gold_transformation()
        logger.info(f"Résultat étape 3: {result3}")
        
        logger.info("="*60)
        logger.info("🎉 PIPELINE COMPLET TERMINÉ AVEC SUCCÈS!")
        
        return {
            "pipeline_status": "success",
            "etapes": [result1, result2, result3]
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur dans le pipeline complet: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        logger.info("Exécution du pipeline complet...")
        run_complete_pipeline()
    else:
        stage = sys.argv[1]
        
        if stage == "landing_to_bronze":
            landing_to_bronze_transformation()
        elif stage == "bronze_to_silver":
            bronze_to_silver_transformation()
        elif stage == "silver_to_gold":
            silver_to_gold_transformation()
        elif stage == "complete":
            run_complete_pipeline()
        else:
            logger.error(f"Étape inconnue: {stage}")
            sys.exit(1)
