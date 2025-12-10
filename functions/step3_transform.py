"""
Étape 3 : Transformation des données dans BigQuery
Crée les vues de nettoyage et d'enrichissement avec filtrage par timestamp
"""

from google.cloud import bigquery, storage
import logging
from typing import Dict, List, Optional
import os
from pathlib import Path
from datetime import datetime
import streamlit as st
import logging

from config import CONFIG, ENV

logging.basicConfig(level=ENV.get('log_level', 'INFO'))
logger = logging.getLogger(__name__)

def get_gcp_client(client_type='storage'):
    """Initialise un client GCP - détecte automatiquement l'environnement"""
    
    # Construire le chemin vers le fichier local
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    creds_path = os.path.join(parent_dir, 'config', 'gcp-credentials.json')
        # TESTER si le fichier existe
    if os.path.exists(creds_path):
        try:
            # ENVIRONNEMENT LOCAL
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
            
            if client_type == 'storage':
                return storage.Client(project=ENV['project_id'])
            else:
                return bigquery.Client(project=ENV['project_id'])
        except:
            logger.info("DEBUG: Mode LOCAL")
            return None

    else:
            try:
            # ENVIRONNEMENT STREAMLIT CLOUD (pas de fichier local)
                from google.oauth2 import service_account
                creds = service_account.Credentials.from_service_account_info(st.secrets["gcp"])
                
                if client_type == 'storage':
                    return storage.Client(credentials=creds, project=ENV['project_id'])
                else:
                    return bigquery.Client(credentials=creds, project=ENV['project_id'])
            except Exception as e:
                    logger.info("DEBUG: Mode streamlit")
                    return None

def obtenir_timestamps_disponibles() -> List[datetime]:
    """Récupère la liste des timestamps disponibles dans les tables raw"""
    client = get_gcp_client('bigquery')
    
    query = f"""
    SELECT DISTINCT extraction_timestamp
    FROM `{ENV['project_id']}.{ENV['dataset']}.ratios_inpi_raw`
    WHERE extraction_timestamp IS NOT NULL
    ORDER BY extraction_timestamp DESC
    """
    
    try:
        results = client.query(query).result()
        timestamps = [row.extraction_timestamp for row in results]
        return timestamps
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des timestamps : {e}")
        return []


def selectionner_timestamp(timestamp: Optional[str] = None) -> Optional[datetime]:
    """Permet de sélectionner un timestamp pour filtrer les données"""
    timestamps_disponibles = obtenir_timestamps_disponibles()
    
    if not timestamps_disponibles:
        logger.error("Aucun timestamp disponible dans les données")
        return None
    
    if timestamp is None:
        logger.info(f"Aucun timestamp spécifié, utilisation du plus récent : {timestamps_disponibles[0]}")
        return timestamps_disponibles[0]
    
    try:
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        if timestamp_dt not in timestamps_disponibles:
            logger.warning(f"Timestamp {timestamp} non trouvé dans les données disponibles")
            logger.info("Timestamps disponibles :")
            for i, ts in enumerate(timestamps_disponibles[:10], 1):
                logger.info(f"  {i}. {ts}")
            
            logger.info(f"Utilisation du plus récent : {timestamps_disponibles[0]}")
            return timestamps_disponibles[0]
        
        return timestamp_dt
        
    except Exception as e:
        logger.error(f"Erreur lors du parsing du timestamp '{timestamp}' : {e}")
        logger.info(f"Utilisation du plus récent : {timestamps_disponibles[0]}")
        return timestamps_disponibles[0]


def lire_fichier_sql(nom_fichier: str) -> str:
    """Lit un fichier SQL depuis le dossier sql/views/"""
    sql_path = Path(__file__).parent.parent / "sql" / "views" / nom_fichier
    
    if not sql_path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_path}")
    
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()


def formater_sql(sql_template: str, timestamp: Optional[datetime] = None) -> str:
    """Remplace les placeholders dans le SQL par les valeurs de config"""
    if timestamp is None:
        timestamp = selectionner_timestamp()
    
    if timestamp:
        timestamp_filter = f"AND extraction_timestamp = TIMESTAMP('{timestamp.isoformat()}')"
    else:
        timestamp_filter = ""
    
    return sql_template.format(
        project_id=ENV['project_id'],
        dataset=ENV['dataset'],
        timestamp_filter=timestamp_filter
    )


def creer_vue(nom_vue: str, fichier_sql: str, timestamp: Optional[datetime] = None) -> bool:
    """Crée ou remplace une vue dans BigQuery"""
    client = get_gcp_client('bigquery')
    
    logger.info(f"Création de la vue : {nom_vue}")
    logger.info(f"  Fichier SQL : {fichier_sql}")
    if timestamp:
        logger.info(f"  Timestamp : {timestamp}")
    
    try:
        sql = lire_fichier_sql(fichier_sql)
        sql_formate = formater_sql(sql, timestamp)
        
        query_job = client.query(sql_formate)
        query_job.result()
        
        logger.info(f"SUCCESS : Vue {nom_vue} créée\n")
        return True
        
    except Exception as e:
        logger.error(f"ERREUR lors de la création de {nom_vue} : {e}\n")
        return False


def transform_data(timestamp: Optional[str] = None) -> Dict[str, bool]:
    """Fonction principale : crée toutes les vues de transformation"""
    logger.info("=" * 80)
    logger.info("ÉTAPE 3 : TRANSFORMATION DES DONNÉES (VUES BIGQUERY)")
    logger.info("=" * 80)
    
    timestamp_dt = selectionner_timestamp(timestamp)
    
    if timestamp_dt:
        logger.info(f"\nTimestamp sélectionné : {timestamp_dt}")
        logger.info(f"Date : {timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        logger.warning("\nAucun timestamp disponible - les vues utiliseront toutes les données")
    
    logger.info("=" * 80)
    
    vues = [
        {
            'nom': CONFIG['bigquery']['views']['ratios_cleaned'],
            'fichier': '01_ratios_cleaned.sql',
            'description': 'Nettoyage des ratios financiers'
        },
        {
            'nom': CONFIG['bigquery']['views']['stock_cleaned'],
            'fichier': '02_stock_cleaned.sql',
            'description': 'Nettoyage du stock des entreprises'
        },
        {
            'nom': CONFIG['bigquery']['views']['looker'],
            'fichier': '04_vue_looker_studio.sql',
            'description': 'Vue finale pour Looker Studio'
        }
    ]
    
    resultats = {}
    
    for vue in vues:
        logger.info(f"\n{'-' * 80}")
        logger.info(f"Vue : {vue['nom']}")
        logger.info(f"Description : {vue['description']}")
        logger.info(f"{'-' * 80}")
        
        succes = creer_vue(vue['nom'], vue['fichier'], timestamp_dt)
        resultats[vue['nom']] = succes
    
    logger.info("\n" + "=" * 80)
    logger.info("RÉSUMÉ DES TRANSFORMATIONS")
    logger.info("=" * 80)
    
    succes_count = sum(resultats.values())
    total_count = len(resultats)
    
    for nom_vue, succes in resultats.items():
        status = "SUCCESS" if succes else "FAILED"
        logger.info(f"  {nom_vue}: {status}")
    
    logger.info(f"\nTotal : {succes_count}/{total_count} vues créées avec succès")
    logger.info("=" * 80)
    
    return resultats


# Point d'entrée CLI
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == 'list':
            print("\n" + "=" * 80)
            print("TIMESTAMPS DISPONIBLES")
            print("=" * 80)
            
            timestamps = obtenir_timestamps_disponibles()
            
            if timestamps:
                print(f"\n{len(timestamps)} timestamp(s) trouvé(s) :\n")
                for i, ts in enumerate(timestamps, 1):
                    marker = "(plus récent)" if i == 1 else ""
                    print(f"{i:2d}. {ts.strftime('%Y-%m-%d %H:%M:%S')} {marker}")
                print(f"\n{'=' * 80}")
            else:
                print("\n Aucun timestamp trouvé dans les données")
                print("=" * 80)
        
        elif mode in ['transform', 'run']:
            timestamp = sys.argv[2] if len(sys.argv) > 2 else None
            resultats = transform_data(timestamp=timestamp)
            
            print("\n" + "=" * 80)
            for vue, succes in resultats.items():
                status = "SUCCESS" if succes else "FAILED"
                print(f"  {vue}: {status}")
            print("=" * 80)
        
        else:
            print(f"Commande inconnue : {mode}")
            print("\nUsage:")
            print("  python -m functions.step3_transform list                    # Lister les timestamps")
            print("  python -m functions.step3_transform transform               # Créer vues (timestamp récent)")
            print("  python -m functions.step3_transform transform TIMESTAMP     # Créer vues (timestamp spécifique)")
    
    else:
        print("\nUsage:")
        print("  python -m functions.step3_transform list                    # Lister les timestamps")
        print("  python -m functions.step3_transform transform               # Créer vues (timestamp récent)")
        print("  python -m functions.step3_transform transform TIMESTAMP     # Créer vues (timestamp spécifique)")