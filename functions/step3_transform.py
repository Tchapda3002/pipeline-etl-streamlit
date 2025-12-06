"""
Étape 3 : Transformation des données dans BigQuery
Crée les vues de nettoyage et d'enrichissement avec filtrage par timestamp
"""

from google.cloud import bigquery
import logging
from typing import Dict, List, Optional
import os
from pathlib import Path
from datetime import datetime

from config import CONFIG, ENV

logging.basicConfig(level=ENV.get('log_level', 'INFO'))
logger = logging.getLogger(__name__)


def get_gcp_client():
    """Initialise les clients GCP - détecte automatiquement l'\''environnement"""
    try:
        import streamlit as st
        from google.oauth2 import service_account
        if 'gcp' in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp"])
            from google.cloud import storage, bigquery
            storage_client = storage.Client(credentials=credentials, project=ENV['project_id'])
            bigquery_client = bigquery.Client(credentials=credentials, project=ENV['project_id'])
            return storage_client, bigquery_client
    except Exception:
        pass


def obtenir_timestamps_disponibles() -> List[datetime]:
    """
    Récupère la liste des timestamps disponibles dans les tables raw
    
    Returns:
        List[datetime]: Liste triée des timestamps uniques (du plus récent au plus ancien)
    """
    credentials_path = ENV['credentials']
    if credentials_path and os.path.exists(credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    client = get_gcp_client()
    
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
    """
    Permet de sélectionner un timestamp pour filtrer les données
    
    Args:
        timestamp: Timestamp au format ISO (ex: '2024-12-05T10:30:00')
                  Si None, utilise le plus récent
    
    Returns:
        datetime: Le timestamp sélectionné ou None si erreur
    """
    timestamps_disponibles = obtenir_timestamps_disponibles()
    
    if not timestamps_disponibles:
        logger.error("Aucun timestamp disponible dans les données")
        return None
    
    # Si aucun timestamp spécifié, prendre le plus récent
    if timestamp is None:
        logger.info(f"Aucun timestamp spécifié, utilisation du plus récent : {timestamps_disponibles[0]}")
        return timestamps_disponibles[0]
    
    # Convertir le timestamp fourni en datetime
    try:
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        # Vérifier que le timestamp existe
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
    """
    Lit un fichier SQL depuis le dossier sql/views/
    
    Args:
        nom_fichier: Nom du fichier SQL (ex: "01_ratios_cleaned.sql")
    
    Returns:
        str: Contenu du fichier SQL
    """
    sql_path = Path(__file__).parent.parent / "sql" / "views" / nom_fichier
    
    if not sql_path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_path}")
    
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()


def formater_sql(sql_template: str, timestamp: Optional[datetime] = None) -> str:
    """
    Remplace les placeholders dans le SQL par les valeurs de config
    
    Args:
        sql_template: Template SQL avec {project_id}, {dataset} et {timestamp_filter}
        timestamp: Timestamp à utiliser pour le filtre (None = le plus récent)
    
    Returns:
        str: SQL formaté
    """
    # Obtenir le timestamp à utiliser
    if timestamp is None:
        timestamp = selectionner_timestamp()
    
    # Créer le filtre de timestamp
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
    """
    Crée ou remplace une vue dans BigQuery
    
    Args:
        nom_vue: Nom de la vue à créer
        fichier_sql: Nom du fichier SQL contenant la définition
        timestamp: Timestamp à utiliser pour filtrer les données
    
    Returns:
        bool: True si succès
    """
    credentials_path = ENV['credentials']
    if credentials_path and os.path.exists(credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    client = get_gcp_client()
    
    logger.info(f"Création de la vue : {nom_vue}")
    logger.info(f"  Fichier SQL : {fichier_sql}")
    if timestamp:
        logger.info(f"  Timestamp : {timestamp}")
    
    try:
        # Lire le fichier SQL
        sql = lire_fichier_sql(fichier_sql)
        
        # Formater avec les variables
        sql_formate = formater_sql(sql, timestamp)
        
        # Exécuter la requête
        query_job = client.query(sql_formate)
        query_job.result()
        
        logger.info(f"SUCCESS : Vue {nom_vue} créée\n")
        return True
        
    except Exception as e:
        logger.error(f"ERREUR lors de la création de {nom_vue} : {e}\n")
        return False


def transform_data(timestamp: Optional[str] = None) -> Dict[str, bool]:
    """
    Fonction principale : crée toutes les vues de transformation
    
    Args:
        timestamp: Timestamp au format ISO pour filtrer les données
                  Si None, utilise le plus récent automatiquement
    
    Returns:
        Dict avec le statut de chaque vue
    """
    logger.info("=" * 80)
    logger.info("ÉTAPE 3 : TRANSFORMATION DES DONNÉES (VUES BIGQUERY)")
    logger.info("=" * 80)
    
    # Sélectionner le timestamp à utiliser
    timestamp_dt = selectionner_timestamp(timestamp)
    
    if timestamp_dt:
        logger.info(f"\nTimestamp sélectionné : {timestamp_dt}")
        logger.info(f"Date : {timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        logger.warning("\nAucun timestamp disponible - les vues utiliseront toutes les données")
    
    logger.info("=" * 80)
    
    # Liste des vues à créer (ordre important : dépendances)
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
    
    # Résumé
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
    
    # Mode CLI
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == 'list':
            # JUSTE lister les timestamps, NE PAS créer de vues
            print("\n" + "=" * 80)
            print("TIMESTAMPS DISPONIBLES")
            print("=" * 80)
            
            timestamps = obtenir_timestamps_disponibles()
            
            if timestamps:
                print(f"\n{len(timestamps)} timestamp(s) trouvé(s) :\n")
                for i, ts in enumerate(timestamps, 1):
                    marker = "⭐ (plus récent)" if i == 1 else ""
                    print(f"{i:2d}. {ts.strftime('%Y-%m-%d %H:%M:%S')} {marker}")
                print(f"\n{'=' * 80}")
            else:
                print("\n⚠️  Aucun timestamp trouvé dans les données")
                print("=" * 80)
        
        elif mode in ['transform', 'run']:
            # Créer les vues
            timestamp = sys.argv[2] if len(sys.argv) > 2 else None
            resultats = transform_data(timestamp=timestamp)
            
            # Afficher résultats
            print("\n" + "=" * 80)
            for vue, succes in resultats.items():
                status = "✅ SUCCESS" if succes else "❌ FAILED"
                print(f"  {vue}: {status}")
            print("=" * 80)
        
        else:
            print(f"Mode inconnu : {mode}")
            print("\nUsage:")
            print("  python -m functions.step3_transform list")
            print("  python -m functions.step3_transform transform")
            print("  python -m functions.step3_transform transform 2024-12-05T10:30:00")
    
    else:
        # Par défaut : afficher l'aide
        print("\nUsage:")
        print("  python -m functions.step3_transform list                    # Lister les timestamps")
        print("  python -m functions.step3_transform transform               # Créer vues (timestamp récent)")
        print("  python -m functions.step3_transform transform TIMESTAMP     # Créer vues (timestamp spécifique)")