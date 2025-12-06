"""
Étape 2 : Chargement des données depuis GCS vers BigQuery
Vérifie le dossier et le timestamp pour charger les bons fichiers
Ajoute les colonnes extraction_date et extraction_timestamp
Chaque source va dans sa propre table avec historique
"""

from google.cloud import storage, bigquery
from datetime import datetime
import logging
from typing import List, Dict, Optional
import os
import re

from config import CONFIG, ENV

logging.basicConfig(level=ENV.get('log_level', 'INFO'))
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fonctions utilitaires
# ---------------------------------------------------------------------------

def get_gcp_client(client_type='bigquery'):
    """Initialise un client GCP - détecte automatiquement l'environnement"""
    try:
        import streamlit as st
        from google.oauth2 import service_account
        if 'gcp' in st.secrets:
            creds = service_account.Credentials.from_service_account_info(st.secrets["gcp"])
            
            if client_type == 'storage':
                return storage.Client(credentials=creds, project=ENV['project_id'])
            else:
                return bigquery.Client(credentials=creds, project=ENV['project_id'])
    except:
        pass
    
    creds_path = ENV.get('credentials', 'config/gcp-credentials.json')
    if creds_path and os.path.exists(creds_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
    
    if client_type == 'storage':
        return storage.Client(project=ENV['project_id'])
    else:
        return bigquery.Client(project=ENV['project_id'])


def creer_dataset_si_necessaire():
    """Crée le dataset BigQuery si nécessaire"""
    client = get_gcp_client('bigquery')
    dataset_ref = bigquery.Dataset(f"{ENV['project_id']}.{ENV['dataset']}")
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset existant : {ENV['dataset']}")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = ENV['region']
        client.create_dataset(dataset)
        logger.info(f"Dataset créé : {ENV['dataset']}")


def obtenir_nom_table(source_name: str, table_type: str = 'raw') -> str:
    """Retourne le nom de la table BigQuery pour une source donnée"""
    if table_type == 'raw':
        pattern = CONFIG['bigquery']['raw_tables']['pattern']
    elif table_type == 'transformed':
        pattern = CONFIG['bigquery']['transformed_tables']['pattern']
    else:
        raise ValueError(f"Type invalide : {table_type}. Utilisez 'raw' ou 'transformed'")
    
    return pattern.format(source=source_name)


def extraire_infos_fichier(blob_name: str) -> Optional[Dict]:
    """Extrait les informations d'un nom de fichier GCS"""
    pattern = r'.*?/(\d{4})-(\d{2})/(.+?)__(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})\.(parquet|csv)$'
    match = re.match(pattern, blob_name)
    if not match:
        return None
    year, month, source, date, time, ext = match.groups()
    dt = datetime.strptime(f"{date} {time.replace('-', ':')}", "%Y-%m-%d %H:%M:%S")
    return {
        'source': source,
        'year': year,
        'month': month,
        'date': date,
        'time': time,
        'timestamp': f"{date}_{time}",
        'datetime': dt,
        'blob_name': blob_name
    }


def lister_fichiers_par_timestamp(year_month: str = None, timestamp: str = None) -> Dict[str, List[Dict]]:
    """Liste les fichiers GCS groupés par timestamp"""
    client = get_gcp_client('storage')
    bucket = client.bucket(ENV['bucket'])
    prefix = f"{CONFIG['storage']['raw_folder']}/" if not year_month else f"{CONFIG['storage']['raw_folder']}/{year_month}/"
    blobs = bucket.list_blobs(prefix=prefix)
    fichiers_par_timestamp = {}
    for blob in blobs:
        if blob.name.endswith('/'):
            continue
        infos = extraire_infos_fichier(blob.name)
        if not infos:
            logger.warning(f"Fichier ignoré : {blob.name}")
            continue
        if timestamp and infos['timestamp'] != timestamp:
            continue
        ts = infos['timestamp']
        fichiers_par_timestamp.setdefault(ts, []).append(infos)
    return fichiers_par_timestamp


def creer_table_si_necessaire(table_name: str):
    """Crée la table BigQuery si elle n'existe pas et ajoute les colonnes temporelles"""
    client = get_gcp_client('bigquery')
    table_ref = f"{ENV['project_id']}.{ENV['dataset']}.{table_name}"
    try:
        table = client.get_table(table_ref)
        logger.info(f"Table existante : {table_ref}")
        schema_fields = [f.name for f in table.schema]
        colonnes_a_ajouter = []
        if CONFIG['historique']['colonne_timestamp'] not in schema_fields:
            colonnes_a_ajouter.append(f"ADD COLUMN {CONFIG['historique']['colonne_timestamp']} TIMESTAMP")
        if CONFIG['historique']['colonne_date'] not in schema_fields:
            colonnes_a_ajouter.append(f"ADD COLUMN {CONFIG['historique']['colonne_date']} DATE")
        for col in colonnes_a_ajouter:
            client.query(f"ALTER TABLE `{table_ref}` {col}").result()
            logger.info(f"Colonne ajoutée : {col}")
    except Exception:
        logger.info(f"Table {table_name} sera créée au premier chargement")


# ---------------------------------------------------------------------------
# Chargement
# ---------------------------------------------------------------------------

def charger_fichier_vers_bigquery(source_info: Dict, extraction_datetime: datetime) -> bool:
    """Charge un fichier GCS vers BigQuery"""
    client = get_gcp_client('bigquery')
    table_name = obtenir_nom_table(source_info['source'], 'raw')
    creer_table_si_necessaire(table_name)
    table_ref = f"{ENV['project_id']}.{ENV['dataset']}.{table_name}"
    uri = f"gs://{ENV['bucket']}/{source_info['blob_name']}"
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()
        logger.info(f"{source_info['source']} chargé : {load_job.output_rows} lignes")

        # Ajouter colonnes temporelles
        timestamp_col = CONFIG['historique']['colonne_timestamp']
        date_col = CONFIG['historique']['colonne_date']
        update_query = f"""
        UPDATE `{table_ref}`
        SET 
            {timestamp_col} = TIMESTAMP('{extraction_datetime.strftime('%Y-%m-%d %H:%M:%S')}'),
            {date_col} = DATE('{extraction_datetime.strftime('%Y-%m-%d')}')
        WHERE {timestamp_col} IS NULL
        """
        client.query(update_query).result()
        logger.info(f"Colonnes temporelles mises à jour : {timestamp_col}, {date_col}")
        return True
    except Exception as e:
        logger.error(f"Erreur chargement {source_info['source']} : {e}")
        return False


def charger_batch_vers_bigquery(timestamp: str = None, date: str = None) -> bool:
    """Charge tous les fichiers d'un batch vers BigQuery"""
    logger.info("=" * 80)
    logger.info("ÉTAPE 2 : CHARGEMENT VERS BIGQUERY")
    logger.info("=" * 80)

    creer_dataset_si_necessaire()

    # Sélection du batch
    if not timestamp and not date:
        fichiers = lister_fichiers_par_timestamp()
        if not fichiers:
            logger.error("Aucun fichier trouvé dans GCS")
            return False
        timestamp = sorted(fichiers.keys(), reverse=True)[0]
        logger.info(f"Batch le plus récent : {timestamp}")
    elif date and not timestamp:
        year_month = date[:7]
        fichiers = lister_fichiers_par_timestamp(year_month)
        batches_date = [ts for ts in fichiers.keys() if ts.startswith(date)]
        if not batches_date:
            logger.error(f"Aucun batch trouvé pour la date {date}")
            return False
        timestamp = sorted(batches_date, reverse=True)[0]
        logger.info(f"Batch le plus récent du {date} : {timestamp}")

    # Récupérer les fichiers du batch
    year_month = timestamp[:7]
    fichiers = lister_fichiers_par_timestamp(year_month, timestamp)
    sources = fichiers.get(timestamp, [])
    if not sources:
        logger.error(f"Batch {timestamp} introuvable")
        return False

    extraction_datetime = sources[0]['datetime']
    resultats = [charger_fichier_vers_bigquery(s, extraction_datetime) for s in sources]

    # Résumé
    succes_count = sum(resultats)
    total_count = len(resultats)
    logger.info("\n" + "=" * 80)
    logger.info(f"Total : {succes_count}/{total_count} fichiers chargés")
    logger.info(f"Timestamp batch : {timestamp}")
    logger.info("=" * 80)
    return all(resultats)


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        
        if cmd == "list":
            year_month = sys.argv[2] if len(sys.argv) > 2 else None
            fichiers = lister_fichiers_par_timestamp(year_month)
            print(f"\n{len(fichiers)} batch(s) trouvé(s) :\n")
            for ts, batch in sorted(fichiers.items(), reverse=True):
                print(f"  {ts}: {len(batch)} fichier(s)")
        
        elif cmd == "load":
            ts = sys.argv[2] if len(sys.argv) > 2 else None
            charger_batch_vers_bigquery(timestamp=ts)
        
        else:
            print(f"Commande inconnue : {cmd}")
            print("\nUsage:")
            print("  python -m functions.step2_load                    # Charge le batch le plus récent")
            print("  python -m functions.step2_load list               # Liste tous les batchs")
            print("  python -m functions.step2_load list 2024-12       # Liste les batchs de déc 2024")
            print("  python -m functions.step2_load load <TIMESTAMP>   # Charge un batch spécifique")
    else:
        # Sans argument : charge le batch le plus récent
        charger_batch_vers_bigquery()