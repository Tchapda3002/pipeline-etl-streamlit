"""
Étape 1 : Téléchargement des données depuis les URLs vers Google Cloud Storage
Version streaming direct : URL → GCS (sans fichier temporaire)
Adapté pour Streamlit Cloud
"""

import requests
from google.cloud import storage, bigquery
from datetime import datetime
import logging
from typing import Dict, Optional
import os

from config import CONFIG, ENV

# Configuration du logging
logging.basicConfig(level=ENV.get('log_level', 'INFO'))
logger = logging.getLogger(__name__)


def get_gcp_client(client_type='storage'):
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


def verifier_et_creer_bucket():
    """Vérifie que le bucket existe, sinon le crée"""
    client = get_gcp_client('storage')
    bucket_name = ENV['bucket']
    
    bucket = client.bucket(bucket_name)
    
    if not bucket.exists():
        logger.info(f"Bucket '{bucket_name}' n'existe pas, création en cours...")
        try:
            bucket = client.create_bucket(bucket_name, location=ENV['region'])
            logger.info(f"Bucket créé : gs://{bucket_name}")
        except Exception as e:
            logger.error(f"Impossible de créer le bucket : {e}")
            raise
    else:
        logger.info(f"Bucket existant : gs://{bucket_name}")
    
    return bucket


def generer_chemin_gcs(source_name: str, url: str, execution_datetime: datetime) -> str:
    """Génère le chemin GCS selon le pattern défini dans la config"""
    if 'parquet' in url.lower():
        extension = 'parquet'
    elif 'csv' in url.lower():
        extension = 'csv'
    else:
        extension = 'parquet'
    
    structure = CONFIG['storage']['structure']
    chemin = structure.format(
        raw_folder=CONFIG['storage']['raw_folder'],
        source=source_name,
        year=execution_datetime.strftime('%Y'),
        month=execution_datetime.strftime('%m'),
        date=execution_datetime.strftime('%Y-%m-%d'),
        time=execution_datetime.strftime('%H-%M-%S')
    )
    
    chemin = chemin.rsplit('.', 1)[0] + f".{extension}"
    return chemin


def telecharger_et_streamer_vers_gcs(url: str, chemin_gcs: str, source_name: str) -> bool:
    """Télécharge et stream directement vers GCS sans fichier temporaire"""
    try:
        logger.info(f"Téléchargement et streaming de {source_name}...")
        logger.info(f"URL: {url[:80]}...")
        
        client = get_gcp_client('storage')
        bucket = client.bucket(ENV['bucket'])
        blob = bucket.blob(chemin_gcs)
        
        timeout = CONFIG['execution']['timeout_seconds']
        
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        if total_size:
            logger.info(f"Taille totale : {total_size / 1024**2:.2f} MB")
        
        logger.info(f"Streaming vers gs://{ENV['bucket']}/{chemin_gcs}...")
        
        bytes_uploaded = 0
        last_log = 0
        chunk_size = 32 * 1024 * 1024
        log_interval = 50 * 1024 * 1024
        
        with blob.open("wb", chunk_size=chunk_size) as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    bytes_uploaded += len(chunk)
                    
                    if bytes_uploaded - last_log >= log_interval:
                        if total_size > 0:
                            progress = (bytes_uploaded / total_size) * 100
                            logger.info(f"Progression : {progress:.1f}% ({bytes_uploaded / 1024**2:.0f} MB / {total_size / 1024**2:.0f} MB)")
                        else:
                            logger.info(f"Téléchargé : {bytes_uploaded / 1024**2:.0f} MB")
                        last_log = bytes_uploaded
        
        logger.info(f"Upload terminé : {bytes_uploaded / 1024**2:.2f} MB")
        logger.info(f"Destination : gs://{ENV['bucket']}/{chemin_gcs}")
        
        return True
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du téléchargement : {e}")
        return False
    
    except Exception as e:
        logger.error(f"Erreur lors du streaming vers GCS : {e}")
        return False


def download_data(source_name: Optional[str] = None) -> Dict[str, bool]:
    """Télécharge les données depuis les URLs et les stream vers GCS"""
    execution_datetime = datetime.now()
    
    logger.info("=" * 80)
    logger.info("ÉTAPE 1 : TÉLÉCHARGEMENT DES DONNÉES (STREAMING DIRECT)")
    logger.info(f"Timestamp du batch : {execution_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    try:
        verifier_et_creer_bucket()
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du bucket : {e}")
        return {}
    
    sources = CONFIG['data_sources']['sources']
    resultats = {}
    
    if source_name:
        sources = [s for s in sources if s['name'] == source_name]
        if not sources:
            logger.error(f"Source '{source_name}' introuvable dans la configuration")
            return {}
    
    for source in sources:
        if not source.get('active', True):
            logger.info(f"Source désactivée : {source['name']}")
            continue
        
        logger.info(f"\n{'-' * 80}")
        logger.info(f"Source : {source['name']}")
        logger.info(f"Description : {source['description']}")
        logger.info(f"{'-' * 80}")
        
        try:
            chemin_gcs = generer_chemin_gcs(source['name'], source['url'], execution_datetime)
            
            succes = telecharger_et_streamer_vers_gcs(
                url=source['url'],
                chemin_gcs=chemin_gcs,
                source_name=source['name']
            )
            
            resultats[source['name']] = succes
            
            if succes:
                logger.info(f"SUCCESS : {source['name']} traité avec succès\n")
            else:
                logger.error(f"ÉCHEC : {source['name']}\n")
        
        except Exception as e:
            logger.error(f"Erreur lors du traitement de {source['name']} : {e}")
            resultats[source['name']] = False
    
    logger.info("\n" + "=" * 80)
    logger.info("RÉSUMÉ DE L'ÉTAPE 1")
    logger.info("=" * 80)
    
    succes_count = sum(1 for v in resultats.values() if v)
    total_count = len(resultats)
    
    for source_name, succes in resultats.items():
        status = "SUCCESS" if succes else "FAILED"
        logger.info(f"  {source_name}: {status}")
    
    logger.info(f"\nTotal : {succes_count}/{total_count} sources traitées avec succès")
    logger.info(f"Timestamp commun : {execution_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    return resultats


if __name__ == "__main__":
    resultats = download_data()
    
    print("\nTest de step1_download.py")
    for source, succes in resultats.items():
        status = "SUCCESS" if succes else "FAILED"
        print(f"  {source}: {status}")