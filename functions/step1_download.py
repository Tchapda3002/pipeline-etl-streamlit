"""
Étape 1 : Téléchargement des données depuis les URLs vers Google Cloud Storage
Version streaming direct : URL → GCS (sans fichier temporaire)
Adapté pour Streamlit Cloud
"""

import requests
from google.cloud import storage
from datetime import datetime
import logging
from typing import Dict, Optional
import os
from google.cloud import bigquery, storage
import streamlit as st

from config import CONFIG, ENV

# Configuration du logging
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
    
    credentials_path = ENV.get('credentials', 'config/gcp-credentials.json')
    if credentials_path and os.path.exists(credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    from google.cloud import storage, bigquery
    return storage.Client(project=ENV['project_id']), bigquery.Client(project=ENV['project_id'])


def verifier_et_creer_bucket():
    """Vérifie que le bucket existe, sinon le crée"""
    client = get_gcp_client()
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
    """
    Génère le chemin GCS selon le pattern défini dans la config
    STRUCTURE : raw_data/2025-12/ratios_inpi__2025-12-03_14-30-15.parquet
    """
    # Détecter l'extension depuis l'URL
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
    
    # Remplacer l'extension
    chemin = chemin.rsplit('.', 1)[0] + f".{extension}"
    
    return chemin


def telecharger_et_streamer_vers_gcs(url: str, chemin_gcs: str, source_name: str) -> bool:
    """Télécharge et stream directement vers GCS sans fichier temporaire"""
    try:
        logger.info(f"Téléchargement et streaming de {source_name}...")
        logger.info(f"URL: {url[:80]}...")
        
        # Initialiser le client GCS
        client = get_gcp_client()
        bucket = client.bucket(ENV['bucket'])
        blob = bucket.blob(chemin_gcs)
        
        # Configuration
        timeout = CONFIG['execution']['timeout_seconds']
        
        # Téléchargement en streaming
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
        
        # Récupérer la taille du fichier si disponible
        total_size = int(response.headers.get('content-length', 0))
        if total_size:
            logger.info(f"Taille totale : {total_size / 1024**2:.2f} MB")
        
        logger.info(f"Streaming vers gs://{ENV['bucket']}/{chemin_gcs}...")
        
        # Stream directement vers GCS avec progression
        bytes_uploaded = 0
        last_log = 0
        chunk_size = 32 * 1024 * 1024  # 32 MB chunks
        log_interval = 50 * 1024 * 1024  # Log tous les 50 MB
        
        # Ouvrir le blob en mode écriture streaming
        with blob.open("wb", chunk_size=chunk_size) as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    bytes_uploaded += len(chunk)
                    
                    # Afficher la progression tous les 50 MB
                    if bytes_uploaded - last_log >= log_interval:
                        if total_size > 0:
                            progress = (bytes_uploaded / total_size) * 100
                            logger.info(f"Progression : {progress:.1f}% ({bytes_uploaded / 1024**2:.0f} MB / {total_size / 1024**2:.0f} MB)")
                        else:
                            logger.info(f"Téléchargé : {bytes_uploaded / 1024**2:.0f} MB")
                        last_log = bytes_uploaded
        
        # Log final
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
    """
    Fonction principale : télécharge les données depuis les URLs configurées
    et les stream directement vers GCS
    """
    # Timestamp unique pour tout le batch
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
    
    # Filtrer par source si spécifié
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
            
            # Télécharger et streamer directement vers GCS
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
    
    # Résumé final
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


# Point d'entrée pour tests
if __name__ == "__main__":
    resultats = download_data()
    
    print("\nTest de step1_download.py (streaming direct)")
    for source, succes in resultats.items():
        status = "SUCCESS" if succes else "FAILED"
        print(f"  {source}: {status}")