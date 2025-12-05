"""
Utilitaires communs à toutes les fonctions
"""
import yaml
import os
from datetime import datetime
from google.cloud import storage, bigquery
from dotenv import load_dotenv
import logging
from rich.logging import RichHandler

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger("pipeline-etl")


def load_config():
    """Charge la configuration depuis config.yaml"""
    with open('config/config.yaml', 'r') as f:
        return yaml.safe_load(f)


def get_storage_client():
    """Retourne un client Google Cloud Storage"""
    return storage.Client()


def get_bigquery_client():
    """Retourne un client BigQuery"""
    config = load_config()
    return bigquery.Client(project=config['gcp']['project_id'])


def get_timestamp():
    """Retourne un timestamp formaté"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


class PipelineError(Exception):
    """Exception personnalisée pour les erreurs du pipeline"""
    pass