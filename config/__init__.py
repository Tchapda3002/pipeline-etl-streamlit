"""
Package config - Gestion de la configuration
Charge automatiquement config.yaml et les variables d'environnement
"""

import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

def load_config():
    """
    Charge la configuration depuis config.yaml
    Returns:
        dict: Configuration complète du projet
    """
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_env():
    """
    Charge les variables d'environnement
    Returns:
        dict: Variables d'environnement du projet
    """
    return {
        'project_id': os.getenv('GCP_PROJECT_ID'),
        'region': os.getenv('GCP_REGION'),
        'credentials': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'dataset': os.getenv('BQ_DATASET'),
        'bucket': os.getenv('GCS_BUCKET'),
        'log_level': os.getenv('LOG_LEVEL', 'INFO'),
        'environment': os.getenv('ENVIRONMENT', 'development')
    }

# Chargement automatique au démarrage
CONFIG = load_config()
ENV = load_env()

# Exports publics
__all__ = [
    "CONFIG",
    "ENV",
    "load_config",
    "load_env"
]