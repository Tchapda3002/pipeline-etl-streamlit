"""
Interface Streamlit moderne pour le pipeline ETL
Design épuré et professionnel avec logs en temps réel
"""

import streamlit as st
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
import sys
import os
from PIL import Image
import logging
from io import StringIO
import threading
import queue

# Configuration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from functions.step1_download import download_data
from functions.step2_load import charger_batch_vers_bigquery
from functions.step3_transform import transform_data
from functions.orchestrator import run_pipeline

import yaml
from google.cloud import bigquery, storage

# Chargement config
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.yaml')

with open(config_path, 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f)

ENV = {
    'project_id': CONFIG['gcp']['project_id'],
    'region': CONFIG['gcp']['region'],
    'bucket': CONFIG['storage']['bucket_name'],
    'dataset': CONFIG['bigquery']['dataset'],
    'credentials': CONFIG['gcp']['credentials_path'],
    'log_level': 'INFO'
}

# Configuration du logger personnalisé pour Streamlit
class StreamlitLogHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
        
    def emit(self, record):
        log_entry = self.format(record)
        self.log_queue.put(log_entry)

# Configuration de la page
st.set_page_config(
    page_title="Pipeline ETL",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Style CSS moderne (sans emojis)
st.markdown("""
<style>
    /* Variables */
    :root {
        --primary: #2E86AB;
        --secondary: #A23B72;
        --success: #06A77D;
        --warning: #F18F01;
        --danger: #C73E1D;
        --dark: #1A1A2E;
        --light: #F8F9FA;
    }
    
    /* Général */
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
    }
    
    .block-container {
        background: white;
        border-radius: 20px;
        padding: 3rem;
        box-shadow: 0 20px 60px rgba(0,0,0,0.1);
    }
    
    /* Header */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--dark);
        margin-bottom: 0.5rem;
        letter-spacing: -0.5px;
    }
    
    .subtitle {
        font-size: 1.1rem;
        color: #6c757d;
        margin-bottom: 2rem;
    }
    
    /* Top bar */
    .top-bar {
        background: white;
        padding: 1rem 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    
    /* Cards */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        padding: 1.5rem;
        color: white;
        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
        transition: transform 0.3s;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Terminal logs */
    .log-terminal {
        background: #1e1e1e;
        color: #d4d4d4;
        font-family: 'Courier New', monospace;
        padding: 1rem;
        border-radius: 10px;
        max-height: 400px;
        overflow-y: auto;
        font-size: 0.9rem;
        line-height: 1.5;
        box-shadow: 0 5px 20px rgba(0,0,0,0.3);
    }
    
    .log-entry {
        margin: 0.2rem 0;
        white-space: pre-wrap;
        word-wrap: break-word;
    }
    
    .log-info {
        color: #4EC9B0;
    }
    
    .log-warning {
        color: #FFD700;
    }
    
    .log-error {
        color: #F48771;
    }
    
    .log-success {
        color: #4EC9B0;
        font-weight: bold;
    }
    
    /* Boutons */
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        letter-spacing: 0.5px;
        transition: all 0.3s;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 25px rgba(102, 126, 234, 0.4);
    }
    
    /* Bouton stop */
    .stop-button {
        background: linear-gradient(135deg, #C73E1D 0%, #8B0000 100%) !important;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1A1A2E 0%, #16213E 100%);
    }
    
    /* Tables */
    .dataframe {
        border: none !important;
        border-radius: 10px;
        overflow: hidden;
    }
    
    .dataframe thead tr {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    .dataframe tbody tr:nth-child(even) {
        background: #f8f9fa;
    }
    
    /* Info boxes */
    .stAlert {
        border-radius: 10px;
        border: none;
        box-shadow: 0 5px 15px rgba(0,0,0,0.1);
    }
    
    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
    }
    
    /* Documentation */
    .doc-section {
        background: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        border-left: 4px solid #667eea;
    }
    
    .doc-title {
        font-size: 1.3rem;
        font-weight: 600;
        color: #1A1A2E;
        margin-bottom: 0.5rem;
    }
    
    .code-block {
        background: #1e1e1e;
        color: #d4d4d4;
        padding: 1rem;
        border-radius: 5px;
        font-family: 'Courier New', monospace;
        overflow-x: auto;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def get_gcp_client(client_type='storage'):
    """Initialise un client GCP - détecte automatiquement l'environnement"""
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    creds_path = os.path.join(parent_dir, 'config', 'gcp-credentials.json')
    
    if os.path.exists(creds_path):
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
            
            if client_type == 'storage':
                return storage.Client(project=ENV['project_id'])
            else:
                return bigquery.Client(project=ENV['project_id'])
        except:
            return None
    else:
        try:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_info(st.secrets["gcp"])
            
            if client_type == 'storage':
                return storage.Client(credentials=creds, project=ENV['project_id'])
            else:
                return bigquery.Client(credentials=creds, project=ENV['project_id'])
        except:
            return None


def lister_batchs_disponibles() -> List[Dict]:
    try:
        client = get_gcp_client('storage')
        bucket = client.bucket(ENV['bucket'])
        batchs = []
        timestamps_vus = set()
        
        blobs = bucket.list_blobs(prefix=CONFIG['storage']['raw_folder'])
        
        for blob in blobs:
            parts = blob.name.split('__')
            if len(parts) >= 2:
                timestamp_part = parts[1].split('.')[0]
                if timestamp_part not in timestamps_vus:
                    timestamps_vus.add(timestamp_part)
                    try:
                        date_part, time_part = timestamp_part.split('_')
                        batchs.append({
                            'timestamp': timestamp_part,
                            'date': date_part,
                            'time': time_part.replace('-', ':')
                        })
                    except:
                        continue
        
        batchs.sort(key=lambda x: x['timestamp'], reverse=True)
        return batchs
    except:
        return []


def obtenir_timestamps_disponibles() -> List[datetime]:
    try:
        client = get_gcp_client('bigquery')
        query = f"""
        SELECT DISTINCT extraction_timestamp
        FROM `{ENV['project_id']}.{ENV['dataset']}.ratios_inpi_raw`
        WHERE extraction_timestamp IS NOT NULL
        ORDER BY extraction_timestamp DESC
        """
        results = client.query(query).result()
        return [row.extraction_timestamp for row in results]
    except:
        return []


def compter_batchs_gcs() -> Dict[str, int]:
    try:
        client = get_gcp_client('storage')
        bucket = client.bucket(ENV['bucket'])
        ratios_count = stock_count = 0
        
        blobs = bucket.list_blobs(prefix=CONFIG['storage']['raw_folder'])
        for blob in blobs:
            if 'ratios_inpi' in blob.name:
                ratios_count += 1
            elif 'stock_entreprises' in blob.name:
                stock_count += 1
        
        return {
            'ratios_inpi': ratios_count,
            'stock_entreprises': stock_count,
            'total': ratios_count + stock_count
        }
    except:
        return {'ratios_inpi': 0, 'stock_entreprises': 0, 'total': 0}


def obtenir_stats_bigquery() -> Dict:
    try:
        client = get_gcp_client('bigquery')
        stats = {}
        tables = {
            'ratios_inpi_raw': 'Ratios INPI',
            'stock_entreprises_raw': 'Informations Entreprises',
            'v_looker_studio': 'Vue Looker'
        }
        
        for table_name, label in tables.items():
            try:
                query = f"SELECT COUNT(*) as count FROM `{ENV['project_id']}.{ENV['dataset']}.{table_name}`"
                result = client.query(query).result()
                stats[label] = next(result).count
            except:
                stats[label] = 0
        return stats
    except:
        return {}


def setup_logger(log_queue):
    """Configure le logger pour capturer les logs dans la queue"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Supprimer les handlers existants
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Ajouter le handler Streamlit
    handler = StreamlitLogHandler(log_queue)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


def display_logs(log_container, log_queue):
    """Affiche les logs en temps réel dans un conteneur"""
    logs = []
    
    while True:
        try:
            log_entry = log_queue.get(timeout=0.1)
            logs.append(log_entry)
            
            # Limiter à 100 lignes
            if len(logs) > 100:
                logs.pop(0)
            
            # Formatage HTML avec coloration
            html_logs = '<div class="log-terminal">'
            for log in logs:
                css_class = "log-info"
                if "WARNING" in log:
                    css_class = "log-warning"
                elif "ERROR" in log:
                    css_class = "log-error"
                elif "SUCCESS" in log or "Terminé" in log:
                    css_class = "log-success"
                
                html_logs += f'<div class="log-entry {css_class}">{log}</div>'
            html_logs += '</div>'
            
            log_container.markdown(html_logs, unsafe_allow_html=True)
            
        except queue.Empty:
            break


# ============================================================================
# INTERFACE
# ============================================================================

def main():
    # Header avec barre supérieure
    st.markdown('<h1 class="main-title">Pipeline ETL - Gestion BigQuery</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Interface moderne de gestion de données</p>', unsafe_allow_html=True)
    
    # Barre supérieure avec bouton tableau de bord
    col1, col2 = st.columns([5, 1])
    
    with col1:
        st.markdown("###")
    
    with col2:
        looker_url = CONFIG.get('looker_studio_url', 'https://lookerstudio.google.com/reporting/5a222634-0196-4b7c-aa28-60c249a4615f')
        st.link_button("📊 Tableau de bord", looker_url, use_container_width=True)
    
    st.markdown("---")
    
    # Navigation horizontale (tabs)
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Accueil",
        "Extraction", 
        "Chargement", 
        "Transformation", 
        "Pipeline Complet"
    ])
    
    with tab1:
        page_dashboard()
    with tab2:
        page_extraction()
    with tab3:
        page_chargement()
    with tab4:
        page_transformation()
    with tab5:
        page_pipeline()


def page_dashboard():
    st.markdown("## Vue d'ensemble du pipeline")
    st.markdown("Visualisez l'état actuel de vos données et des processus ETL en temps réel.")
    
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("🔄 Actualiser", use_container_width=True, key="refresh_dashboard"):
            st.rerun()
    
    st.markdown("---")
    
    # Section Métriques principales
    st.markdown("### Métriques principales")
    st.markdown("Nombre de fichiers disponibles dans Cloud Storage et lignes dans BigQuery.")
    
    with st.spinner("Chargement des métriques..."):
        batchs = compter_batchs_gcs()
        stats = obtenir_stats_bigquery()
        timestamps = obtenir_timestamps_disponibles()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Total Batchs GCS</div>
                <div class="metric-value">{batchs['total']}</div>
                <div style="font-size: 0.8rem; opacity: 0.8; margin-top: 0.5rem;">
                Fichiers stockés
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Ratios INPI</div>
                <div class="metric-value">{batchs['ratios_inpi']}</div>
                <div style="font-size: 0.8rem; opacity: 0.8; margin-top: 0.5rem;">
                Batchs disponibles
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Informations entreprises</div>
                <div class="metric-value">{batchs['stock_entreprises']}</div>
                <div style="font-size: 0.8rem; opacity: 0.8; margin-top: 0.5rem;">
                Batchs disponibles
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            if timestamps:
                ts_recent = timestamps[0]
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Version Vue Looker</div>
                    <div class="metric-value" style="font-size: 1.5rem;">{ts_recent.strftime('%Y-%m-%d')}</div>
                    <div style="font-size: 0.8rem; opacity: 0.8; margin-top: 0.5rem;">
                    {ts_recent.strftime('%H:%M:%S')}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Version Vue Looker</div>
                    <div class="metric-value" style="font-size: 1.5rem;">N/A</div>
                    <div style="font-size: 0.8rem; opacity: 0.8; margin-top: 0.5rem;">
                    Aucune donnée
                    </div>
                </div>
                """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Section Données BigQuery
    st.markdown("### Données dans BigQuery")
    st.markdown("Nombre de lignes dans chaque table et vue de la base de données.")
    
    if stats:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.info(f"""
**Ratios INPI (raw)**

{stats.get('Ratios INPI', 0):,} lignes

Table brute des ratios financiers
            """)
        
        with col2:
            st.info(f"""
**Informations Entreprises (raw)**

{stats.get('Informations Entreprises', 0):,} lignes

Table brute des informations des entreprises
            """)
        
        with col3:
            st.info(f"""
**Vue Looker Studio**

{stats.get('Vue Looker', 0):,} lignes

Vue finale pour visualisation
            """)
    else:
        st.warning("Impossible de récupérer les statistiques BigQuery")
    
    st.markdown("---")
    
    # Section Timestamps disponibles
    st.markdown("### Timestamps disponibles")
    st.markdown("Historique des versions de données chargées dans BigQuery.")
    
    if timestamps:
        df = pd.DataFrame({
            'Timestamp': [ts.strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps[:10]],
            'Date': [ts.strftime('%Y-%m-%d') for ts in timestamps[:10]],
            'Heure': [ts.strftime('%H:%M:%S') for ts in timestamps[:10]],
            'Statut': ['Plus récent' if i == 0 else 'Disponible' for i in range(min(10, len(timestamps)))]
        })
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        if len(timestamps) > 10:
            st.caption(f"Affichage des 10 timestamps les plus récents sur {len(timestamps)} disponibles")
    else:
        st.warning("Aucun timestamp disponible dans les données")


def page_extraction():
    st.markdown("## Extraction des données")
    st.markdown("Télécharge les données depuis les sources externes (Data.gouv, INPI) et les stocke dans Cloud Storage.")
    st.markdown("**Durée estimée :** 2-5 minutes selon la taille des fichiers.")
    
    st.markdown("---")
    
    # Détails des sources
    with st.expander("Voir les détails des sources de données"):
        for source in CONFIG['data_sources']['sources']:
            if source.get('active', True):
                st.markdown(f"**{source['name']}**")
                st.markdown(f"- Description : {source['description']}")
                st.markdown(f"- URL : `{source['url'][:80]}...`")
                st.markdown("---")
    
    st.markdown("---")
    
    sources = [s['name'] for s in CONFIG['data_sources']['sources'] if s.get('active', True)]
    
    col1, col2 = st.columns([2, 1])
    with col1:
        choix = st.selectbox(
            "Sélectionnez la source à télécharger",
            ["Toutes"] + sources,
            help="Choisissez une source spécifique ou téléchargez toutes les sources"
        )
    
    st.markdown("---")
    
    # Initialiser l'état stop pour cette page
    if 'stop_extraction' not in st.session_state:
        st.session_state.stop_extraction = False
    
    # Boutons Lancer et Stop côte à côte
    col_btn1, col_btn2 = st.columns([3, 1])
    
    with col_btn1:
        launch_btn = st.button("Lancer l'extraction", use_container_width=True, type="primary", key="launch_extraction")
    
    with col_btn2:
        if st.button("⛔ STOP", use_container_width=True, type="secondary", key="stop_btn_extraction"):
            st.session_state.stop_extraction = True
            st.warning("Arrêt demandé")
    
    if launch_btn:
        # Reset stop
        st.session_state.stop_extraction = False
        
        source = None if choix == "Toutes" else choix
        
        # Configuration du logger
        log_queue = queue.Queue()
        logger = setup_logger(log_queue)
        
        # Conteneur pour les logs
        st.markdown("### Logs d'exécution en temps réel")
        log_container = st.empty()
        
        progress = st.progress(0)
        status_container = st.empty()
        
        # Fonction d'exécution
        def run_extraction():
            try:
                logger.info("═══════════════════════════════════════")
                logger.info("🚀 Démarrage de l'extraction...")
                logger.info("═══════════════════════════════════════")
                
                if st.session_state.stop_extraction:
                    logger.warning("⛔ Arrêt demandé avant le démarrage")
                    return None
                
                logger.info(f"Source sélectionnée : {source if source else 'Toutes'}")
                resultats = download_data(source_name=source)
                
                if st.session_state.stop_extraction:
                    logger.warning("⛔ Extraction interrompue")
                    return None
                
                logger.info("═══════════════════════════════════════")
                logger.info("✓ Extraction terminée")
                logger.info("═══════════════════════════════════════")
                
                return resultats
            except Exception as e:
                logger.error(f"❌ Erreur: {str(e)}")
                return None
        
        # Exécution avec affichage des logs
        import time
        resultats = None
        
        with status_container:
            st.info("⏳ Extraction en cours...")
        
        # Simulation de progression avec affichage logs
        for i in range(0, 101, 5):
            if st.session_state.stop_extraction:
                logger.warning("⛔ ARRÊT DEMANDÉ PAR L'UTILISATEUR")
                with status_container:
                    st.error("❌ Extraction annulée par l'utilisateur")
                break
            
            progress.progress(i)
            display_logs(log_container, log_queue)
            time.sleep(0.05)
        
        if not st.session_state.stop_extraction:
            resultats = run_extraction()
            progress.progress(100)
            display_logs(log_container, log_queue)
            
            # Attendre un peu pour afficher les derniers logs
            time.sleep(0.5)
            display_logs(log_container, log_queue)
        
        if resultats and not st.session_state.stop_extraction:
            st.markdown("---")
            st.markdown("### Résultats de l'extraction")
            
            succes = sum(1 for v in resultats.values() if v)
            total = len(resultats)
            
            if succes == total:
                st.success(f"✓ Extraction réussie : {succes}/{total} sources téléchargées")
                st.balloons()
            else:
                st.warning(f"⚠ Extraction partielle : {succes}/{total} sources réussies")
            
            df = pd.DataFrame([
                {'Source': s, 'Statut': '✓ Succès' if r else '✗ Échec'}
                for s, r in resultats.items()
            ])
            st.dataframe(df, use_container_width=True, hide_index=True)


def page_chargement():
    st.markdown("## Chargement BigQuery")
    st.markdown("Importe les données depuis Cloud Storage vers les tables BigQuery (raw).")
    st.markdown("**Durée estimée :** 1-3 minutes selon le volume de données.")
    
    st.markdown("---")
    
    st.info("""
**Fonctionnement**

1. Sélection d'un batch (timestamp) dans Cloud Storage
2. Chargement des fichiers Parquet vers BigQuery
3. Création/remplacement des tables raw
4. Conservation du timestamp pour traçabilité
    """)
    
    st.markdown("---")
    
    batchs = lister_batchs_disponibles()
    
    if not batchs:
        st.warning("Aucun batch disponible dans Cloud Storage. Lancez d'abord l'extraction.")
        return
    
    options = ["Plus récent (recommandé)"] + [f"{b['date']} à {b['time']}" for b in batchs[:20]]
    batch_dict = {"Plus récent (recommandé)": None}
    batch_dict.update({f"{b['date']} à {b['time']}": b['timestamp'] for b in batchs[:20]})
    
    col1, col2 = st.columns([2, 1])
    with col1:
        choix = st.selectbox(
            "Sélectionnez un batch à charger",
            options,
            help="Le batch le plus récent est utilisé par défaut"
        )
    
    if len(batchs) > 20:
        st.caption(f"{len(batchs)} batchs disponibles (affichage des 20 plus récents)")
    else:
        st.caption(f"{len(batchs)} batch(s) disponible(s)")
    
    st.markdown("---")
    
    # Initialiser l'état stop
    if 'stop_chargement' not in st.session_state:
        st.session_state.stop_chargement = False
    
    # Boutons Lancer et Stop côte à côte
    col_btn1, col_btn2 = st.columns([3, 1])
    
    with col_btn1:
        launch_btn = st.button("Lancer le chargement", use_container_width=True, type="primary", key="launch_chargement")
    
    with col_btn2:
        if st.button("⛔ STOP", use_container_width=True, type="secondary", key="stop_btn_chargement"):
            st.session_state.stop_chargement = True
            st.warning("Arrêt demandé")
    
    if launch_btn:
        # Reset stop
        st.session_state.stop_chargement = False
        
        timestamp = batch_dict[choix]
        
        # Configuration du logger
        log_queue = queue.Queue()
        logger = setup_logger(log_queue)
        
        # Conteneur pour les logs
        st.markdown("### Logs d'exécution en temps réel")
        log_container = st.empty()
        
        progress = st.progress(0)
        status_container = st.empty()
        
        def run_load():
            try:
                logger.info("═══════════════════════════════════════")
                logger.info("🚀 Démarrage du chargement BigQuery...")
                logger.info("═══════════════════════════════════════")
                
                if st.session_state.stop_chargement:
                    logger.warning("⛔ Arrêt demandé avant le démarrage")
                    return False
                
                logger.info(f"Timestamp sélectionné : {timestamp if timestamp else 'Plus récent'}")
                success = charger_batch_vers_bigquery(timestamp=timestamp)
                
                if st.session_state.stop_chargement:
                    logger.warning("⛔ Chargement interrompu")
                    return False
                
                logger.info("═══════════════════════════════════════")
                logger.info("✓ Chargement terminé")
                logger.info("═══════════════════════════════════════")
                
                return success
            except Exception as e:
                logger.error(f"❌ Erreur: {str(e)}")
                return False
        
        # Exécution avec logs
        import time
        success = False
        
        with status_container:
            st.info("⏳ Chargement en cours...")
        
        for i in range(0, 101, 5):
            if st.session_state.stop_chargement:
                logger.warning("⛔ ARRÊT DEMANDÉ PAR L'UTILISATEUR")
                with status_container:
                    st.error("❌ Chargement annulé par l'utilisateur")
                break
            
            progress.progress(i)
            display_logs(log_container, log_queue)
            time.sleep(0.05)
        
        if not st.session_state.stop_chargement:
            success = run_load()
            progress.progress(100)
            display_logs(log_container, log_queue)
            
            # Attendre un peu pour afficher les derniers logs
            time.sleep(0.5)
            display_logs(log_container, log_queue)
        
        st.markdown("---")
        st.markdown("### Résultat du chargement")
        
        if success and not st.session_state.stop_chargement:
            st.success("✓ Chargement vers BigQuery réussi")
            st.balloons()
        elif not st.session_state.stop_chargement:
            st.error("✗ Échec du chargement vers BigQuery")


def page_transformation():
    st.markdown("## Transformation")
    st.markdown("Crée les vues BigQuery nettoyées et enrichies pour la visualisation.")
    st.markdown("**Durée estimée :** 30 secondes - 1 minute.")
    
    st.markdown("---")
    
    st.info("""
**Vues créées**

1. **v_ratios_cleaned** : Ratios financiers nettoyés et typés
2. **v_stock_cleaned** : informations entreprises nettoyées
3. **v_looker_studio** : Vue finale combinée pour Looker Studio

Ces vues utilisent le timestamp sélectionné pour filtrer les données.
    """)
    
    st.markdown("---")
    
    timestamps = obtenir_timestamps_disponibles()
    
    if not timestamps:
        st.warning("Aucun timestamp disponible. Lancez d'abord le chargement.")
        return
    
    options = ["Plus récent (recommandé)"] + [ts.strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps[:20]]
    ts_dict = {"Plus récent (recommandé)": None}
    ts_dict.update({ts.strftime('%Y-%m-%d %H:%M:%S'): ts.isoformat() for ts in timestamps[:20]})
    
    col1, col2 = st.columns([2, 1])
    with col1:
        choix = st.selectbox(
            "Sélectionnez un timestamp pour filtrer les vues",
            options,
            help="Les vues seront créées avec les données de ce timestamp"
        )
    
    if len(timestamps) > 20:
        st.caption(f"{len(timestamps)} timestamps disponibles (affichage des 20 plus récents)")
    else:
        st.caption(f"{len(timestamps)} timestamp(s) disponible(s)")
    
    st.markdown("---")
    
    # Initialiser l'état stop
    if 'stop_transformation' not in st.session_state:
        st.session_state.stop_transformation = False
    
    # Boutons Lancer et Stop côte à côte
    col_btn1, col_btn2 = st.columns([3, 1])
    
    with col_btn1:
        launch_btn = st.button("Créer les vues", use_container_width=True, type="primary", key="launch_transformation")
    
    with col_btn2:
        if st.button("⛔ STOP", use_container_width=True, type="secondary", key="stop_btn_transformation"):
            st.session_state.stop_transformation = True
            st.warning("Arrêt demandé")
    
    if launch_btn:
        # Reset stop
        st.session_state.stop_transformation = False
        
        timestamp = ts_dict[choix]
        
        # Configuration du logger
        log_queue = queue.Queue()
        logger = setup_logger(log_queue)
        
        # Conteneur pour les logs
        st.markdown("### Logs d'exécution en temps réel")
        log_container = st.empty()
        
        progress = st.progress(0)
        status_container = st.empty()
        
        def run_transform():
            try:
                logger.info("═══════════════════════════════════════")
                logger.info("🚀 Démarrage de la transformation...")
                logger.info("═══════════════════════════════════════")
                
                if st.session_state.stop_transformation:
                    logger.warning("⛔ Arrêt demandé avant le démarrage")
                    return None
                
                logger.info(f"Timestamp sélectionné : {timestamp if timestamp else 'Plus récent'}")
                resultats = transform_data(timestamp=timestamp)
                
                if st.session_state.stop_transformation:
                    logger.warning("⛔ Transformation interrompue")
                    return None
                
                logger.info("═══════════════════════════════════════")
                logger.info("✓ Transformation terminée")
                logger.info("═══════════════════════════════════════")
                
                return resultats
            except Exception as e:
                logger.error(f"❌ Erreur: {str(e)}")
                return None
        
        # Exécution avec logs
        import time
        resultats = None
        
        with status_container:
            st.info("⏳ Transformation en cours...")
        
        for i in range(0, 101, 5):
            if st.session_state.stop_transformation:
                logger.warning("⛔ ARRÊT DEMANDÉ PAR L'UTILISATEUR")
                with status_container:
                    st.error("❌ Transformation annulée par l'utilisateur")
                break
            
            progress.progress(i)
            display_logs(log_container, log_queue)
            time.sleep(0.05)
        
        if not st.session_state.stop_transformation:
            resultats = run_transform()
            progress.progress(100)
            display_logs(log_container, log_queue)
            
            # Attendre un peu pour afficher les derniers logs
            time.sleep(0.5)
            display_logs(log_container, log_queue)
        
        st.markdown("---")
        st.markdown("### Résultats de la transformation")
        
        if resultats and not st.session_state.stop_transformation:
            succes = sum(1 for v in resultats.values() if v)
            total = len(resultats)
            
            if succes == total:
                st.success(f"✓ Transformation réussie : {succes}/{total} vues créées")
                st.balloons()
                
                # Lien Looker
                looker_url = CONFIG.get('looker_studio_url', 'https://lookerstudio.google.com/reporting/5a222634-0196-4b7c-aa28-60c249a4615f')
                st.markdown("---")
                st.link_button("📊 Voir Tableau de bord", looker_url, use_container_width=True)
            else:
                st.warning(f"⚠ Transformation partielle : {succes}/{total} vues créées")
            
            df = pd.DataFrame([
                {'Vue': v.split('.')[-1], 'Statut': '✓ Créée' if r else '✗ Échec'}
                for v, r in resultats.items()
            ])
            st.dataframe(df, use_container_width=True, hide_index=True)


def page_pipeline():
    st.markdown("## Pipeline Complet")
    st.markdown("Exécute les 3 étapes du pipeline en séquence : Extraction → Chargement → Transformation.")
    st.markdown("**Durée estimée :** 5-10 minutes pour un pipeline complet.")
    
    st.markdown("---")
    
    st.info("""
**Processus**

1. **Extraction** : Télécharge les données depuis les URLs
2. **Chargement** : Importe dans BigQuery (tables raw)
3. **Transformation** : Crée les vues nettoyées

Vous pouvez ignorer certaines étapes si les données sont déjà présentes.
    """)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        skip1 = st.checkbox("Ignorer l'extraction", help="Utile si les données sont déjà dans Cloud Storage")
        skip2 = st.checkbox("Ignorer le chargement", help="Utile si les données sont déjà dans BigQuery")
    
    with col2:
        sources = [s['name'] for s in CONFIG['data_sources']['sources'] if s.get('active', True)]
        source = st.selectbox(
            "Source pour l'extraction",
            ["Toutes"] + sources,
            help="Sélectionnez une source spécifique ou toutes les sources"
        )
    
    if not skip1 and not skip2:
        st.warning("⚠ Le pipeline complet va exécuter les 3 étapes. Cela peut prendre plusieurs minutes.")
    
    st.markdown("---")
    
    # Initialiser l'état stop
    if 'stop_pipeline' not in st.session_state:
        st.session_state.stop_pipeline = False
    
    # Boutons Lancer et Stop côte à côte
    col_btn1, col_btn2 = st.columns([3, 1])
    
    with col_btn1:
        launch_btn = st.button("Lancer le pipeline", use_container_width=True, type="primary", key="launch_pipeline")
    
    with col_btn2:
        if st.button("⛔ STOP", use_container_width=True, type="secondary", key="stop_btn_pipeline"):
            st.session_state.stop_pipeline = True
            st.warning("Arrêt demandé")
    
    if launch_btn:
        # Reset stop
        st.session_state.stop_pipeline = False
        
        src = None if source == "Toutes" else source
        
        start = datetime.now()
        st.info(f"🚀 Démarrage : {start.strftime('%H:%M:%S')}")
        
        # Configuration du logger
        log_queue = queue.Queue()
        logger = setup_logger(log_queue)
        
        # Conteneur pour les logs
        st.markdown("### Logs d'exécution en temps réel")
        log_container = st.empty()
        
        progress = st.progress(0)
        status_container = st.empty()
        
        def run_full_pipeline():
            try:
                logger.info("╔═══════════════════════════════════════╗")
                logger.info("║   PIPELINE COMPLET - DÉMARRAGE        ║")
                logger.info("╚═══════════════════════════════════════╝")
                
                if st.session_state.stop_pipeline:
                    logger.warning("⛔ Arrêt demandé avant le démarrage")
                    return False
                
                logger.info("")
                logger.info(f"Configuration:")
                logger.info(f"  - Ignorer extraction: {skip1}")
                logger.info(f"  - Ignorer chargement: {skip2}")
                logger.info(f"  - Source: {src if src else 'Toutes'}")
                logger.info("")
                
                success = run_pipeline(
                    source_name=src,
                    skip_download=skip1,
                    skip_load=skip2
                )
                
                if st.session_state.stop_pipeline:
                    logger.warning("⛔ Pipeline interrompu")
                    return False
                
                logger.info("")
                logger.info("╔═══════════════════════════════════════╗")
                logger.info("║   PIPELINE TERMINÉ                    ║")
                logger.info("╚═══════════════════════════════════════╝")
                
                return success
            except Exception as e:
                logger.error(f"❌ Erreur critique: {str(e)}")
                return False
        
        # Exécution avec logs
        import time
        success = False
        
        with status_container:
            st.info("⏳ Pipeline en cours d'exécution...")
        
        for i in range(0, 101, 3):
            if st.session_state.stop_pipeline:
                logger.warning("⛔ ARRÊT DEMANDÉ PAR L'UTILISATEUR")
                with status_container:
                    st.error("❌ Pipeline interrompu par l'utilisateur")
                break
            
            progress.progress(i)
            display_logs(log_container, log_queue)
            time.sleep(0.05)
        
        if not st.session_state.stop_pipeline:
            success = run_full_pipeline()
            progress.progress(100)
            display_logs(log_container, log_queue)
            
            # Attendre un peu pour afficher les derniers logs
            time.sleep(0.5)
            display_logs(log_container, log_queue)
        
        duration = (datetime.now() - start).total_seconds()
        
        st.markdown("---")
        st.markdown("### Résultats du pipeline")
        
        st.info(f"**Durée totale :** {duration:.2f}s ({duration/60:.2f} minutes)")
        
        if success and not st.session_state.stop_pipeline:
            st.success("✓ Pipeline terminé avec succès")
            st.balloons()
            
            # Lien Looker
            looker_url = CONFIG.get('looker_studio_url', 'https://lookerstudio.google.com/reporting/5a222634-0196-4b7c-aa28-60c249a4615f')
            st.markdown("---")
            st.link_button("📊 Voir le Tableau de bord", looker_url, use_container_width=True)
        elif not st.session_state.stop_pipeline:
            st.error("✗ Pipeline terminé avec des erreurs")


if __name__ == "__main__":
    main()