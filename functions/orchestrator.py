"""
Orchestrateur simplifié du pipeline ETL
Gère l'exécution séquentielle des étapes 1, 2 et 3
"""

import logging
from typing import Optional
from datetime import datetime

from functions.step1_download import download_data
from functions.step2_load import charger_batch_vers_bigquery

from config import ENV

logging.basicConfig(
    level=ENV.get('log_level', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_pipeline(
    source_name: Optional[str] = None,
    timestamp_filter: Optional[str] = None,
    skip_download: bool = False,
    skip_load: bool = False
) -> bool:
    """
    Exécute le pipeline ETL complet
    
    Args:
        source_name: Source spécifique à télécharger (None = toutes)
        timestamp_filter: Timestamp pour filtrer les vues (None = le plus récent)
        skip_download: Si True, ignore l'étape 1 (téléchargement)
        skip_load: Si True, ignore l'étape 2 (chargement BigQuery)
    
    Returns:
        bool: True si tout s'est bien passé
    
    Usage:
        # Pipeline complet
        run_pipeline()
        
        # Télécharger une seule source
        run_pipeline(source_name='ratios_inpi')
        
        # Sauter le téléchargement (données déjà présentes)
        run_pipeline(skip_download=True)
        
        # Transformation avec un timestamp spécifique
        run_pipeline(skip_download=True, skip_load=True, timestamp_filter='2024-12-05T10:00:00')
    """
    start_time = datetime.now()
    
    logger.info("=" * 80)
    logger.info("PIPELINE ETL - DÉMARRAGE")
    logger.info(f"Heure : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    success = True
    
    # ÉTAPE 1 : Téléchargement
    if not skip_download:
        logger.info("\nÉTAPE 1/3 : Téléchargement des données")
        logger.info("-" * 80)
        
        try:
            resultats = download_data(source_name)
            step1_success = all(resultats.values()) if resultats else False
            
            if step1_success:
                logger.info("Étape 1 : Téléchargement réussi")
            else:
                logger.error("Étape 1 : Échec du téléchargement")
                return False
                
        except Exception as e:
            logger.error(f"Étape 1 : Erreur - {e}")
            return False
    else:
        logger.info("\n⏭Étape 1 : Téléchargement ignoré")
    
    # ÉTAPE 2 : Chargement BigQuery
    if not skip_load:
        logger.info("\nÉTAPE 2/3 : Chargement vers BigQuery")
        logger.info("-" * 80)
        
        try:
            step2_success = charger_batch_vers_bigquery()
            
            if step2_success:
                logger.info("Étape 2 : Chargement réussi")
            else:
                logger.error("Étape 2 : Échec du chargement")
                return False
                
        except Exception as e:
            logger.error(f"Étape 2 : Erreur - {e}")
            return False
    else:
        logger.info("\n⏭️Étape 2 : Chargement ignoré")
    
    # ÉTAPE 3 : Transformation (vues)
    logger.info("\n🔄 ÉTAPE 3/3 : Transformation des données")
    logger.info("-" * 80)
    
    try:
        resultats = transform_data(timestamp=timestamp_filter)
        step3_success = all(resultats.values()) if resultats else False
        
        if step3_success:
            logger.info("✅ Étape 3 : Transformation réussie")
        else:
            logger.error("❌ Étape 3 : Échec de la transformation")
            success = False
            
    except Exception as e:
        logger.error(f"❌ Étape 3 : Erreur - {e}")
        success = False
    
    # Résumé
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE ETL - TERMINÉ")
    logger.info(f"Durée : {duration:.2f}s ({duration/60:.2f} minutes)")
    
    if success:
        logger.info("Statut : ✅ SUCCÈS")
    else:
        logger.info("Statut : ❌ ÉCHEC")
    
    logger.info("=" * 80)
    
    return success


def run_step1_only(source_name: Optional[str] = None) -> bool:
    """Exécute seulement l'étape 1 (téléchargement)"""
    logger.info("📥 Exécution : Étape 1 uniquement (Téléchargement)")
    
    try:
        resultats = download_data(source_name)
        success = all(resultats.values()) if resultats else False
        
        if success:
            logger.info("✅ Téléchargement réussi")
        else:
            logger.error("❌ Téléchargement échoué")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        return False


def run_step2_only(timestamp: Optional[str] = None, date: Optional[str] = None) -> bool:
    """Exécute seulement l'étape 2 (chargement BigQuery)"""
    logger.info("📤 Exécution : Étape 2 uniquement (Chargement BigQuery)")
    
    try:
        success = charger_batch_vers_bigquery(timestamp=timestamp, date=date)
        
        if success:
            logger.info("✅ Chargement réussi")
        else:
            logger.error("❌ Chargement échoué")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        return False


def run_step3_only(timestamp_filter: Optional[str] = None, list_only: bool = False) -> bool:
    """
    Exécute seulement l'étape 3 (transformation) ou liste les timestamps
    
    Args:
        timestamp_filter: Timestamp pour filtrer les vues (None = le plus récent)
        list_only: Si True, liste uniquement les timestamps sans créer de vues
    """
    if list_only:
        logger.info("📋 Liste des timestamps disponibles")
        logger.info("-" * 80)
        
        try:
            timestamps = obtenir_timestamps_disponibles()
            
            if timestamps:
                logger.info(f"\n{len(timestamps)} timestamp(s) trouvé(s) :\n")
                for i, ts in enumerate(timestamps, 1):
                    marker = "⭐ (plus récent)" if i == 1 else ""
                    logger.info(f"  {i:2d}. {ts.strftime('%Y-%m-%d %H:%M:%S')} {marker}")
                return True
            else:
                logger.warning("Aucun timestamp trouvé")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erreur : {e}")
            return False
    
    else:
        logger.info("🔄 Exécution : Étape 3 uniquement (Transformation)")
        
        try:
            resultats = transform_data(timestamp=timestamp_filter)
            success = all(resultats.values()) if resultats else False
            
            if success:
                logger.info("✅ Transformation réussie")
            else:
                logger.error("❌ Transformation échouée")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Erreur : {e}")
            return False


# CLI simple
if __name__ == "__main__":
    import sys
    
    # Pas d'argument = pipeline complet
    if len(sys.argv) == 1:
        success = run_pipeline()
    
    # Avec argument
    else:
        cmd = sys.argv[1].lower()
        
        if cmd == "step1":
            # python -m functions.orchestrator step1 [source_name]
            source = sys.argv[2] if len(sys.argv) > 2 else None
            success = run_step1_only(source)
        
        elif cmd == "step2":
            # python -m functions.orchestrator step2
            success = run_step2_only()
        
        elif cmd == "step3":
            # python -m functions.orchestrator step3 [timestamp]
            timestamp = sys.argv[2] if len(sys.argv) > 2 else None
            success = run_step3_only(timestamp_filter=timestamp, list_only=False)
        
        elif cmd == "list":
            # python -m functions.orchestrator list
            success = run_step3_only(list_only=True)
        
        else:
            print("Usage:")
            print("  python -m functions.orchestrator               # Pipeline complet")
            print("  python -m functions.orchestrator step1         # Téléchargement seul")
            print("  python -m functions.orchestrator step2         # Chargement seul")
            print("  python -m functions.orchestrator step3         # Transformation seule (timestamp récent)")
            print("  python -m functions.orchestrator step3 <ts>    # Transformation avec timestamp spécifique")
            print("  python -m functions.orchestrator list          # Liste les timestamps disponibles")
            sys.exit(1)
    
    sys.exit(0 if success else 1)