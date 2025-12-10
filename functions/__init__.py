"""
Package functions - Pipeline ETL
"""

__version__ = "1.0.0"
__all__ = []

# Import conditionnel des fonctions principales
try:
    from .step1_download import download_data
    __all__.append("download_data")
except ImportError:
    pass

try:
    from .step2_load import charger_batch_vers_bigquery, lister_fichiers_par_timestamp
    __all__.extend(["charger_batch_vers_bigquery", "lister_fichiers_par_timestamp"])
except ImportError:
    pass

try:
    from .step3_transform import transform_data, obtenir_timestamps_disponibles
    __all__.append("transform_data", "obtenir_timestamps_disponibles")
except ImportError:
    pass


try:
    from .orchestrator import *
    __all__.append("")
except ImportError:
    pass
