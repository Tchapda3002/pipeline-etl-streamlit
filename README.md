# 📊 Pipeline ELT BigQuery avec Streamlit

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-red.svg)
![GCP](https://img.shields.io/badge/GCP-BigQuery-yellow.svg)

> Pipeline ETL moderne avec interface Streamlit pour l'extraction, le chargement et la transformation de données vers BigQuery

## 👤 Auteur

- **TCHAPDA KOUADJO Wilfred Rod**
- **Pape Magette DIOP**
- **Fatou Soumaya WADE**
- **Naba Ahmadou Seydou TOURE**

## 🎯 Vue d'ensemble

Ce projet implémente un pipeline ELT (Extract, Load, Transform) complet avec une interface web moderne développée en Streamlit. Il permet d'automatiser le flux de données depuis des sources externes (Data.gouv, INPI) vers Google BigQuery, en passant par Google Cloud Storage pour en suite permettre une visualisation des KPI financiers sur looker studio.

### ✨ Fonctionnalités principales

- **Extraction automatisée** - Téléchargement des données depuis Data.gouv et l'INPI
- **Stockage Cloud** - Conversion en format Parquet et stockage dans GCS
- **Chargement BigQuery** - Import automatique dans des tables BigQuery
- **Transformation SQL** - Création de vues nettoyées et enrichies
- **Interface moderne** - Dashboard Streamlit 
- **Visualisation** - Intégration avec Looker Studio

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Sources de Données                        │
│              (Data.gouv, INPI, APIs)                        │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  1. EXTRACTION                               │
│    • Téléchargement HTTP/API                                │
│    • Conversion Parquet                                      │
│    • Horodatage des batchs                                   │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Google Cloud Storage (GCS)                      │
│    gs://bucket/raw_data/table__timestamp.parquet            │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  2. CHARGEMENT                               │
│    • Import BigQuery                                         │
│    • Tables raw avec timestamp                               │
│    • Gestion des schémas                                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                 BigQuery - Tables Raw                        │
│    • ratios_inpi_raw                                        │
│    • stock_entreprises_raw                                  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  3. TRANSFORMATION                           │
│    • Nettoyage SQL                                          │
│    • Enrichissement                                          │
│    • Création de vues                                        │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              BigQuery - Vues Transformées                    │
│    • v_ratios_cleaned                                       │
│    • v_stock_cleaned                                        │
│    • v_looker_studio (vue finale)                           │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   Looker Studio                              │
│              Dashboards & Visualisations                     │
└─────────────────────────────────────────────────────────────┘
```

## 📦 Structure du Projet

```
pipeline-etl-streamlit/
├── config/
│   ├── __init__.py
│   ├── config.yaml              # Configuration principale
│   └── gcp-credentials.json     # Clés GCP (non versionné)
│
├── functions/
│   ├── __init__.py
│   ├── step1_download.py        # Extraction des données
│   ├── step2_load.py            # Chargement BigQuery
│   ├── step3_transform.py       # Transformation SQL
│   └── orchestrator.py          # Orchestration du pipeline
│
├── interface/
│   ├──app.py                    # Application web Streamlit
│   └── .streamlit/
│        ├── config.toml
│        ├── secrets.toml
│           
├── venv/                        # Environnement virtuel
├── .env                         # Variables d'environnement
├── requirements.txt             # Dépendances Python
├── README.md                    # Documentation
├── Dockerfile.streamlit         # Conteneur pour l'application streamlit
└── .gitignore                   # Fichiers à ignorer
```

## 🚀 Installation

### Prérequis

- Python 3.9+
- Compte Google Cloud Platform avec :
  - BigQuery API activée
  - Cloud Storage API activée
  - Compte de service avec permissions appropriées
- Git

### Installation locale

1. **Cloner le repository**

```bash
git clone https://github.com/Tchapda3002/pipeline-etl-streamlit.git
cd pipeline-etl-streamlit
```

2. **Créer un environnement virtuel**

```bash
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate
```

3. **Installer les dépendances**

```bash
pip install -r requirements.txt
```

4. **Configuration GCP**

Créer le fichier `config/gcp-credentials.json` avec vos credentials GCP :

```json
{
  "type": "service_account",
  "project_id": "votre-projet-id",
  "private_key_id": "...",
  "private_key": "...",
  "client_email": "...",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "..."
}
```

5. **Configurer le fichier config.yaml**

```yaml
gcp:
  project_id: "votre-projet-id"
  region: "europe-west1"
  credentials_path: "config/gcp-credentials.json"

storage:
  bucket_name: "votre-bucket"
  raw_folder: "raw_data"

bigquery:
  dataset: "votre_dataset"

data_sources:
  sources:
    - name: "Stock entreprises"
      url: "https://data.gouv.fr/..."
      active: true
      description: "Données d'entreprises"
    # Ajouter vos sources ici
```

6. **Lancer l'application**

```bash
streamlit run interface/streamlit_app.py
```

L'application sera accessible à `http://localhost:8501`

## ☁️ Déploiement sur Streamlit Cloud

### Configuration des secrets

1. Aller sur [Streamlit Cloud](https://share.streamlit.io/)
2. Connecter votre repository GitHub
3. Dans **Advanced settings** > **Secrets**, ajouter :

```toml
[gcp]
project_id = "votre-projet-id"
type = "service_account"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "..."
```

### Déploiement

```bash
git add .
git commit -m "Configuration déploiement"
git push origin main
```

Streamlit Cloud déploiera automatiquement votre application.

## 🎮 Utilisation

### Ligne de commande (CLI)

Vous pouvez exécuter chaque étape du pipeline directement depuis le terminal :

#### **Étape 1 : Extraction**

```bash
# Extraire toutes les sources
python -m functions.step1_download

# Extraire une source spécifique
python -m functions.step1_download --source "Stock entreprises"
```

#### **Étape 2 : Chargement**

```bash
# Lister les batchs disponibles dans Cloud Storage
python -m functions.step2_load list

# Charger le batch le plus récent (par défaut)
python -m functions.step2_load

# Charger un timestamp spécifique
python -m functions.step2_load --timestamp "20241210_14-30-00"
```

#### **Étape 3 : Transformation**

```bash
# Lister les timestamps disponibles dans BigQuery
python -m functions.step3_transform list

# Créer les vues avec le timestamp le plus récent (par défaut)
python -m functions.step3_transform

# Créer les vues avec un timestamp spécifique
python -m functions.step3_transform --timestamp "2024-12-10T14:30:00"
```

#### **Pipeline complet**

```bash
# Exécuter tout le pipeline (extraction → chargement → transformation)
python -m functions.orchestrator

# Ignorer certaines étapes
python -m functions.orchestrator --skip-download  # Ignorer l'extraction
python -m functions.orchestrator --skip-load      # Ignorer le chargement

# Extraire une source spécifique
python -m functions.orchestrator --source "Stock entreprises"
```

### Interface Streamlit

L'application propose 5 onglets principaux :

#### 1. **Accueil**
- Vue d'ensemble des métriques
- Nombre de batchs disponibles
- Statistiques BigQuery
- Historique des timestamps

#### 2. **Extraction**
- Sélection de la source de données
- Téléchargement et conversion en Parquet
- Logs en temps réel
- Bouton d'arrêt

#### 3. **Chargement**
- Sélection du batch à charger
- Import vers BigQuery
- Tables raw créées automatiquement
- Suivi de l'exécution

#### 4. **Transformation**
- Sélection du timestamp
- Création des vues SQL
- Nettoyage et enrichissement
- Lien vers Looker Studio

#### 5. **Pipeline Complet**
- Exécution des 3 étapes en séquence
- Options d'ignorer certaines étapes
- Logs en temps réel
- Durée totale d'exécution

### Exemple d'utilisation

**Workflow recommandé pour débutants :**

**Via l'interface Streamlit :**
```bash
1. Aller dans "Pipeline Complet"
2. Cliquer sur "Lancer le pipeline"
3. Attendre 5-10 minutes
4. Consulter le tableau de bord Looker Studio
```

**Via la ligne de commande :**
```bash
# Pipeline complet en une commande
python -m functions.orchestrator
```

**Workflow pas à pas :**

**Via l'interface Streamlit :**
```bash
1. Extraction → Télécharger les données
2. Chargement → Importer dans BigQuery
3. Transformation → Créer les vues nettoyées
4. Visualisation → Ouvrir Looker Studio
```

**Via la ligne de commande :**
```bash
# Étape 1 : Extraction
python -m functions.step1_download

# Étape 2 : Lister les batchs disponibles
python -m functions.step2_load list

# Charger le batch le plus récent
python -m functions.step2_load

# Étape 3 : Lister les timestamps BigQuery
python -m functions.step3_transform list

# Créer les vues avec le timestamp le plus récent
python -m functions.step3_transform

# Ou tout en une fois
python -m functions.orchestrator
```

**Cas d'usage avancés :**

```bash
# Extraire uniquement les données INPI
python -m functions.step1_download --source "Ratios INPI"

# Charger un batch spécifique
python -m functions.step2_load --timestamp "20241210_09-15-00"

# Créer les vues pour un timestamp spécifique
python -m functions.step3_transform --timestamp "2024-12-10T09:15:00"

# Pipeline sans extraction (données déjà téléchargées)
python -m functions.orchestrator --skip-download

# Pipeline sans chargement (données déjà dans BigQuery)
python -m functions.orchestrator --skip-load

# Pipeline avec une source spécifique
python -m functions.orchestrator --source "Stock entreprises"
```

## 🔧 Configuration GCP

### Permissions requises

Le compte de service GCP doit avoir les rôles suivants :

- **Storage Object Admin** - Pour créer/lire les objets dans GCS
- **BigQuery Data Editor** - Pour créer des tables et vues
- **BigQuery Job User** - Pour exécuter des requêtes

### Commandes gcloud utiles

```bash
# Créer un bucket
gsutil mb -p votre-projet-id -l europe-west1 gs://votre-bucket/

# Créer un dataset BigQuery
bq mk --dataset votre-projet-id:votre_dataset

# Lister les tables
bq ls votre-projet-id:votre_dataset

# Vérifier les permissions
gcloud projects get-iam-policy votre-projet-id
```

## 📊 Sources de données

Le pipeline peut extraire des données depuis :

- **Data.gouv.fr** - Données ouvertes du gouvernement français
- **INPI** - Données de l'Institut National de la Propriété Industrielle
- **APIs personnalisées** - Ajout de nouvelles sources via configuration

### Ajouter une nouvelle source

Dans `config/config.yaml` :

```yaml
data_sources:
  sources:
    - name: "Ma nouvelle source"
      url: "https://api.example.com/data"
      active: true
      description: "Description de la source"
      format: "csv"  # ou json, parquet, etc.
```

## 🔧 Référence CLI

### Commandes disponibles

#### **step1_download** - Extraction des données

```bash
# Syntaxe
python -m functions.step1_download [--source SOURCE_NAME]

# Arguments
--source    Nom de la source à extraire (optionnel)
            Si non spécifié, toutes les sources actives sont extraites

# Exemples
python -m functions.step1_download                           # Toutes les sources
python -m functions.step1_download --source "Stock entreprises"
python -m functions.step1_download --source "Ratios INPI"
```

**Sortie :**
- Fichiers Parquet dans Cloud Storage : `gs://bucket/raw_data/table__timestamp.parquet`
- Format timestamp : `YYYYMMDD_HH-MM-SS`

#### **step2_load** - Chargement BigQuery

```bash
# Syntaxe
python -m functions.step2_load [list|load] [--timestamp TIMESTAMP]

# Commandes
list                Affiche tous les batchs disponibles dans GCS
load (défaut)       Charge un batch dans BigQuery

# Arguments
--timestamp    Timestamp du batch à charger (optionnel)
               Format: YYYYMMDD_HH-MM-SS
               Si non spécifié, le batch le plus récent est utilisé

# Exemples
python -m functions.step2_load list                           # Liste les batchs
python -m functions.step2_load                                # Charge le plus récent
python -m functions.step2_load --timestamp "20241210_14-30-00"
```

**Sortie :**
- Tables BigQuery créées/remplacées :
  - `projet.dataset.ratios_inpi_raw`
  - `projet.dataset.stock_entreprises_raw`
- Colonne `extraction_timestamp` ajoutée automatiquement

#### **step3_transform** - Transformation SQL

```bash
# Syntaxe
python -m functions.step3_transform [list|transform] [--timestamp TIMESTAMP]

# Commandes
list                  Affiche tous les timestamps disponibles dans BigQuery
transform (défaut)    Crée les vues transformées

# Arguments
--timestamp    Timestamp pour filtrer les vues (optionnel)
               Format: YYYY-MM-DDTHH:MM:SS (ISO 8601)
               Si non spécifié, le timestamp le plus récent est utilisé

# Exemples
python -m functions.step3_transform list                      # Liste les timestamps
python -m functions.step3_transform                           # Transforme le plus récent
python -m functions.step3_transform --timestamp "2024-12-10T14:30:00"
```

**Sortie :**
- Vues BigQuery créées/remplacées :
  - `projet.dataset.v_ratios_cleaned`
  - `projet.dataset.v_stock_cleaned`
  - `projet.dataset.v_looker_studio`

#### **orchestrator** - Pipeline complet

```bash
# Syntaxe
python -m functions.orchestrator [OPTIONS]

# Arguments
--source          Nom de la source pour l'extraction (optionnel)
--skip-download   Ignore l'étape d'extraction
--skip-load       Ignore l'étape de chargement

# Exemples
python -m functions.orchestrator                              # Pipeline complet
python -m functions.orchestrator --skip-download              # Sans extraction
python -m functions.orchestrator --skip-load                  # Sans chargement
python -m functions.orchestrator --source "Stock entreprises"
python -m functions.orchestrator --skip-download --skip-load  # Transformation uniquement
```

**Sortie :**
- Exécution séquentielle : Extraction → Chargement → Transformation
- Rapport de succès/échec pour chaque étape
- Durée totale d'exécution

### Scénarios d'utilisation CLI

#### **Automatisation avec cron**

```bash
# Ajouter au crontab (crontab -e)

# Exécution quotidienne à 2h du matin
0 2 * * * cd /chemin/vers/projet && /chemin/vers/venv/bin/python -m functions.orchestrator

# Exécution toutes les 6 heures
0 */6 * * * cd /chemin/vers/projet && /chemin/vers/venv/bin/python -m functions.orchestrator --skip-download
```

#### **Scripts bash**

```bash
#!/bin/bash
# pipeline.sh - Script d'automatisation

set -e  # Arrêter en cas d'erreur

echo "Démarrage du pipeline ETL..."

# Activer l'environnement virtuel
source venv/bin/activate

# Extraction
echo "Étape 1/3 : Extraction..."
python -m functions.step1_download

# Chargement
echo "Étape 2/3 : Chargement..."
python -m functions.step2_load

# Transformation
echo "Étape 3/3 : Transformation..."
python -m functions.step3_transform

echo "Pipeline terminé avec succès !"
```

#### **Intégration CI/CD**

```yaml
# .github/workflows/etl-pipeline.yml
name: ETL Pipeline

on:
  schedule:
    - cron: '0 2 * * *'  # Tous les jours à 2h
  workflow_dispatch:      # Déclenchement manuel

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      
      - name: Run pipeline
        env:
          GCP_CREDENTIALS: ${{ secrets.GCP_CREDENTIALS }}
        run: |
          echo "$GCP_CREDENTIALS" > config/gcp-credentials.json
          python -m functions.orchestrator
```

## 🐛 Dépannage

### Problèmes courants

#### Erreur : "Impossible de se connecter à GCP"

**Solution :**
- Vérifier que `gcp-credentials.json` existe
- Vérifier les permissions du compte de service
- Vérifier le `project_id` dans config.yaml

#### Erreur : "Aucun batch disponible"

**Solution :**
- Lancer d'abord l'extraction
- Vérifier que les fichiers sont dans GCS
- Vérifier le préfixe `raw_folder` dans config.yaml

#### Le pipeline est lent

**Solution :**
- Utiliser "Ignorer l'extraction" si les données sont déjà téléchargées
- Utiliser "Ignorer le chargement" si les données sont déjà dans BigQuery
- Vérifier la connexion internet

### Logs et debugging

Pour activer les logs détaillés :

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Inspecter les données dans BigQuery :

```sql
-- Vérifier les données raw
SELECT * FROM `projet.dataset.ratios_inpi_raw` LIMIT 10;

-- Vérifier les timestamps disponibles
SELECT DISTINCT extraction_timestamp 
FROM `projet.dataset.ratios_inpi_raw`
ORDER BY extraction_timestamp DESC;

-- Vérifier les vues transformées
SELECT COUNT(*) FROM `projet.dataset.v_looker_studio`;
```

## 🔐 Sécurité

### Bonnes pratiques

- ❌ **Ne jamais** commiter `gcp-credentials.json`
- ✅ Utiliser `.gitignore` pour exclure les fichiers sensibles
- ✅ Utiliser des secrets Streamlit Cloud pour le déploiement
- ✅ Limiter les permissions du compte de service au strict minimum
- ✅ Activer l'authentification sur Streamlit Cloud si nécessaire

### Fichiers à ne pas versionner

```gitignore
# Credentials
config/gcp-credentials.json
.streamlit/secrets.toml

# Environment
.env
venv/
__pycache__/

# Data
data/
*.parquet
*.csv
```


- Repository: [pipeline-etl-streamlit](https://github.com/Tchapda3002/pipeline-etl-streamlit)

## 🙏 Remerciements

- Madame Mously DIAW, Enseignante de Big Data et Clous Computing
- [Streamlit](https://streamlit.io/) - Framework web Python
- [Google Cloud Platform](https://cloud.google.com/) - Infrastructure cloud
- [Data.gouv.fr](https://data.gouv.fr/) - Données ouvertes françaises
- [INPI](https://www.inpi.fr/) - Données propriété industrielle

## 📚 Ressources

### Documentation

- [Documentation Streamlit](https://docs.streamlit.io/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [Looker Studio](https://lookerstudio.google.com/)

### Tutoriels

- [Getting Started with BigQuery](https://cloud.google.com/bigquery/docs/quickstarts)
- [Streamlit Tutorial](https://docs.streamlit.io/get-started/tutorials)
- [GCP Best Practices](https://cloud.google.com/docs/enterprise/best-practices-for-enterprise-organizations)

---

