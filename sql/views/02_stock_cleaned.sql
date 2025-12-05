-- Vue : Nettoyage du stock des unités légales
-- Supprime les valeurs manquantes, recode trancheEffectifsUniteLegale et filtre par timestamp

CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_stock_cleaned` AS
SELECT
  -- Identification
  sl.siren,
  
  -- Recodage de la tranche d'effectifs
  CASE
    WHEN trancheEffectifsUniteLegale IN ('00') THEN '0 salarié'
    WHEN trancheEffectifsUniteLegale IN ('01','02','03') THEN '1–9 salariés'
    WHEN trancheEffectifsUniteLegale IN ('11','12') THEN '10–49 salariés'
    WHEN trancheEffectifsUniteLegale IN ('21','22') THEN '50–199 salariés'
    WHEN trancheEffectifsUniteLegale IN ('31','32') THEN '200–499 salariés'
    WHEN trancheEffectifsUniteLegale IN ('41','42','51','52','53') THEN '500+ salariés'
    ELSE 'Non renseigné'
  END AS classe_effectif,
  
  -- Garder l'originale pour référence
  sl.trancheEffectifsUniteLegale AS tranche_effectifs_originale,
  
  -- Autres informations
  sl.categorieEntreprise,
  sl.etatAdministratifUniteLegale,
  sl.categorieJuridiqueUniteLegale,
  sl.activitePrincipaleUniteLegale,
  sl.economieSocialeSolidaireUniteLegale,
  
  -- Métadonnées temporelles
  sl.extraction_timestamp,
  sl.extraction_date

FROM `{project_id}.{dataset}.stock_entreprises_raw` AS sl

WHERE 
    -- Supprimer les lignes avec SIREN manquant ou invalide
    sl.siren IS NOT NULL
    AND sl.trancheEffectifsUniteLegale IS NOT NULL
    AND sl.categorieEntreprise IS NOT NULL
    AND sl.etatAdministratifUniteLegale IS NOT NULL
    AND sl.categorieJuridiqueUniteLegale IS NOT NULL
    AND sl.activitePrincipaleUniteLegale IS NOT NULL
    AND sl.nomenclatureActivitePrincipaleUniteLegale IS NOT NULL
    AND sl.economieSocialeSolidaireUniteLegale IS NOT NULL
    AND sl.etatAdministratifUniteLegale IS NOT NULL
    {timestamp_filter}

-- Garder uniquement la version la plus récente de chaque SIREN
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY siren
  ORDER BY extraction_timestamp DESC
) = 1;