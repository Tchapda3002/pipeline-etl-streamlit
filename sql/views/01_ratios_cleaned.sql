-- Vue : Nettoyage des ratios financiers
-- Supprime les lignes avec valeurs manquantes et filtre par timestamp

CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_ratios_cleaned` AS
SELECT
  cp.*
FROM `{project_id}.{dataset}.ratios_inpi_raw` AS cp
WHERE 
    cp.siren IS NOT NULL
    AND cp.chiffre_d_affaires IS NOT NULL
    AND cp.date_cloture_exercice IS NOT NULL
    AND cp.marge_brute IS NOT NULL
    AND cp.ebe IS NOT NULL
    AND cp.ebit IS NOT NULL
    AND cp.resultat_net IS NOT NULL
    AND cp.taux_d_endettement IS NOT NULL
    AND cp.ratio_de_liquidite IS NOT NULL
    AND cp.ratio_de_vetuste IS NOT NULL
    AND cp.autonomie_financiere IS NOT NULL
    AND cp.poids_bfr_exploitation_sur_ca IS NOT NULL
    AND cp.couverture_des_interets IS NOT NULL
    AND cp.caf_sur_ca IS NOT NULL
    AND cp.capacite_de_remboursement IS NOT NULL
    AND cp.marge_ebe IS NOT NULL
    AND cp.resultat_courant_avant_impots_sur_ca IS NOT NULL
    AND cp.poids_bfr_exploitation_sur_ca_jours IS NOT NULL
    AND cp.rotation_des_stocks_jours IS NOT NULL
    AND cp.credit_clients_jours IS NOT NULL
    AND cp.credit_fournisseurs_jours IS NOT NULL
    AND cp.type_bilan IS NOT NULL
    {timestamp_filter}
   

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY siren, date_cloture_exercice
  ORDER BY extraction_timestamp DESC
) = 1;