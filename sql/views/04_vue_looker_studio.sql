-- Vue : Jointure entre ratios et stock
-- Combine les données financières et administratives
-- Note : Le filtrage par timestamp est déjà appliqué dans v_ratios_cleaned et v_stock_cleaned

CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_looker_studio` AS
SELECT
    -- Identifiant
    s.siren,

    -- Variables administratives propres
    s.classe_effectif,
    s.tranche_effectifs_originale,
    s.categorieEntreprise,
    s.etatAdministratifUniteLegale,
    s.categorieJuridiqueUniteLegale,
    s.activitePrincipaleUniteLegale,
    s.economieSocialeSolidaireUniteLegale,

    -- Ratios financiers (issus de ratios_cleaned)
    r.date_cloture_exercice,
    r.chiffre_d_affaires,
    r.marge_brute,
    r.ebe,
    r.ebit,
    r.resultat_net,
    r.taux_d_endettement,
    r.ratio_de_liquidite,
    r.ratio_de_vetuste,
    r.autonomie_financiere,
    r.poids_bfr_exploitation_sur_ca,
    r.couverture_des_interets,
    r.caf_sur_ca,
    r.capacite_de_remboursement,
    r.marge_ebe,
    r.resultat_courant_avant_impots_sur_ca,
    r.poids_bfr_exploitation_sur_ca_jours,
    r.rotation_des_stocks_jours,
    r.credit_clients_jours,
    r.credit_fournisseurs_jours,
    r.type_bilan

FROM `{project_id}.{dataset}.v_stock_cleaned` AS s
LEFT JOIN `{project_id}.{dataset}.v_ratios_cleaned` AS r
    ON s.siren = r.siren;