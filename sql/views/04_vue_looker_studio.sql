CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_looker_studio` AS
WITH ratios_enriched AS (
    SELECT
        r.*,

        -- Indicateurs simples
        SAFE_DIVIDE(r.marge_brute, r.chiffre_d_affaires) AS marge_brute_sur_ca,
        SAFE_DIVIDE(r.resultat_net, r.chiffre_d_affaires) AS resultat_net_sur_ca,

        -- Classes et zones
        CASE 
            WHEN r.marge_ebe < 0 THEN '<0%'
            WHEN r.marge_ebe < 5 THEN '0–5%'
            WHEN r.marge_ebe < 10 THEN '5–10%'
            WHEN r.marge_ebe < 15 THEN '10–15%'
            WHEN r.marge_ebe < 20 THEN '15–20%'
            ELSE '>20%' 
        END AS classe_marge_ebe,

        CASE 
            WHEN r.taux_d_endettement IS NULL OR r.autonomie_financiere IS NULL THEN 'NON CLASSE'
            WHEN r.taux_d_endettement <= 80 AND r.autonomie_financiere >= 25 THEN 'SAIN'
            WHEN r.taux_d_endettement <= 150 AND r.autonomie_financiere >= 15 THEN 'VIGILANCE'
            ELSE 'RISQUE ÉLEVÉ'
        END AS zone_solvabilite,

        CASE WHEN r.taux_d_endettement > 80 THEN TRUE ELSE FALSE END AS alerte_endettement_flag,
        CASE WHEN r.autonomie_financiere < 20 THEN TRUE ELSE FALSE END AS alerte_autonomie_flag,
        CASE WHEN r.taux_d_endettement > 80 OR r.autonomie_financiere < 20 THEN TRUE ELSE FALSE END AS alerte_solvabilite,

        CASE 
            WHEN r.ratio_de_liquidite IS NULL THEN 'NON CLASSE'
            WHEN r.ratio_de_liquidite < 1 THEN 'RISQUE'
            WHEN r.ratio_de_liquidite < 1.5 THEN 'VIGILANCE'
            ELSE 'CONFORTABLE'
        END AS zone_liquidite,

        CASE WHEN r.ratio_de_liquidite < 1 THEN TRUE ELSE FALSE END AS alerte_liquidite_flag,

        CASE 
            WHEN r.caf_sur_ca IS NULL THEN 'NON CLASSE'
            WHEN r.caf_sur_ca < 2 THEN '<2%'
            WHEN r.caf_sur_ca < 4 THEN '2–4%'
            WHEN r.caf_sur_ca < 6 THEN '4–6%'
            WHEN r.caf_sur_ca < 8 THEN '6–8%'
            ELSE '>8%' 
        END AS classe_caf_sur_ca,

        CASE 
            WHEN r.capacite_de_remboursement IS NULL THEN 'NON CLASSE'
            WHEN r.capacite_de_remboursement <= 3 THEN 'CONFORTABLE'
            WHEN r.capacite_de_remboursement <= 5 THEN 'VIGILANCE'
            ELSE 'RISQUE'
        END AS zone_capacite_remboursement,

        -- Cycle d'exploitation
        (r.rotation_des_stocks_jours + r.credit_clients_jours - r.credit_fournisseurs_jours)
            AS cycle_conversion_tresorerie_jours,

        CASE 
            WHEN r.poids_bfr_exploitation_sur_ca_jours IS NULL THEN 'NON CLASSE'
            WHEN r.poids_bfr_exploitation_sur_ca_jours < 0 THEN 'BFR NÉGATIF (RESSOURCE)'
            WHEN r.poids_bfr_exploitation_sur_ca_jours <= 30 THEN 'MODÉRÉ'
            WHEN r.poids_bfr_exploitation_sur_ca_jours <= 60 THEN 'ÉLEVÉ'
            ELSE 'TRÈS ÉLEVÉ'
        END AS zone_bfr_jours,

        CASE WHEN r.poids_bfr_exploitation_sur_ca_jours > 60 THEN TRUE ELSE FALSE END AS alerte_bfr,

        CASE 
            WHEN r.poids_bfr_exploitation_sur_ca_jours < -10 THEN '<-10 j'
            WHEN r.poids_bfr_exploitation_sur_ca_jours < 0 THEN '-10–0 j'
            WHEN r.poids_bfr_exploitation_sur_ca_jours < 20 THEN '0–20 j'
            WHEN r.poids_bfr_exploitation_sur_ca_jours < 40 THEN '20–40 j'
            ELSE '>40 j'
        END AS classe_bfr_jours,

        CASE 
            WHEN r.rotation_des_stocks_jours IS NULL THEN 'NON CLASSE'
            WHEN r.rotation_des_stocks_jours <= 30 THEN 'RAPIDE'
            WHEN r.rotation_des_stocks_jours <= 60 THEN 'NORMAL'
            ELSE 'LENT'
        END AS zone_rotation_stocks,

        CASE 
            WHEN r.credit_clients_jours IS NULL THEN 'NON CLASSE'
            WHEN r.credit_clients_jours <= 30 THEN 'COURT'
            WHEN r.credit_clients_jours <= 60 THEN 'NORMAL'
            ELSE 'LONG'
        END AS zone_credit_clients,

        CASE 
            WHEN r.credit_fournisseurs_jours IS NULL THEN 'NON CLASSE'
            WHEN r.credit_fournisseurs_jours <= 30 THEN 'COURT'
            WHEN r.credit_fournisseurs_jours <= 60 THEN 'NORMAL'
            ELSE 'LONG'
        END AS zone_credit_fournisseurs,

        CASE WHEN r.resultat_net < 0 THEN TRUE ELSE FALSE END AS alerte_resultat_net_negatif,
        CASE WHEN r.capacite_de_remboursement > 5 THEN TRUE ELSE FALSE END AS alerte_remboursement_flag,

        -- Score risque
        (
            (CASE WHEN r.taux_d_endettement > 150 THEN 1 ELSE 0 END) +
            (CASE WHEN r.autonomie_financiere < 15 THEN 1 ELSE 0 END) +
            (CASE WHEN r.ratio_de_liquidite < 1 THEN 1 ELSE 0 END) +
            (CASE WHEN r.capacite_de_remboursement > 5 THEN 1 ELSE 0 END) +
            (CASE WHEN r.poids_bfr_exploitation_sur_ca_jours > 60 THEN 1 ELSE 0 END)
        ) AS nb_alertes_dures,

        LEAST(
            100,
            20 * (
                (CASE WHEN r.taux_d_endettement > 150 THEN 1 ELSE 0 END) +
                (CASE WHEN r.autonomie_financiere < 15 THEN 1 ELSE 0 END) +
                (CASE WHEN r.ratio_de_liquidite < 1 THEN 1 ELSE 0 END) +
                (CASE WHEN r.capacite_de_remboursement > 5 THEN 1 ELSE 0 END) +
                (CASE WHEN r.poids_bfr_exploitation_sur_ca_jours > 60 THEN 1 ELSE 0 END)
            )
        ) AS score_risque_simple

    FROM `{project_id}.{dataset}.v_ratios_cleaned` AS r
),

ratios_with_classe AS (
    SELECT
        *,
        CASE 
            WHEN score_risque_simple IS NULL THEN 'NON CLASSE'
            WHEN score_risque_simple < 25 THEN 'FAIBLE'
            WHEN score_risque_simple < 60 THEN 'MOYEN'
            ELSE 'ÉLEVÉ'
        END AS classe_risque_globale,

        CASE 
            WHEN score_risque_simple > 70 THEN 'Critique'
            WHEN score_risque_simple > 40 THEN 'Vigilance'
            ELSE 'OK'
        END AS signal_risque
    FROM ratios_enriched
)

-- Jointure finale avec la table stock
SELECT
    r.*,
    s.* EXCEPT(siren, extraction_timestamp, extraction_date)
FROM ratios_with_classe r
INNER JOIN `{project_id}.{dataset}.v_stock_cleaned` s
    ON r.siren = s.siren;