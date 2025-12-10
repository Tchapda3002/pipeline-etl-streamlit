CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_stock_cleaned` AS
WITH first_element AS (
    SELECT
      sl.siren,
      sl.nomUniteLegale,
      sl.categorieEntreprise,
      sl.etatAdministratifUniteLegale,
      sl.categorieJuridiqueUniteLegale,
      sl.activitePrincipaleUniteLegale,
      sl.nomenclatureActivitePrincipaleUniteLegale,
      sl.economieSocialeSolidaireUniteLegale,
      sl.trancheEffectifsUniteLegale,
      sl.extraction_timestamp,
      sl.extraction_date
    FROM `{project_id}.{dataset}.stock_entreprises_raw` AS sl
    WHERE sl.siren IS NOT NULL
      AND sl.trancheEffectifsUniteLegale IS NOT NULL
      AND sl.categorieEntreprise IS NOT NULL
      AND sl.etatAdministratifUniteLegale IS NOT NULL
      AND sl.categorieJuridiqueUniteLegale IS NOT NULL
      AND sl.activitePrincipaleUniteLegale IS NOT NULL
      AND sl.nomenclatureActivitePrincipaleUniteLegale IS NOT NULL
      AND sl.economieSocialeSolidaireUniteLegale IS NOT NULL
      {timestamp_filter}
),

second_element AS (
    SELECT
      t.*,
      COALESCE(
        cj.categorieJuridique,
        CAST(t.categorieJuridiqueUniteLegale AS STRING)
      ) AS categorieJuridique
    FROM first_element AS t
    LEFT JOIN UNNEST([
        STRUCT("0" AS code_niveau_I, "Organisme de placement collectif en valeurs mobilières sans personnalité morale" AS categorieJuridique),
        STRUCT("1","Entrepreneur individuel"),
        STRUCT("2","Groupement de droit privé non doté de la personnalité morale"),
        STRUCT("3","Personne morale de droit étranger"),
        STRUCT("4","Personne morale de droit public soumise au droit commercial"),
        STRUCT("5","Société commerciale"),
        STRUCT("6","Autre personne morale immatriculée au RCS"),
        STRUCT("7","Personne morale et organisme soumis au droit administratif"),
        STRUCT("8","Organisme privé spécialisé"),
        STRUCT("9","Groupement de droit privé")
    ]) AS cj
    ON SUBSTR(CAST(t.categorieJuridiqueUniteLegale AS STRING), 1, 1) = cj.code_niveau_I
),

third_element AS (
    SELECT
      t.*,
      COALESCE(am.macro_section, "Non classé") AS secteur_activite
    FROM second_element AS t
    LEFT JOIN UNNEST(
        [
          -- Secteur primaire_Ressources naturelles
          STRUCT("01" AS code, "Secteur primaire_Ressources naturelles" AS macro_section),
          STRUCT("02", "Secteur primaire_Ressources naturelles"),
          STRUCT("03", "Secteur primaire_Ressources naturelles"),
          STRUCT("05", "Secteur primaire_Ressources naturelles"),
          STRUCT("06", "Secteur primaire_Ressources naturelles"),
          STRUCT("07", "Secteur primaire_Ressources naturelles"),
          STRUCT("08", "Secteur primaire_Ressources naturelles"),
          STRUCT("09", "Secteur primaire_Ressources naturelles"),

          -- Industrie, énergie & environnement
          STRUCT("10", "Industrie, énergie & environnement"),
          STRUCT("11", "Industrie, énergie & environnement"),
          STRUCT("12", "Industrie, énergie & environnement"),
          STRUCT("13", "Industrie, énergie & environnement"),
          STRUCT("14", "Industrie, énergie & environnement"),
          STRUCT("15", "Industrie, énergie & environnement"),
          STRUCT("16", "Industrie, énergie & environnement"),
          STRUCT("17", "Industrie, énergie & environnement"),
          STRUCT("18", "Industrie, énergie & environnement"),
          STRUCT("19", "Industrie, énergie & environnement"),
          STRUCT("20", "Industrie, énergie & environnement"),
          STRUCT("21", "Industrie, énergie & environnement"),
          STRUCT("22", "Industrie, énergie & environnement"),
          STRUCT("23", "Industrie, énergie & environnement"),
          STRUCT("24", "Industrie, énergie & environnement"),
          STRUCT("25", "Industrie, énergie & environnement"),
          STRUCT("26", "Industrie, énergie & environnement"),
          STRUCT("27", "Industrie, énergie & environnement"),
          STRUCT("28", "Industrie, énergie & environnement"),
          STRUCT("29", "Industrie, énergie & environnement"),
          STRUCT("30", "Industrie, énergie & environnement"),
          STRUCT("31", "Industrie, énergie & environnement"),
          STRUCT("32", "Industrie, énergie & environnement"),
          STRUCT("33", "Industrie, énergie & environnement"),
          STRUCT("35", "Industrie, énergie & environnement"),
          STRUCT("36", "Industrie, énergie & environnement"),
          STRUCT("37", "Industrie, énergie & environnement"),
          STRUCT("38", "Industrie, énergie & environnement"),
          STRUCT("39", "Industrie, énergie & environnement"),

          -- Bâtiment & infrastructures
          STRUCT("41", "Bâtiment & infrastructures"),
          STRUCT("42", "Bâtiment & infrastructures"),
          STRUCT("43", "Bâtiment & infrastructures"),

          -- Commerce, transport & tourisme
          STRUCT("45", "Commerce, transport & tourisme"),
          STRUCT("46", "Commerce, transport & tourisme"),
          STRUCT("47", "Commerce, transport & tourisme"),
          STRUCT("49", "Commerce, transport & tourisme"),
          STRUCT("50", "Commerce, transport & tourisme"),
          STRUCT("51", "Commerce, transport & tourisme"),
          STRUCT("52", "Commerce, transport & tourisme"),
          STRUCT("53", "Commerce, transport & tourisme"),
          STRUCT("55", "Commerce, transport & tourisme"),
          STRUCT("56", "Commerce, transport & tourisme"),

          -- Économie de la connaissance, finance & immobilier
          STRUCT("58", "Économie de la connaissance, finance & immobilier"),
          STRUCT("59", "Économie de la connaissance, finance & immobilier"),
          STRUCT("60", "Économie de la connaissance, finance & immobilier"),
          STRUCT("61", "Économie de la connaissance, finance & immobilier"),
          STRUCT("62", "Économie de la connaissance, finance & immobilier"),
          STRUCT("63", "Économie de la connaissance, finance & immobilier"),
          STRUCT("64", "Économie de la connaissance, finance & immobilier"),
          STRUCT("65", "Économie de la connaissance, finance & immobilier"),
          STRUCT("66", "Économie de la connaissance, finance & immobilier"),
          STRUCT("68", "Économie de la connaissance, finance & immobilier"),
          STRUCT("69", "Économie de la connaissance, finance & immobilier"),
          STRUCT("70", "Économie de la connaissance, finance & immobilier"),
          STRUCT("71", "Économie de la connaissance, finance & immobilier"),
          STRUCT("72", "Économie de la connaissance, finance & immobilier"),
          STRUCT("73", "Économie de la connaissance, finance & immobilier"),
          STRUCT("74", "Économie de la connaissance, finance & immobilier"),
          STRUCT("75", "Économie de la connaissance, finance & immobilier"),

          -- Services publics, sociaux & à la population
          STRUCT("77", "Services publics, sociaux & à la population"),
          STRUCT("78", "Services publics, sociaux & à la population"),
          STRUCT("79", "Services publics, sociaux & à la population"),
          STRUCT("80", "Services publics, sociaux & à la population"),
          STRUCT("81", "Services publics, sociaux & à la population"),
          STRUCT("82", "Services publics, sociaux & à la population"),
          STRUCT("84", "Services publics, sociaux & à la population"),
          STRUCT("85", "Services publics, sociaux & à la population"),
          STRUCT("86", "Services publics, sociaux & à la population"),
          STRUCT("87", "Services publics, sociaux & à la population"),
          STRUCT("88", "Services publics, sociaux & à la population"),
          STRUCT("90", "Services publics, sociaux & à la population"),
          STRUCT("91", "Services publics, sociaux & à la population"),
          STRUCT("92", "Services publics, sociaux & à la population"),
          STRUCT("93", "Services publics, sociaux & à la population"),
          STRUCT("94", "Services publics, sociaux & à la population"),
          STRUCT("95", "Services publics, sociaux & à la population"),
          STRUCT("96", "Services publics, sociaux & à la population"),

          -- Économie domestique & extraterritoriale
          STRUCT("97", "Économie domestique & extraterritoriale"),
          STRUCT("98", "Économie domestique & extraterritoriale"),
          STRUCT("99", "Économie domestique & extraterritoriale")
        ]
    ) AS am
    ON SUBSTR(t.activitePrincipaleUniteLegale, 1, 2) = am.code
    WHERE SUBSTR(t.activitePrincipaleUniteLegale, 1, 2) != "00"
),

fourth_element AS (
    SELECT
      t.*,
      CASE
        WHEN t.trancheEffectifsUniteLegale = "00" THEN "Sans salarié"
        WHEN t.trancheEffectifsUniteLegale IN ("01","02","03") THEN "1 à 9 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("11","12") THEN "10 à 49 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("21","22") THEN "50 à 199 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("31","32") THEN "200 à 499 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("41","42") THEN "500 à 1999 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("51","52","53") THEN "2000 salariés et plus"
        ELSE "Non renseigné"
      END AS categorie_effectif
    FROM third_element AS t
),

fifth_element AS (
    SELECT
      t.*,
      CASE t.categorieEntreprise
        WHEN "PME" THEN "Petites et Moyennes Entreprises"
        WHEN "ETI" THEN "Entreprises de Taille Intermédiaire"
        WHEN "GE" THEN "Grandes Entreprises"
        ELSE t.categorieEntreprise
      END AS categorie_entreprise,

      CASE t.economieSocialeSolidaireUniteLegale
        WHEN "N" THEN "NON"
        WHEN "O" THEN "OUI"
        ELSE t.economieSocialeSolidaireUniteLegale
      END AS ess_recodee,

      CASE t.etatAdministratifUniteLegale
        WHEN "A" THEN "ACTIF"
        WHEN "C" THEN "CESSÉ"
        ELSE t.etatAdministratifUniteLegale
      END AS etat_administratif
    FROM fourth_element AS t
)

SELECT
  sl.siren,
  sl.nomUniteLegale,
  sl.categorieJuridique,
  sl.secteur_activite,
  sl.categorie_effectif,
  sl.categorie_entreprise,
  sl.ess_recodee,
  sl.etat_administratif,
  sl.extraction_timestamp,
  sl.extraction_date
FROM fifth_element AS sl
QUALIFY ROW_NUMBER() OVER (PARTITION BY siren ORDER BY extraction_timestamp DESC) = 1;
