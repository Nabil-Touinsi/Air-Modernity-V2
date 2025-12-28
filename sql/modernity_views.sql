USE air_modernity;

-- ============================================================
-- Étape 7 — Modernity Index (vues d’analyse)
-- Source: fleet_enriched (~55k lignes)
-- ============================================================

-- 1) Vue de base: lignes exploitables (entry_year nettoyé)
CREATE OR REPLACE VIEW v_fleet_base AS
SELECT
  airline_name,
  country,
  region,
  aircraft_type,
  registration,
  total_fleet_size,
  manufacturer,
  model_key,
  CASE
    WHEN entry_year BETWEEN 1900 AND YEAR(CURDATE())
    THEN CAST(entry_year AS UNSIGNED)
    ELSE NULL
  END AS entry_year
FROM fleet_enriched;


-- 2) Stats par compagnie (basé sur entry_year réel)
CREATE OR REPLACE VIEW v_modernity_by_airline AS
SELECT
  airline_name,
  region,
  country,
  COUNT(*) AS aircraft_count,
  AVG(entry_year) AS avg_entry_year,
  SUM(entry_year >= 2015) AS modern_count_2015,
  ROUND(100 * SUM(entry_year >= 2015) / COUNT(*), 2) AS pct_modern_2015,
  SUM(entry_year >= 2010) AS modern_count_2010,
  ROUND(100 * SUM(entry_year >= 2010) / COUNT(*), 2) AS pct_modern_2010
FROM v_fleet_base
WHERE entry_year IS NOT NULL
GROUP BY airline_name, region, country;


-- 3) Stats par région (basé sur entry_year réel)
CREATE OR REPLACE VIEW v_modernity_by_region AS
SELECT
  region,
  COUNT(*) AS aircraft_count,
  AVG(entry_year) AS avg_entry_year,
  ROUND(100 * SUM(entry_year >= 2015) / COUNT(*), 2) AS pct_modern_2015,
  ROUND(100 * SUM(entry_year >= 2010) / COUNT(*), 2) AS pct_modern_2010
FROM v_fleet_base
WHERE entry_year IS NOT NULL
GROUP BY region;


-- ============================================================
-- 4) Vue A — Catégorie d’aéronef (basée sur model_key)
-- Objectif: décrire le dataset (GA / Military / Commercial / Helicopter)
-- ============================================================
CREATE OR REPLACE VIEW v_modernity_by_aircraft_category AS
WITH base AS (
  SELECT
    entry_year,
    model_key,
    manufacturer,
    CASE
      -- Helicopter (indices fréquents)
      WHEN UPPER(model_key) REGEXP '^(EC|AS|AW|UH|MI|KA|SA|H[0-9])'
        OR UPPER(manufacturer) LIKE '%HELICOPTER%'
      THEN 'Helicopter'

      -- Military / State (quelques clés explicites + patterns)
      WHEN UPPER(model_key) IN ('C17','U2','B1','HAWK','UH1','DC10')
        OR UPPER(model_key) REGEXP '^(F|C[0-9]{2}|B[0-9])'
      THEN 'Military / State'

      -- Commercial Jet (Airbus/Boeing/Embraer jets)
      WHEN UPPER(model_key) REGEXP '^A(19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39)'
        OR UPPER(model_key) REGEXP '^B(70|71|72|73|74|75|76|77|78|79|80|81|82|83|84|85|86|87|88|89)'
        OR UPPER(model_key) REGEXP '^E(17|18|19|90|95)'
      THEN 'Commercial Jet'

      -- Commercial Turboprop (ATR / Dash 8 / etc.)
      WHEN UPPER(model_key) REGEXP '^(AT|DH|Q4|SF)'
      THEN 'Commercial Turboprop'

      -- General Aviation (Cessna/Piper/TBM/Diamond/Pilatus…)
      WHEN UPPER(model_key) REGEXP '^(C[0-9]|P[0-9]|PA|PC|TB|SR|DA|BE|CL|LJ|GL|MU|TOBA|CRUZ|Z[0-9])'
      THEN 'General Aviation'

      ELSE 'Other'
    END AS aircraft_category
  FROM v_fleet_base
  WHERE entry_year IS NOT NULL
)
SELECT
  aircraft_category,
  COUNT(*) AS aircraft_count,
  AVG(entry_year) AS avg_entry_year,
  ROUND(100 * SUM(entry_year >= 2015) / COUNT(*), 2) AS pct_modern_2015,
  ROUND(100 * SUM(entry_year >= 2010) / COUNT(*), 2) AS pct_modern_2010
FROM base
GROUP BY aircraft_category;


-- ============================================================
-- 5) Vue intermédiaire — Base estimée (entry_year réel + estimation par model_key)
-- Objectif: permettre une analyse OEM même si entry_year manque (Airbus/Boeing etc.)
-- ============================================================
CREATE OR REPLACE VIEW v_fleet_base_estimated AS
SELECT
  airline_name,
  country,
  region,
  aircraft_type,
  registration,
  total_fleet_size,
  manufacturer,
  model_key,

  -- entry_year réel (si déjà exploitable)
  CASE
    WHEN entry_year BETWEEN 1900 AND YEAR(CURDATE())
    THEN CAST(entry_year AS UNSIGNED)
    ELSE NULL
  END AS entry_year_real,

  -- entry_year estimé (Option A : proxy par familles, basé sur model_key observés)
  CASE
    /* ================= Airbus ================= */
    WHEN model_key LIKE 'A21N%' OR model_key LIKE 'A20N%' THEN 2016
    WHEN model_key LIKE 'Airbus A32%' OR model_key LIKE 'A319%' THEN 1988
    WHEN model_key LIKE 'A332%' OR model_key LIKE 'Airbus A33%' THEN 1994
    WHEN model_key LIKE 'Airbus A359%' THEN 2015
    WHEN model_key LIKE 'A343%' OR model_key LIKE 'A346%' THEN 1993
    WHEN model_key LIKE 'Airbus A400%' THEN 2009
    WHEN model_key LIKE 'Airbus A310%' THEN 1983
    WHEN model_key LIKE 'Airbus A318%' THEN 2003

    /* ================= Boeing ================= */
    WHEN model_key LIKE 'B78%' THEN 2011
    WHEN model_key LIKE 'B748%' THEN 2012
    WHEN model_key LIKE 'B77W%' OR model_key LIKE 'B772%' OR model_key LIKE 'B77%' THEN 1995
    WHEN model_key LIKE 'B763%' OR model_key LIKE 'B762%' OR model_key LIKE 'B76%' THEN 1982
    WHEN model_key LIKE 'B752%' OR model_key LIKE 'B75%' THEN 1983
    WHEN model_key LIKE 'B739%' THEN 2000
    WHEN model_key LIKE 'B738%' OR model_key LIKE 'B737%' OR model_key LIKE 'B73%' THEN 1998
    WHEN model_key LIKE 'B734%' OR model_key LIKE 'B733%' OR model_key LIKE 'B732%' THEN 1984

    /* ================= Bombardier ================= */
    WHEN model_key LIKE 'CRJ%' THEN 1992

    /* ================= Embraer ================= */
    WHEN model_key LIKE 'Embraer E170%' OR model_key LIKE 'Embraer E17%' THEN 2004
    WHEN model_key LIKE 'Embraer E195%' OR model_key LIKE 'Embraer E19%' THEN 2004

    /* ================= ATR ================= */
    WHEN model_key LIKE 'ATR 72%' THEN 1989

    /* ================= McDonnell Douglas ================= */
    WHEN model_key LIKE 'MD11%' THEN 1990
    WHEN model_key LIKE 'MD8%' THEN 1980

    /* ================= Learjet ================= */
    WHEN model_key LIKE 'Learjet 45%' THEN 1997
    WHEN model_key LIKE 'Learjet 60%' THEN 1993
    WHEN model_key LIKE 'Learjet 75%' THEN 2013

    ELSE NULL
  END AS entry_year_est,

  -- Année utilisée (réelle sinon estimée)
  COALESCE(
    CASE
      WHEN entry_year BETWEEN 1900 AND YEAR(CURDATE())
      THEN CAST(entry_year AS UNSIGNED)
      ELSE NULL
    END,
    CASE
      WHEN model_key LIKE 'A21N%' OR model_key LIKE 'A20N%' THEN 2016
      WHEN model_key LIKE 'Airbus A32%' OR model_key LIKE 'A319%' THEN 1988
      WHEN model_key LIKE 'A332%' OR model_key LIKE 'Airbus A33%' THEN 1994
      WHEN model_key LIKE 'Airbus A359%' THEN 2015
      WHEN model_key LIKE 'A343%' OR model_key LIKE 'A346%' THEN 1993
      WHEN model_key LIKE 'Airbus A400%' THEN 2009
      WHEN model_key LIKE 'Airbus A310%' THEN 1983
      WHEN model_key LIKE 'Airbus A318%' THEN 2003
      WHEN model_key LIKE 'B78%' THEN 2011
      WHEN model_key LIKE 'B748%' THEN 2012
      WHEN model_key LIKE 'B77W%' OR model_key LIKE 'B772%' OR model_key LIKE 'B77%' THEN 1995
      WHEN model_key LIKE 'B763%' OR model_key LIKE 'B762%' OR model_key LIKE 'B76%' THEN 1982
      WHEN model_key LIKE 'B752%' OR model_key LIKE 'B75%' THEN 1983
      WHEN model_key LIKE 'B739%' THEN 2000
      WHEN model_key LIKE 'B738%' OR model_key LIKE 'B737%' OR model_key LIKE 'B73%' THEN 1998
      WHEN model_key LIKE 'B734%' OR model_key LIKE 'B733%' OR model_key LIKE 'B732%' THEN 1984
      WHEN model_key LIKE 'CRJ%' THEN 1992
      WHEN model_key LIKE 'Embraer E170%' OR model_key LIKE 'Embraer E17%' THEN 2004
      WHEN model_key LIKE 'Embraer E195%' OR model_key LIKE 'Embraer E19%' THEN 2004
      WHEN model_key LIKE 'ATR 72%' THEN 1989
      WHEN model_key LIKE 'MD11%' THEN 1990
      WHEN model_key LIKE 'MD8%' THEN 1980
      WHEN model_key LIKE 'Learjet 45%' THEN 1997
      WHEN model_key LIKE 'Learjet 60%' THEN 1993
      WHEN model_key LIKE 'Learjet 75%' THEN 2013
      ELSE NULL
    END
  ) AS entry_year_used
FROM fleet_enriched;


-- ============================================================
-- 6) Vue B — OEM commercial (basée sur manufacturer, année utilisée = réel ou estimé)
-- Objectif: se limiter aux grands constructeurs commerciaux
-- (Airbus, Boeing, Embraer, Bombardier, ATR)
-- ============================================================
CREATE OR REPLACE VIEW v_modernity_by_oem_commercial AS
WITH base AS (
  SELECT
    CASE
      WHEN manufacturer LIKE 'Airbus%' THEN 'Airbus'
      WHEN manufacturer LIKE 'Boeing%' THEN 'Boeing'
      WHEN manufacturer LIKE 'Embraer%' OR manufacturer = 'Embraer' THEN 'Embraer'
      WHEN manufacturer LIKE 'Bombardier%' THEN 'Bombardier'
      WHEN manufacturer = 'ATR' OR manufacturer LIKE 'ATR%' THEN 'ATR'
      ELSE NULL
    END AS oem,
    entry_year_used
  FROM v_fleet_base_estimated
)
SELECT
  oem,
  COUNT(*) AS aircraft_count_total,
  SUM(entry_year_used IS NOT NULL) AS aircraft_with_year,
  ROUND(100 * SUM(entry_year_used >= 2015) / NULLIF(SUM(entry_year_used IS NOT NULL), 0), 2) AS pct_modern_2015,
  ROUND(100 * SUM(entry_year_used >= 2010) / NULLIF(SUM(entry_year_used IS NOT NULL), 0), 2) AS pct_modern_2010
FROM base
WHERE oem IS NOT NULL
GROUP BY oem;

-- Remarque clé :
-- - AUCUNE condition sur airline
-- - AUCUNE condition sur catégorie
-- - On accepte que ces avions soient : militaires / institutionnels / privés / étatiques
-- => Ce sont quand même des Airbus/Boeing (OEM), même si l'opérateur n'est pas une compagnie aérienne.


-- ============================================================
-- 7) Score simple de modernité (par compagnie) — basé sur entry_year réel
-- ============================================================
CREATE OR REPLACE VIEW v_modernity_score_by_airline AS
SELECT
  airline_name,
  region,
  country,
  aircraft_count,
  avg_entry_year,
  pct_modern_2015,
  pct_modern_2010,
  ROUND(pct_modern_2015 + 0.5 * pct_modern_2010, 2) AS modernity_score
FROM v_modernity_by_airline
WHERE aircraft_count >= 10;


-- ============================================================
-- Requêtes de contrôle
-- ============================================================

-- Top 20 compagnies les plus "modernes"
-- SELECT * FROM v_modernity_score_by_airline ORDER BY modernity_score DESC LIMIT 20;

-- Comparaison régions
-- SELECT * FROM v_modernity_by_region ORDER BY avg_entry_year DESC;

-- Catégories d'aéronefs (dataset)
-- SELECT * FROM v_modernity_by_aircraft_category ORDER BY aircraft_count DESC;

-- OEM commercial uniquement
-- SELECT * FROM v_modernity_by_oem_commercial ORDER BY aircraft_count_total DESC;
