-- ============================================================
-- Étape 6 — Création base + table principale (MySQL)
-- Projet : Air-Modernity
--
-- Objectif :
-- - créer le schema air_modernity
-- - créer la table fleet_enriched (données consolidées)
--
-- Remarque :
-- - cleaned_at : on le stocke en VARCHAR(32) pour éviter les soucis
--   d’import (ISO 8601 avec "T" et "Z" ex: 2025-12-26T19:25:28Z)
-- - entry_year : SMALLINT NULL (année d’entrée en service)
-- ============================================================

CREATE DATABASE IF NOT EXISTS air_modernity
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE air_modernity;

DROP TABLE IF EXISTS fleet_enriched;

CREATE TABLE fleet_enriched (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  -- Dimensions principales
  airline_name VARCHAR(255) NULL,
  country VARCHAR(120) NULL,
  region VARCHAR(40) NULL,

  -- Avion / flotte
  aircraft_type VARCHAR(32) NULL,
  registration VARCHAR(32) NULL,

  -- Mesures + métadonnées
  total_fleet_size INT NULL,
  cleaned_at VARCHAR(32) NULL,

  model_key VARCHAR(255) NULL,
  manufacturer VARCHAR(120) NULL,
  entry_year SMALLINT NULL,

  PRIMARY KEY (id),

  -- Index utiles pour filtres + agrégations
  INDEX idx_airline (airline_name),
  INDEX idx_country (country),
  INDEX idx_region (region),
  INDEX idx_aircraft_type (aircraft_type),
  INDEX idx_entry_year (entry_year),
  INDEX idx_airline_aircraft (airline_name, aircraft_type)
) ENGINE=InnoDB;
