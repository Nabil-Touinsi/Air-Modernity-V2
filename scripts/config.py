from __future__ import annotations

from pathlib import Path

# ============================================================
# Config — Air-Modernity (pipeline V2 + front Streamlit conservé)
#
# Structure :
# - data/raw       : sources
# - data/interim   : sorties intermédiaires (clean)
# - data/ref       : mapping pays -> région
# - data/processed : datasets finaux + tables pour l'app
# - data/out       : artefacts ML (scaler, knn, json elbow/metrics)
# ============================================================

# --- Base dir (racine du projet) ---
BASE_DIR = Path(__file__).resolve().parent.parent

# --- Répertoires ---
RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"
REF_DIR = BASE_DIR / "data" / "ref"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MODEL_DIR = BASE_DIR / "data" / "out"

# Crée les dossiers si absents
for d in [RAW_DIR, INTERIM_DIR, REF_DIR, PROCESSED_DIR, MODEL_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# --- Fichiers du pipeline V2 ---
# Mapping pays -> region (utilisé par 02_merge...)
DATA_REF = REF_DIR / "country_region_mapping.csv"

# Dataset final consolidé (produit par 02_merge...)
DATA_FLEET_ENRICHED = PROCESSED_DIR / "fleet_enriched_v2.csv"

# --- Fichiers consommés par l'app (front Streamlit) ---
FILE_FEATURES = PROCESSED_DIR / "airlines_features.csv"
FILE_SCORES = PROCESSED_DIR / "airlines_scores.csv"
FILE_CLUSTERS = PROCESSED_DIR / "airlines_clusters.csv"

# --- Sorties modèles / métriques (simulateur) ---
DATA_OUT = MODEL_DIR  # compat avec ton step3 actuel (joblib + json)

# --- Paramètres ---
MIN_FLEET_SIZE = 5
