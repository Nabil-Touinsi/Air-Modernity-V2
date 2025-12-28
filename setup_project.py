import os
from pathlib import Path

# ============================================================
# setup_project.py (V2)
# - Structure conforme au pipeline actuel (fleet_enriched_v2)
# - Artefacts ML dans data/out/
# - main.py orchestre le pipeline
# ============================================================

files = {}

# 1) requirements.txt (V2)
files["requirements.txt"] = """pandas
numpy
openpyxl
requests
lxml
scikit-learn
matplotlib
streamlit
plotly
"""

# 2) scripts/__init__.py
files["scripts/__init__.py"] = ""

# 3) scripts/config.py (V2)
files["scripts/config.py"] = """from __future__ import annotations

from pathlib import Path

# ============================================================
# Configuration V2 — Air-Modernity
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
REF_DIR = DATA_DIR / "ref"
PROCESSED_DIR = DATA_DIR / "processed"
OUT_DIR = DATA_DIR / "out"

for p in [RAW_DIR, INTERIM_DIR, REF_DIR, PROCESSED_DIR, OUT_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Inputs
# ----------------------------
# Source FR24 (raw)
FR24_RAW_XLSX = RAW_DIR / "flightradar24_raw.xlsx"

# Cleaned intermediate
FR24_CLEAN_CSV = INTERIM_DIR / "flightradar24_clean.csv"

# Aircraft metadata (sortie script 01)
AIRCRAFT_MODELS_CSV = RAW_DIR / "aircraft_models_api.csv"
AIRCRAFT_MODELS_CACHE_CSV = RAW_DIR / "aircraft_models_api_cache.csv"

# Patch manuel + fichiers optionnels (script 01)
AIRCRAFT_MANUAL_PATCH = INTERIM_DIR / "aircraft_type_manual_patch.csv"
AIRCRAFT_ICAO_IATA_JSON = RAW_DIR / "aircraftIcaoIata.json"
AIRCRAFT_TYPES_IN_DATASET = INTERIM_DIR / "aircraft_types_in_dataset.csv"
AIRCRAFT_TYPES_TO_EXCLUDE = INTERIM_DIR / "aircraft_types_to_exclude.csv"

# Country -> region mapping
COUNTRY_REGION_MAPPING = REF_DIR / "country_region_mapping.csv"

# ----------------------------
# Outputs
# ----------------------------
# Dataset final V2 (celui que tout le pipeline consomme)
DATA_FLEET_ENRICHED = PROCESSED_DIR / "fleet_enriched_v2.csv"

# Features / scores / clusters utilisés par l'app
FILE_FEATURES = PROCESSED_DIR / "airlines_features.csv"
FILE_SCORES   = PROCESSED_DIR / "airlines_scores.csv"
FILE_CLUSTERS = PROCESSED_DIR / "airlines_clusters.csv"

# Trace mapping
FILE_COUNTRIES_UNKNOWN = PROCESSED_DIR / "countries_unknown.csv"

# Artefacts ML (utilisés par Streamlit)
FILE_SCALER = OUT_DIR / "scaler.pkl"
FILE_KNN_MODEL = OUT_DIR / "knn_model.pkl"
FILE_ELBOW = OUT_DIR / "elbow_data.json"
FILE_KNN_METRICS = OUT_DIR / "knn_metrics.json"

# ----------------------------
# Paramètres
# ----------------------------
MIN_FLEET_SIZE = 5
RANDOM_STATE = 42
"""

# 4) main.py (V2)
files["main.py"] = """from __future__ import annotations

# Orchestrateur V2 : lance les scripts dans l'ordre
# (les scripts doivent exister dans /scripts)

def main() -> None:
    print(">>> AIR-MODERNITY V2 — PIPELINE START <<<")

    try:
        # 00 : clean FR24 raw -> interim csv
        from scripts import 00_clean_flightradar24  # type: ignore
        00_clean_flightradar24.main()

        # 01 : fetch aircraft metadata -> data/raw/aircraft_models_api.csv
        from scripts import 01_fetch_aircraft_metadata_wikidata  # type: ignore
        01_fetch_aircraft_metadata_wikidata.main()

        # 02 : merge + region -> data/processed/fleet_enriched_v2.csv
        from scripts import 02_merge_enriched_and_add_region  # type: ignore
        02_merge_enriched_and_add_region.main()

        # 03 : features par compagnie -> airlines_features.csv
        from scripts import 03_generate_features  # type: ignore
        03_generate_features.main()

        # 04 : scores -> airlines_scores.csv
        from scripts import 04_compute_scores  # type: ignore
        04_compute_scores.main()

        # 05 : clustering + PCA -> airlines_clusters.csv + elbow json (si prévu)
        from scripts import 05_clustering_and_pca  # type: ignore
        05_clustering_and_pca.main()

        # 06 : KNN + artefacts -> data/out/*
        from scripts import 06_train_knn  # type: ignore
        06_train_knn.main()

        print("\\n>>> PIPELINE OK <<<")
        print("Lance ensuite : streamlit run app.py")

    except Exception as e:
        print(f"\\n❌ ERREUR PIPELINE : {e}")
        raise


if __name__ == "__main__":
    main()
"""

# 5) app.py (rappel : ton app actuelle doit lire airlines_clusters.csv)
# -> Je ne la réécris pas ici car tu as déjà une version travaillée.
#    Mais on peut ajouter un template minimal si tu veux.

files["app.py"] = """import streamlit as st
import pandas as pd

st.set_page_config(page_title="Air-Modernity V2", layout="wide", page_icon="✈️")

@st.cache_data
def load_clusters():
    try:
        return pd.read_csv("data/processed/airlines_clusters.csv")
    except Exception:
        return pd.DataFrame()

df = load_clusters()
if df.empty:
    st.error("Aucune donnée trouvée. Lance d'abord : python main.py")
    st.stop()

st.title("✈️ Air-Modernity V2 — Dashboard")
st.write("Données : data/processed/airlines_clusters.csv")
st.dataframe(df.head(50))
"""

# --- création structure + fichiers ---
def create_project() -> None:
    print("🚀 Création de la structure du projet (V2)...")

    folders = [
        "data/raw",
        "data/interim",
        "data/ref",
        "data/processed",
        "data/out",
        "scripts",
    ]

    for folder in folders:
        Path(folder).mkdir(parents=True, exist_ok=True)
        print(f"📁 Dossier créé : {folder}")

    for filepath, content in files.items():
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"📄 Fichier créé : {filepath}")

    print("\\n✅ PROJET V2 PRÊT !")
    print("-------------------------------------------------------")
    print("👉 1) Mets le fichier FR24 raw : data/raw/flightradar24_raw.xlsx")
    print("👉 2) Mets le mapping : data/ref/country_region_mapping.csv")
    print("👉 3) (Optionnel) Patch : data/interim/aircraft_type_manual_patch.csv")
    print("👉 4) pip install -r requirements.txt")
    print("👉 5) python main.py")
    print("👉 6) streamlit run app.py")
    print("-------------------------------------------------------")


if __name__ == "__main__":
    create_project()
