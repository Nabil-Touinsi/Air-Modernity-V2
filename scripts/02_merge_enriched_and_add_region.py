from __future__ import annotations

import unicodedata
from pathlib import Path

import pandas as pd


# ============================================================
# Étape 5 — Fusion (clean + modèles enrichis) + ajout "region"
#
# Entrées :
# - data/interim/flightradar24_clean.csv
# - data/raw/aircraft_models_api.csv
# - data/ref/country_region_mapping.csv    (pays -> region)
#
# Sorties :
# - data/processed/fleet_enriched.csv      (dataset final consolidé)
# - data/processed/countries_unknown.csv   (pays sans region)
# ============================================================

CLEAN_CSV = Path("data/interim/flightradar24_clean.csv")
MODELS_CSV = Path("data/raw/aircraft_models_api.csv")
COUNTRY_REGION_CSV = Path("data/ref/country_region_mapping.csv")

OUT = Path("data/processed/fleet_enriched_v2.csv")
OUT_UNKNOWN = Path("data/processed/countries_unknown.csv")


def ensure_dirs() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)


def normalize_text(s: str) -> str:
    """
    Normalisation légère pour les jointures (lisible + traçable) :
    - strip
    - normalisation Unicode NFKC
    - remplacement espaces insécables
    - harmonisation des tirets
    """
    s = s.strip()
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00A0", " ")  # NBSP -> space
    s = s.replace("–", "-").replace("—", "-")
    s = " ".join(s.split())  # espaces multiples -> 1
    return s


def find_country_column(df: pd.DataFrame) -> str:
    for c in ["country", "pays", "Country", "COUNTRY"]:
        if c in df.columns:
            return c
    raise ValueError("Colonne pays introuvable dans flightradar24_clean.csv (attendu: country ou pays).")


def main() -> None:
    ensure_dirs()

    if not CLEAN_CSV.exists():
        raise FileNotFoundError(f"Fichier introuvable : {CLEAN_CSV}")
    if not MODELS_CSV.exists():
        raise FileNotFoundError(f"Fichier introuvable : {MODELS_CSV}")
    if not COUNTRY_REGION_CSV.exists():
        raise FileNotFoundError(f"Mapping pays->region introuvable : {COUNTRY_REGION_CSV}")

    df = pd.read_csv(CLEAN_CSV)
    models = pd.read_csv(MODELS_CSV)
    cr = pd.read_csv(COUNTRY_REGION_CSV)

    if "aircraft_type" not in df.columns:
        raise ValueError("Colonne aircraft_type introuvable dans le CSV clean.")
    if "aircraft_type" not in models.columns:
        raise ValueError("Colonne aircraft_type introuvable dans aircraft_models_api.csv.")
    if not {"country", "region"}.issubset(set(cr.columns)):
        raise ValueError("country_region_mapping.csv doit contenir les colonnes: country,region")

    # 1) Merge métadonnées avions (left join) — on map sur aircraft_type uniquement
    #    -> plus besoin de model_key
    keep_cols = [c for c in ["aircraft_type", "model_name", "manufacturer", "entry_year"] if c in models.columns]
    df = df.merge(models[keep_cols], on="aircraft_type", how="left")

    # 2) Merge region (via country)
    country_col = find_country_column(df)

    # IMPORTANT : préserver les valeurs manquantes (ne pas faire astype(str))
    df[country_col] = df[country_col].astype("string")
    cr["country"] = cr["country"].astype("string")

    df[country_col] = df[country_col].map(lambda x: normalize_text(x) if pd.notna(x) else pd.NA)
    cr["country"] = cr["country"].map(lambda x: normalize_text(x) if pd.notna(x) else pd.NA)

    df = df.merge(
        cr.rename(columns={"country": country_col}),
        on=country_col,
        how="left",
    )

    # 3) Export pays non mappés (on ignore les NA)
    unknown = (
        df[df["region"].isna()][country_col]
        .dropna()
        .value_counts()
        .reset_index()
    )
    unknown.columns = [country_col, "count"]
    unknown.to_csv(OUT_UNKNOWN, index=False, encoding="utf-8")

    # 4) Export dataset final
    df.to_csv(OUT, index=False, encoding="utf-8")

    print("Terminé.")
    print(f"Sortie       : {OUT}")
    print(f"Pays inconnus: {OUT_UNKNOWN}")


if __name__ == "__main__":
    main()
