from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts import config

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

# ============================================================
# 99_debug.py — Diagnostic Data Quality (Air-Modernity V2)
#
# Objectif :
# - Comprendre pourquoi on a peu de lignes avec entry_year valide
# - Vérifier la couverture par compagnie / modèle / région
# - Aider à décider : correction script vs correction données
#
# Entrée :
# - config.DATA_FLEET_ENRICHED  (data/processed/fleet_enriched.csv)
#
# Usage :
# - python scripts/99_debug.py
# ============================================================


def pct(x: float) -> str:
    return f"{100 * x:.2f}%"


def main() -> None:
    path = Path(config.DATA_FLEET_ENRICHED)

    print("=== 99_DEBUG — DATA QUALITY ===")
    print(f"Fichier: {path}")

    if not path.exists():
        print("❌ fleet_enriched.csv introuvable.")
        print("➡️ Lance d'abord : python scripts/02_merge_enriched_and_add_region.py")
        return

    df = pd.read_csv(path)
    n = len(df)

    print(f"\n--- Shape ---")
    print(f"Lignes: {n}")
    print(f"Colonnes: {len(df.columns)}")
    print(f"Colonnes: {list(df.columns)}")

    # Colonnes attendues (au minimum)
    must = ["airline_name", "aircraft_type", "region", "entry_year"]
    missing = [c for c in must if c not in df.columns]
    if missing:
        print(f"\n❌ Colonnes manquantes: {missing}")
        print("➡️ Vérifie le script 02_merge... (merge modèles + region).")
        return

    # --- Qualité entry_year ---
    entry = df["entry_year"]
    entry_is_na = entry.isna()
    valid_entry = (~entry_is_na) & pd.to_numeric(entry, errors="coerce").notna()

    # cohérence année (facultatif mais utile)
    entry_num = pd.to_numeric(entry, errors="coerce")
    plausible = valid_entry & (entry_num >= 1950) & (entry_num <= 2026)

    print("\n--- Coverage entry_year ---")
    print(f"entry_year NA : {entry_is_na.sum()} / {n} ({pct(entry_is_na.mean())})")
    print(f"entry_year numeric : {valid_entry.sum()} / {n} ({pct(valid_entry.mean())})")
    print(f"entry_year plausible (1950-2026) : {plausible.sum()} / {n} ({pct(plausible.mean())})")

    if plausible.sum() > 0:
        print(
            f"entry_year min/max : {int(entry_num[plausible].min())} / {int(entry_num[plausible].max())}"
        )

    # --- Compagnies ---
    print("\n--- Airlines coverage ---")
    airline_counts = df["airline_name"].astype("string").fillna("Unknown").value_counts()
    print(f"Nb compagnies (distinct): {airline_counts.size}")
    print(f"Top 10 compagnies par volume:\n{airline_counts.head(10).to_string()}")

    # Combien de compagnies "survivraient" au filtre MIN_FLEET_SIZE
    min_fleet = getattr(config, "MIN_FLEET_SIZE", 5)
    survive = (airline_counts >= min_fleet).sum()
    print(f"\nCompagnies avec fleet_size >= {min_fleet}: {survive} / {airline_counts.size}")

    # --- Regions ---
    print("\n--- Regions coverage ---")
    region_counts = df["region"].astype("string").fillna("Unknown").value_counts()
    print(region_counts.to_string())

    # --- Où perd-on entry_year ? (par aircraft_type) ---
    print("\n--- Aircraft types with worst entry_year coverage (Top 20) ---")
    tmp = df.copy()
    tmp["entry_year_num"] = entry_num
    tmp["has_entry_year"] = plausible

    by_type = (
        tmp.groupby("aircraft_type")["has_entry_year"]
        .agg(["count", "sum", "mean"])
        .rename(columns={"count": "rows", "sum": "with_year", "mean": "share_with_year"})
        .sort_values(["share_with_year", "rows"], ascending=[True, False])
    )
    print(by_type.head(20).to_string())

    # --- Où perd-on entry_year ? (par compagnie) ---
    print("\n--- Airlines with worst entry_year coverage (Top 20, min 50 rows) ---")
    by_airline = (
        tmp.groupby("airline_name")["has_entry_year"]
        .agg(["count", "sum", "mean"])
        .rename(columns={"count": "rows", "sum": "with_year", "mean": "share_with_year"})
        .query("rows >= 50")
        .sort_values(["share_with_year", "rows"], ascending=[True, False])
    )
    if by_airline.empty:
        print("Aucune compagnie avec >= 50 lignes.")
    else:
        print(by_airline.head(20).to_string())

    # --- Quick sanity checks ---
    print("\n--- Sanity checks ---")
    # régions inconnues
    unknown_region = (tmp["region"].astype("string").fillna("Unknown") == "Unknown").mean()
    print(f"Part region == 'Unknown' : {pct(unknown_region)}")

    # entry_year manquant mais type présent
    missing_year_types = tmp.loc[~tmp["has_entry_year"], "aircraft_type"].nunique()
    total_types = tmp["aircraft_type"].nunique()
    print(f"Nb aircraft_type distinct : {total_types}")
    print(f"Nb aircraft_type sans entry_year plausible : {missing_year_types}")

    print("\n✅ Diagnostic terminé.")
    print("➡️ Si entry_year plausible est très faible : il faudra un fallback (index hybride).")


if __name__ == "__main__":
    main()
