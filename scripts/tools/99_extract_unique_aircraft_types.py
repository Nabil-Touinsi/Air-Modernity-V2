from __future__ import annotations

from pathlib import Path

import pandas as pd

# ============================================================
# 99 — Extract unique aircraft types (ICAO designators)
#
# Entrée :
# - data/processed/fleet_enriched.csv
#
# Sortie :
# - data/interim/aircraft_types_in_dataset.csv
#   (aircraft_type + count)
#
# Usage :
# - python -m scripts.99_extract_unique_aircraft_types
# ============================================================

IN_FILE = Path("data/processed/fleet_enriched.csv")
OUT_FILE = Path("data/interim/aircraft_types_in_dataset.csv")


def main() -> None:
    if not IN_FILE.exists():
        raise FileNotFoundError(f"Fichier introuvable : {IN_FILE}")

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(IN_FILE)

    if "aircraft_type" not in df.columns:
        raise ValueError("Colonne 'aircraft_type' introuvable dans fleet_enriched.csv")

    out = (
        df["aircraft_type"]
        .astype("string")
        .str.strip()
        .dropna()
        .value_counts()
        .reset_index()
    )
    out.columns = ["aircraft_type", "count"]
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")

    print("✅ OK")
    print(f"Sortie : {OUT_FILE}")
    print(f"Nb types distinct : {len(out)}")


if __name__ == "__main__":
    main()
