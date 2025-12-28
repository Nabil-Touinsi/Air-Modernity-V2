from __future__ import annotations

from pathlib import Path

import pandas as pd

# ============================================================
# 99 — Extract aircraft_type without model_name
#
# Entrée :
# - data/raw/aircraft_models_api.csv
#
# Sorties :
# - data/interim/aircraft_types_missing_model_name.csv
# - data/interim/aircraft_types_missing_model_name.txt
#
# Usage :
# - python -m scripts.99_extract_missing_model_name
# ============================================================

IN_FILE = Path("data/raw/aircraft_models_api.csv")
OUT_CSV = Path("data/interim/aircraft_types_missing_model_name.csv")
OUT_TXT = Path("data/interim/aircraft_types_missing_model_name.txt")


def main() -> None:
    if not IN_FILE.exists():
        raise FileNotFoundError(f"Fichier introuvable : {IN_FILE}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(IN_FILE)

    if "aircraft_type" not in df.columns or "model_name" not in df.columns:
        raise ValueError("aircraft_models_api.csv doit contenir 'aircraft_type' et 'model_name'")

    missing = df[df["model_name"].isna() | (df["model_name"].astype("string").str.strip() == "")]
    missing = (
        missing[["aircraft_type"]]
        .astype("string")
        .dropna()
        .drop_duplicates()
        .sort_values("aircraft_type")
    )

    missing.to_csv(OUT_CSV, index=False, encoding="utf-8")
    OUT_TXT.write_text("\n".join(missing["aircraft_type"].tolist()), encoding="utf-8")

    print("✅ OK")
    print(f"Nb types sans model_name : {len(missing)}")
    print(f"CSV : {OUT_CSV}")
    print(f"TXT : {OUT_TXT}")


if __name__ == "__main__":
    main()
