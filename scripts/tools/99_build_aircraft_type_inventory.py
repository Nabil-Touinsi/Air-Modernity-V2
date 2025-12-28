from __future__ import annotations

from pathlib import Path
import pandas as pd

# ============================================================
# Étape — Injection mapping manuel (aircraft_type -> model_name)
#
# Objectif :
# - Combler les model_name manquants dans fleet_enriched.csv
#   via un patch manuel (prioritaire)
#
# Entrées :
# - data/processed/fleet_enriched.csv
# - data/interim/aircraft_type_manual_patch.csv
#
# Sorties :
# - data/processed/fleet_enriched_v2.csv
# - data/interim/aircraft_types_still_missing_model_name.csv
#
# Usage :
# - python -m scripts.03_apply_manual_model_mapping
# ============================================================

FLEET_IN = Path("data/processed/fleet_enriched.csv")
PATCH = Path("data/interim/aircraft_type_manual_patch.csv")

FLEET_OUT = Path("data/processed/fleet_enriched_v2.csv")
MISSING_OUT = Path("data/interim/aircraft_types_still_missing_model_name.csv")


def ensure_dirs() -> None:
    FLEET_OUT.parent.mkdir(parents=True, exist_ok=True)
    MISSING_OUT.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
    print("============================================================")
    print("🚀 START — Apply manual mapping (aircraft_type -> model_name)")
    print("============================================================")

    ensure_dirs()

    if not FLEET_IN.exists():
        raise FileNotFoundError(f"Fichier introuvable : {FLEET_IN}")
    if not PATCH.exists():
        raise FileNotFoundError(
            f"Patch introuvable : {PATCH}\n"
            "➡️ Crée ce fichier et colle le CSV: aircraft_type,model_name_manual"
        )

    # 1) Load fleet
    print(f"📥 Lecture fleet: {FLEET_IN}")
    df = pd.read_csv(FLEET_IN)
    print(f"✅ fleet loaded | rows={len(df)} | cols={len(df.columns)}")

    if "aircraft_type" not in df.columns:
        raise ValueError("fleet_enriched.csv doit contenir la colonne 'aircraft_type'")

    # 2) Ensure model_name column exists
    if "model_name" not in df.columns:
        print("⚠️ Colonne 'model_name' absente -> création (NA).")
        df["model_name"] = pd.NA

    # 3) Load patch
    print(f"📥 Lecture patch: {PATCH}")
    patch = pd.read_csv(PATCH)

    expected_cols = {"aircraft_type", "model_name_manual"}
    if not expected_cols.issubset(set(patch.columns)):
        raise ValueError(
            f"Le patch doit contenir {expected_cols}. Colonnes actuelles: {list(patch.columns)}"
        )

    patch["aircraft_type"] = patch["aircraft_type"].astype("string").str.strip()
    patch["model_name_manual"] = patch["model_name_manual"].astype("string").str.strip()

    patch = patch.dropna(subset=["aircraft_type", "model_name_manual"])
    patch = patch[patch["model_name_manual"] != ""].drop_duplicates(subset=["aircraft_type"])

    patch_map = dict(zip(patch["aircraft_type"], patch["model_name_manual"]))
    print(f"✅ patch ready | mappings={len(patch_map)}")

    # 4) Apply only when model_name is missing/empty
    before_missing = (
        df["model_name"].isna() | (df["model_name"].astype("string").str.strip() == "")
    ).sum()

    print(f"🔎 Avant injection | model_name manquants: {before_missing}/{len(df)}")

    # mask rows where model_name is missing
    mask_missing = df["model_name"].isna() | (df["model_name"].astype("string").str.strip() == "")

    # candidates that can be filled
    can_fill = df.loc[mask_missing, "aircraft_type"].astype("string").str.strip().isin(patch_map.keys())
    fill_count = int(can_fill.sum())

    print(f"🧩 Lignes éligibles à remplir via patch: {fill_count}")

    # apply mapping
    idx_to_fill = df.index[mask_missing & df["aircraft_type"].astype("string").str.strip().isin(patch_map.keys())]
    df.loc[idx_to_fill, "model_name"] = (
        df.loc[idx_to_fill, "aircraft_type"]
        .astype("string")
        .str.strip()
        .map(patch_map)
    )

    after_missing = (
        df["model_name"].isna() | (df["model_name"].astype("string").str.strip() == "")
    ).sum()

    print(f"✅ Après injection | model_name manquants: {after_missing}/{len(df)}")
    print(f"📉 Gain | remplis: {before_missing - after_missing}")

    # 5) Export fleet v2
    print(f"💾 Export fleet v2 -> {FLEET_OUT}")
    df.to_csv(FLEET_OUT, index=False, encoding="utf-8")
    print("✅ fleet_enriched_v2.csv écrit.")

    # 6) Export still-missing aircraft types (prioritised)
    still_missing = df[df["model_name"].isna() | (df["model_name"].astype("string").str.strip() == "")]
    if len(still_missing) > 0:
        missing_types = (
            still_missing["aircraft_type"]
            .astype("string")
            .str.strip()
            .value_counts()
            .reset_index()
        )
        missing_types.columns = ["aircraft_type", "rows"]
        missing_types.to_csv(MISSING_OUT, index=False, encoding="utf-8")
        print(f"📄 Types encore sans model_name -> {MISSING_OUT} (distinct={len(missing_types)})")
    else:
        print("🎉 Plus aucun aircraft_type sans model_name !")

    print("============================================================")
    print("✅ DONE — Manual mapping applied")
    print("============================================================")


if __name__ == "__main__":
    main()
