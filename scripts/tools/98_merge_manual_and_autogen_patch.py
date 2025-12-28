from __future__ import annotations

from pathlib import Path
import pandas as pd

# ============================================================
# Étape 98b — Merge patch manuel + patch autogen
#
# Objectif :
# - Produire un patch final unique sans doublons
# - Priorité au patch manuel (plus fiable)
#
# Entrées :
# - data/interim/aircraft_type_manual_patch.csv
# - data/interim/aircraft_type_manual_patch_autogen.csv
#
# Sortie :
# - data/interim/aircraft_type_manual_patch_merged.csv
# ============================================================

PATCH_MANUAL = Path("data/interim/aircraft_type_manual_patch.csv")
PATCH_AUTOGEN = Path("data/interim/aircraft_type_manual_patch_autogen.csv")
OUT = Path("data/interim/aircraft_type_manual_patch_merged.csv")


def main() -> None:
    if not PATCH_MANUAL.exists():
        raise FileNotFoundError(f"Patch manuel introuvable: {PATCH_MANUAL}")
    if not PATCH_AUTOGEN.exists():
        raise FileNotFoundError(f"Patch autogen introuvable: {PATCH_AUTOGEN}")

    manual = pd.read_csv(PATCH_MANUAL)
    autogen = pd.read_csv(PATCH_AUTOGEN)

    manual["aircraft_type"] = manual["aircraft_type"].astype("string").str.strip()
    manual["model_name_manual"] = manual["model_name_manual"].astype("string").str.strip()

    autogen["aircraft_type"] = autogen["aircraft_type"].astype("string").str.strip()
    autogen["model_name_manual"] = autogen["model_name_manual"].astype("string").str.strip()

    # priorité au manuel : on concat en mettant autogen d'abord, puis manuel à la fin,
    # et on garde la dernière occurrence
    merged = pd.concat([autogen, manual], ignore_index=True)
    merged = merged.dropna().drop_duplicates(subset=["aircraft_type"], keep="last")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT, index=False, encoding="utf-8")

    print("✅ Patch merged prêt")
    print(f"Sortie: {OUT}")
    print(f"Mappings: {len(merged)}")


if __name__ == "__main__":
    main()
