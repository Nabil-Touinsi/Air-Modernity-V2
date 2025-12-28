from __future__ import annotations

from pathlib import Path
import pandas as pd

# ============================================================
# Étape 98 — Auto-génération d'un patch (aircraft_type -> model_name_manual)
#
# Objectif :
# - Réduire "no_model" en générant automatiquement des libellés
#   plausibles à partir de patterns ICAO (P28*, C4*, BE*, etc.)
#
# Entrées :
# - data/interim/aircraft_types_in_dataset.csv
#
# Sortie :
# - data/interim/aircraft_type_manual_patch_autogen.csv
# ============================================================

IN_TYPES = Path("data/interim/aircraft_types_in_dataset.csv")
OUT_PATCH = Path("data/interim/aircraft_type_manual_patch_autogen.csv")


def guess_model_name(icao: str) -> str | None:
    icao = str(icao).strip().upper()

    # Piper PA-28 variants
    if icao.startswith("P28"):
        return "Piper PA-28"

    # Piper PA-31 variants
    if icao.startswith("PA3") or icao.startswith("P31"):
        return "Piper PA-31 Navajo"

    # Cessna 4xx series
    if icao.startswith("C4"):
        return "Cessna 400 series"

    # Beechcraft (BE*)
    if icao.startswith("BE"):
        return "Beechcraft aircraft"

    # Bell Helicopter (BELL etc.)
    if icao.startswith("UH"):
        return "Bell helicopter"

    # MA60 (Chinese)
    if icao == "MA60":
        return "Xian MA60"

    # Default: no guess
    return None


def main() -> None:
    if not IN_TYPES.exists():
        raise FileNotFoundError(f"Fichier introuvable: {IN_TYPES}")

    df = pd.read_csv(IN_TYPES)
    types = df["aircraft_type"].dropna().astype(str).str.strip().str.upper().drop_duplicates()

    rows = []
    for t in types:
        g = guess_model_name(t)
        if g:
            rows.append({"aircraft_type": t, "model_name_manual": g})

    out = pd.DataFrame(rows).drop_duplicates(subset=["aircraft_type"])
    OUT_PATCH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PATCH, index=False, encoding="utf-8")

    print("✅ Patch auto généré")
    print(f"Sortie: {OUT_PATCH}")
    print(f"Mappings: {len(out)}")


if __name__ == "__main__":
    main()
