from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import config

# ============================================================
# Étape 4 — Export Scores (V2) : filtre + sortie "scores"
#
# Objectif :
# - Garder la compatibilité de la V1 (pipeline step1/step2/step3)
#   tout en respectant le nouveau fonctionnement :
#   modernity_index est déjà calculé en Étape 3.
#
# Entrée :
# - config.FILE_FEATURES
#   (features par compagnie, incluant fleet_size + modernity_index)
#
# Sortie :
# - config.FILE_SCORES
#   (features filtrées + prêtes pour clustering / app Streamlit)
#
# Règle métier :
# - On exclut les compagnies avec flotte < MIN_FLEET_SIZE
# ============================================================


def main() -> None:
    print("=== Étape 4 : Export Scores (filtrage + sauvegarde) ===")

    in_path = Path(str(config.FILE_FEATURES))
    if not in_path.exists():
        raise FileNotFoundError(f"Fichier manquant : {in_path}")

    df = pd.read_csv(in_path)

    # Colonnes minimum attendues
    required = ["fleet_size", "modernity_index"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Colonnes manquantes dans FILE_FEATURES: {missing}. Colonnes dispo: {list(df.columns)}"
        )

    # Filtre flotte
    before = len(df)
    df = df[df["fleet_size"] >= config.MIN_FLEET_SIZE].copy()
    print(f"- Filtre MIN_FLEET_SIZE={config.MIN_FLEET_SIZE}: {len(df)} / {before}")

    # Clamp score 0..1 + numeric
    df["modernity_index"] = pd.to_numeric(df["modernity_index"], errors="coerce").fillna(0).clip(0, 1)

    # Export
    out_path = Path(str(config.FILE_SCORES))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")

    print("✅ Scores exportés.")
    print(f"- Sortie : {out_path}")


if __name__ == "__main__":
    main()
