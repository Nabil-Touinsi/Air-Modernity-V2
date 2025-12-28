from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import config

# ============================================================
# Étape 3 — Build Features : agrégation par compagnie
#
#  Objectif : générer les features utilisées pour le scoring et le clustering.
#
# Entrée :
# - config.DATA_FLEET_ENRICHED (ex: data/processed/fleet_enriched_v2.csv)
#   Colonnes attendues (minimum) :
#   - airline_name, country, region, entry_year
#   + optionnel : aircraft_type OU model_key (pour diversity_score)
#
# Sortie :
# - config.FILE_FEATURES (ex: data/processed/features_by_airline.csv)
# ============================================================

FLEET_ENRICHED = config.DATA_FLEET_ENRICHED


def require_columns(df: pd.DataFrame, cols: list[str], ctx: str) -> None:
    """Stoppe avec un message clair si des colonnes attendues manquent."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{ctx}] Colonnes manquantes: {missing}. Colonnes dispo: {list(df.columns)}"
        )


def main() -> None:
    print("=== Étape 3 : Build Features (par compagnie) ===")

    # 1) Vérifier l'entrée
    if not FLEET_ENRICHED.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {FLEET_ENRICHED}\n"
            f"→ Génère-le d'abord via les scripts 00..02 (clean + merge + region)."
        )

    df = pd.read_csv(FLEET_ENRICHED)

    # 2) Colonnes minimales attendues
    require_columns(df, ["airline_name", "country", "region", "entry_year"], "fleet_enriched")

    # 3) Nettoyage minimal (lisible + stable)
    df["airline_name"] = df["airline_name"].astype("string").str.strip()
    df["country"] = df["country"].astype("string").str.strip()
    df["region"] = df["region"].astype("string").str.strip()

    # entry_year -> numeric (les valeurs non parseables deviennent NA)
    df["entry_year"] = pd.to_numeric(df["entry_year"], errors="coerce")

    # 4) Exclure les lignes sans entry_year (sinon % et moyennes faux)
    before = len(df)
    df = df.dropna(subset=["entry_year"]).copy()
    print(f"- Lignes avec entry_year valide: {len(df)} / {before}")

    # ✅ Option A : EXCLURE uniquement pour les features les lignes sans country/region
    # (évite que "Unknown" pollue les stats par région)
    before_geo = len(df)
    df = df[
        df["country"].notna() & (df["country"] != "") &
        df["region"].notna() & (df["region"] != "")
    ].copy()
    removed_geo = before_geo - len(df)
    print(f"- Lignes exclues (country/region manquant): {removed_geo} | restantes: {len(df)}")

    # 5) Définition de la modernité par seuils (aligné SQL)
    df["is_modern_2015"] = (df["entry_year"] >= 2015).astype(int)
    df["is_modern_2010"] = (df["entry_year"] >= 2010).astype(int)

    # 6) diversity_score : on prend la meilleure colonne dispo (si elle existe)
    diversity_col = None
    if "aircraft_type" in df.columns:
        diversity_col = "aircraft_type"
    elif "model_key" in df.columns:
        diversity_col = "model_key"

    # 7) Agrégation par compagnie
    agg: dict[str, object] = {
        "country": "first",
        "region": "first",
        "entry_year": "mean",
        "is_modern_2015": "sum",
        "is_modern_2010": "sum",
    }
    if diversity_col:
        agg[diversity_col] = pd.Series.nunique

    stats = df.groupby("airline_name", as_index=False).agg(agg)

    # fleet_size = nombre d’avions (lignes)
    fleet_size = df.groupby("airline_name").size().reset_index(name="fleet_size")
    stats = stats.merge(fleet_size, on="airline_name", how="left")

    # 8) Renommage des colonnes pour correspondre aux vues SQL
    stats.rename(
        columns={
            "entry_year": "avg_entry_year",
            "is_modern_2015": "modern_count_2015",
            "is_modern_2010": "modern_count_2010",
        },
        inplace=True,
    )

    if diversity_col:
        stats.rename(columns={diversity_col: "diversity_score"}, inplace=True)
    else:
        stats["diversity_score"] = 0

    # 9) Pourcentages en 0..100 + index ratio 0..1
    denom = stats["fleet_size"].replace(0, 1)
    stats["pct_modern_2015"] = (stats["modern_count_2015"] / denom) * 100.0
    stats["pct_modern_2010"] = (stats["modern_count_2010"] / denom) * 100.0

    # modernity_index = ratio 0..1 (basé sur seuil 2015)
    stats["modernity_index"] = (stats["pct_modern_2015"] / 100.0).clip(0, 1)

    # 10) Sécurité région : plus besoin de forcer Unknown (on a filtré avant)
    # stats["region"] = stats["region"].replace({"": "Unknown"}).fillna("Unknown")

    # 11) Export
    out_path = Path(str(config.FILE_FEATURES))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(out_path, index=False, encoding="utf-8")

    print("✅ Features générées.")
    print(f"- Compagnies : {len(stats)}")
    print(f"- Sortie     : {out_path}")


if __name__ == "__main__":
    main()
