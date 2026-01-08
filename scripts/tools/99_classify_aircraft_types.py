# scripts/99_classify_aircraft_types.py
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

# ============================================================
# 99_classify_aircraft_types.py — Classification & “to-do” list
# (Air-Modernity V2)
#
# Objectif :
# - Classer les aircraft_type de ton dataset selon :
#   1) Couverture du mapping OpenFlights (model_name présent / absent)
#   2) Importance dans le dataset (nb de lignes / poids)
#   3) Catégorie “guess” (airliner / GA / helicopter / military / unknown)
# - Générer des fichiers exploitables pour la suite (scraping + corrections)
#
# Entrées (si présentes) :
# - data/interim/aircraft_types_in_dataset.csv          (liste distincte)
# - data/processed/fleet_enriched.csv                   (pour compter les volumes)
# - data/raw/aircraft_models_api.csv                    (résultat OpenFlights->Wikidata)
#
# Sorties :
# - data/interim/aircraft_types_classification.csv      (vue globale)
# - data/interim/aircraft_types_missing_model_name.csv  (focus “no model_name”)
# - data/interim/aircraft_types_missing_top.csv         (top no_model par volume)
# - data/interim/aircraft_type_manual_mapping_template.csv (template corrections)
#
# Usage :
# - python -m scripts.99_classify_aircraft_types
# ============================================================

TYPES_IN_DATASET = Path("data/interim/aircraft_types_in_dataset.csv")
FLEET_ENRICHED = Path("data/processed/fleet_enriched.csv")
MODELS_API = Path("data/raw/aircraft_models_api.csv")

OUT_DIR = Path("data/interim")
OUT_ALL = OUT_DIR / "aircraft_types_classification.csv"
OUT_MISSING = OUT_DIR / "aircraft_types_missing_model_name.csv"
OUT_MISSING_TOP = OUT_DIR / "aircraft_types_missing_top.csv"
OUT_MANUAL_TEMPLATE = OUT_DIR / "aircraft_type_manual_mapping_template.csv"


# ----------------------------
# Helpers
# ----------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


def pct(x: float) -> str:
    return f"{100 * x:.2f}%"


def classify_guess(code: str, model_name: str | None) -> str:
    """
    Heuristique “simple mais utile” pour une première segmentation.
    Le but n’est PAS la vérité parfaite, mais de trier les priorités.
    """
    c = (code or "").strip().upper()
    mn = (model_name or "").strip().lower()

    # 1) Si on a déjà un model_name, on peut guess plus proprement
    if mn:
        if any(k in mn for k in ["airbus a3", "boeing 7", "embraer e", "atr ", "dash 8", "bombardier", "comac", "sukhoi"]):
            return "airliner"
        if any(k in mn for k in ["cessna", "piper", "beech", "cirrus", "diamond", "gulfstream", "learjet", "citation"]):
            return "general_aviation"
        if any(k in mn for k in ["helicopter", "helicopt", "eurocopter", "airbus helicopter", "bell ", "robinson"]):
            return "helicopter"
        if any(k in mn for k in ["military", "air force", "navy", "army", "lockheed", "northrop", "grumman"]):
            return "military"

    # 2) Heuristique basée sur des patterns ICAO courants
    # Helicopters (souvent R44, R66, EC.., AS.., UH.. etc.)
    if re.match(r"^(R44|R66|EC\d{2}|AS\d{2}|UH\d|H\d{2}|MI\d{1,2})", c):
        return "helicopter"

    # GA typiques : C172, C182, C208, P28A, PA.., BE.. etc.
    if re.match(r"^(C1\d\d|C2\d\d|C3\d\d|C4\d\d|C5\d\d|C6\d\d|C7\d\d|C8\d\d|C9\d\d)", c):
        return "general_aviation"
    if re.match(r"^(P28|PA\d\d|BE\d\d|SR\d\d|DA\d\d)", c):
        return "general_aviation"

    # Airliners usuels : A320/A321 etc (A320 est parfois 4 chars mais pas toujours)
    if c in {"A320", "A321", "A319", "A332", "A333", "A359", "A35K", "A339", "A20N", "A21N"}:
        return "airliner"
    if re.match(r"^B7(3|4|5|6|7|8)\w$", c) or c in {"B738", "B739", "B77W", "B789", "B78X", "B38M"}:
        return "airliner"
    if re.match(r"^(E\d{3}|AT\d{2}|CRJ\d|DH8\w|SF3\w|SU9\w)", c):
        return "airliner_or_regional"

    # Military / special mission : C130, P3C, KC.. etc.
    if re.match(r"^(C1\d\d|C130|C17|C5|KC\d\d|P3C|E3\w|TEX2|F\d\d)", c):
        return "military_or_special"

    return "unknown"


def decide_action(has_model_name: bool, entry_year: float | None, rows: int, guess: str) -> str:
    """
    Propose une action “pratique” :
    - Priorité aux types fréquents (fort volume)
    - Si pas de model_name => à traiter via autre source / mapping manuel
    - Si model_name OK mais pas d’entry_year => requête Wikidata à améliorer / autre source
    """
    if rows >= 1000 and not has_model_name:
        return "PRIORITY: find model_name (manual mapping or alt source)"
    if rows >= 1000 and has_model_name and pd.isna(entry_year):
        return "PRIORITY: fix entry_year lookup for this model_name"
    if not has_model_name:
        if guess in {"helicopter", "general_aviation", "military_or_special"}:
            return "LOW/MED: optional (non-core) — can ignore or map later"
        return "MED: need model_name to scrape entry_year"
    if has_model_name and pd.isna(entry_year):
        return "MED: improve scraping (try other queries / props / sources)"
    return "OK"


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    log("============================================================")
    log("🧩 START — 99_classify_aircraft_types")
    log("============================================================")

    # 1) Load types list
    if not TYPES_IN_DATASET.exists():
        raise FileNotFoundError(f"Fichier introuvable: {TYPES_IN_DATASET}")

    types = pd.read_csv(TYPES_IN_DATASET)
    if "aircraft_type" not in types.columns:
        raise ValueError("aircraft_types_in_dataset.csv doit contenir une colonne 'aircraft_type'")

    types = (
        types[["aircraft_type"]]
        .dropna()
        .astype({"aircraft_type": "string"})
        .drop_duplicates()
    )
    log(f"✅ Types distinct chargés: {len(types)}")

    # 2) Load fleet enriched for counts (optional but recommended)
    counts = None
    if FLEET_ENRICHED.exists():
        fleet = pd.read_csv(FLEET_ENRICHED)
        if "aircraft_type" in fleet.columns:
            counts = (
                fleet["aircraft_type"]
                .astype("string")
                .value_counts()
                .rename_axis("aircraft_type")
                .reset_index(name="rows")
            )
            log(f"✅ fleet_enriched chargé -> comptage OK (types comptés: {len(counts)})")
        else:
            log("⚠️ fleet_enriched.csv présent mais pas de colonne 'aircraft_type' -> pas de comptage")
    else:
        log("⚠️ fleet_enriched.csv absent -> pas de comptage par volume")

    # 3) Load models api (optional)
    models = None
    if MODELS_API.exists():
        models = pd.read_csv(MODELS_API)
        expected = {"aircraft_type", "model_name", "entry_year"}
        missing_cols = expected - set(models.columns)
        if missing_cols:
            log(f"⚠️ aircraft_models_api.csv présent mais colonnes manquantes: {sorted(missing_cols)}")
        else:
            log(f"✅ aircraft_models_api chargé: {len(models)} lignes")
    else:
        log("⚠️ aircraft_models_api.csv absent -> classification 'model_name' impossible (run 01_fetch...)")

    # 4) Merge
    df = types.copy()

    if counts is not None:
        df = df.merge(counts, on="aircraft_type", how="left")
    else:
        df["rows"] = pd.NA

    if models is not None and {"aircraft_type", "model_name"}.issubset(models.columns):
        keep_cols = [c for c in ["aircraft_type", "model_name", "manufacturer", "entry_year", "source"] if c in models.columns]
        df = df.merge(models[keep_cols], on="aircraft_type", how="left")
    else:
        df["model_name"] = pd.NA
        df["manufacturer"] = pd.NA
        df["entry_year"] = pd.NA
        df["source"] = pd.NA

    # 5) Compute flags + guess + action
    df["rows"] = df["rows"].fillna(0).astype(int)
    total_rows = int(df["rows"].sum()) if df["rows"].notna().any() else 0

    df["has_model_name"] = df["model_name"].astype("string").notna() & (df["model_name"].astype("string").str.len() > 0)
    df["has_entry_year"] = df["entry_year"].notna()

    df["share_rows"] = df["rows"].apply(lambda x: (x / total_rows) if total_rows else 0.0)

    df["guess_category"] = df.apply(
        lambda r: classify_guess(str(r["aircraft_type"]), None if pd.isna(r["model_name"]) else str(r["model_name"])),
        axis=1,
    )
    df["suggested_action"] = df.apply(
        lambda r: decide_action(bool(r["has_model_name"]), r.get("entry_year", pd.NA), int(r["rows"]), str(r["guess_category"])),
        axis=1,
    )

    # 6) Sort + export
    df = df.sort_values(["rows", "aircraft_type"], ascending=[False, True])

    df.to_csv(OUT_ALL, index=False, encoding="utf-8")
    log(f"💾 Export global: {OUT_ALL}")

    missing = df[~df["has_model_name"]].copy()
    missing.to_csv(OUT_MISSING, index=False, encoding="utf-8")
    log(f"💾 Export missing model_name: {OUT_MISSING} | rows={len(missing)}")

    missing_top = missing.sort_values("rows", ascending=False).head(60).copy()
    missing_top.to_csv(OUT_MISSING_TOP, index=False, encoding="utf-8")
    log(f"💾 Export missing TOP (60): {OUT_MISSING_TOP}")

    # Template mapping manuel : on te laisse compléter model_name_suggested
    manual = missing[["aircraft_type", "rows", "guess_category"]].copy()
    manual["model_name_suggested"] = ""
    manual["notes"] = ""
    manual = manual.sort_values(["rows", "aircraft_type"], ascending=[False, True])
    manual.to_csv(OUT_MANUAL_TEMPLATE, index=False, encoding="utf-8")
    log(f"💾 Template mapping manuel: {OUT_MANUAL_TEMPLATE}")

    # 7) Console summary
    log("------------------------------------------------------------")
    log(" SUMMARY")
    log(f"- Types total            : {len(df)}")
    log(f"- With model_name         : {int(df['has_model_name'].sum())} ({pct(df['has_model_name'].mean())})")
    log(f"- With entry_year         : {int(df['has_entry_year'].sum())} ({pct(df['has_entry_year'].mean())})")
    if total_rows:
        log(f"- Total rows covered      : {total_rows}")
        log(f"- Share rows w/ model_name: {pct(df.loc[df['has_model_name'], 'share_rows'].sum())}")
        log(f"- Share rows missing name : {pct(df.loc[~df['has_model_name'], 'share_rows'].sum())}")

    log("------------------------------------------------------------")
    log("✅ DONE — Maintenant tu peux :")
    log("1) Ouvrir aircraft_types_missing_top.csv (priorités)")
    log("2) Compléter aircraft_type_manual_mapping_template.csv (model_name_suggested)")
    log("3) Puis on fera un script qui injecte ces model_name dans le pipeline et relance le scraping.")
    log("============================================================")


if __name__ == "__main__":
    main()
