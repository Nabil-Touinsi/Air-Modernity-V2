from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

import pandas as pd


# =========
# CONFIG
# =========
RAW_XLSX = Path("data/raw/flightradar24_raw.xlsx")
OUT_CSV = Path("data/interim/flightradar24_clean.csv")

# Si tu veux forcer un mapping précis, tu peux le remplir ici (sinon auto-détection)
FORCE_COLUMN_MAP: dict[str, str] = {
    # "NomColonneDansExcel": "nom_normalise",
    # ex:
    # "Airline": "airline_name",
}

# Colonnes normalisées attendues (minimales pour la suite)
REQUIRED_MIN = ["airline_name", "country", "aircraft_type", "registration", "total_fleet_size"]


# =========
# UTILS
# =========
def ensure_dirs() -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)


def norm_colname(s: str) -> str:
    # normalisation simple pour aider l’auto-détection
    return (
        str(s)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("__", "_")
    )


def guess_column_map(columns: list[str]) -> dict[str, str]:
    """
    Associe des colonnes sources à des noms normalisés.
    Stratégie :
    - normalise les noms
    - cherche des synonymes fréquents
    """
    src_cols = list(columns)
    normed = {c: norm_colname(c) for c in src_cols}

    # Synonymes possibles -> colonne normalisée cible
    targets: dict[str, list[str]] = {
        "airline_name": ["airline", "airline_name", "operator", "company", "carrier", "airlines"],
        "country": ["country", "nation", "country_name", "state"],
        "aircraft_type": ["aircraft_type", "type", "aircraft", "model", "aircraft_model"],
        "registration": ["registration", "reg", "tail", "tail_number", "immatriculation", "immat"],
        "msn": ["msn", "manufacturer_serial_number", "serial", "serial_number"],
        "total_fleet_size": ["total_fleet_size", "fleet_size", "fleet", "nb_aircraft", "aircraft_count", "count"],
    }

    mapping: dict[str, str] = {}

    # 1) appliquer FORCE_COLUMN_MAP si fourni
    for raw_name, new_name in FORCE_COLUMN_MAP.items():
        if raw_name in src_cols:
            mapping[raw_name] = new_name

    # 2) auto-détection pour les autres
    already_mapped = set(mapping.keys())
    already_targets = set(mapping.values())

    for src in src_cols:
        if src in already_mapped:
            continue
        n = normed[src]
        for target, syns in targets.items():
            if target in already_targets:
                continue
            # match exact sur nom normalisé
            if n == target:
                mapping[src] = target
                already_targets.add(target)
                break
            # match sur synonymes
            if n in syns:
                mapping[src] = target
                already_targets.add(target)
                break

    return mapping


def clean_text_series(s: pd.Series) -> pd.Series:
    # Nettoyage "soft" : strip + espaces multiples
    out = s.astype(str)
    out = out.str.replace(r"\s+", " ", regex=True).str.strip()
    # Remplacer les "nan" textuels (quand on cast en str)
    out = out.replace({"nan": "", "NaN": "", "None": ""})
    return out


def main() -> None:
    ensure_dirs()

    if not RAW_XLSX.exists():
        raise FileNotFoundError(
            f"Fichier introuvable: {RAW_XLSX}\n"
            f"Place ton dataset ici (ou modifie RAW_XLSX)."
        )

    print("=== Étape 3 : Nettoyage & préparation du CSV ===")
    print(f"[IN]  {RAW_XLSX}")

    # Lecture Excel (par défaut première feuille)
    df = pd.read_excel(RAW_XLSX)
    print(f"[INFO] Lignes brutes: {len(df):,} | Colonnes brutes: {len(df.columns)}")
    print("[INFO] Colonnes brutes:", list(df.columns))

    # Renommage colonnes -> mapping
    col_map = guess_column_map(list(df.columns))
    if not col_map:
        print("[WARN] Aucun mapping auto-détecté. Vérifie tes noms de colonnes.")
    else:
        print("[INFO] Mapping détecté:")
        for k, v in col_map.items():
            print(f"  - {k}  ->  {v}")

    df = df.rename(columns=col_map)

    # Si certaines colonnes existent mais pas normalisées, on les normalise en snake_case
    # (sans casser les colonnes déjà normalisées)
    df.columns = [c if c in REQUIRED_MIN or c in ["msn"] else norm_colname(c) for c in df.columns]

    # Garder uniquement les colonnes utiles + celles qui existent
    keep = []
    for c in REQUIRED_MIN + ["msn"]:
        if c in df.columns:
            keep.append(c)

    # On garde aussi toute colonne additionnelle "raisonnable" si tu veux (optionnel)
    # Ici, on reste strict : on ne garde que l’essentiel.
    df = df[keep].copy()

    # Nettoyage texte : airline_name, country, aircraft_type, registration
    for c in ["airline_name", "country", "aircraft_type", "registration", "msn"]:
        if c in df.columns:
            df[c] = clean_text_series(df[c])

    # Nettoyage fleet size en entier si présent
    if "total_fleet_size" in df.columns:
        # coercition en nombre
        df["total_fleet_size"] = pd.to_numeric(df["total_fleet_size"], errors="coerce")
        # On garde NaN si non parsable (on ne force pas)
    else:
        # si absent, on le crée vide (pour garder un schéma stable)
        df["total_fleet_size"] = pd.NA

    # Supprimer lignes inutilisables : airline_name & aircraft_type doivent exister
    before_drop = len(df)
    if "airline_name" in df.columns and "aircraft_type" in df.columns:
        df = df[(df["airline_name"] != "") & (df["aircraft_type"] != "")]
    after_drop = len(df)
    print(f"[INFO] Lignes supprimées (airline_name/aircraft_type vides): {before_drop - after_drop:,}")

    # Déduplication : priorité registration + airline_name
    # Fallback : airline_name + aircraft_type (si registration vide)
    before_dups = len(df)

    if "registration" in df.columns and "airline_name" in df.columns:
        # Cas 1 : registration disponible
        has_reg = df["registration"] != ""
        df_with_reg = df[has_reg].copy()
        df_no_reg = df[~has_reg].copy()

        # Dédoublonner (airline_name, registration)
        df_with_reg = df_with_reg.drop_duplicates(subset=["airline_name", "registration"], keep="first")

        # Fallback sur ceux sans registration : (airline_name, aircraft_type)
        if "aircraft_type" in df_no_reg.columns:
            df_no_reg = df_no_reg.drop_duplicates(subset=["airline_name", "aircraft_type"], keep="first")

        df = pd.concat([df_with_reg, df_no_reg], ignore_index=True)
    else:
        # fallback global si colonnes manquantes
        subset = [c for c in ["airline_name", "aircraft_type", "registration"] if c in df.columns]
        if subset:
            df = df.drop_duplicates(subset=subset, keep="first")

    after_dups = len(df)
    print(f"[INFO] Doublons supprimés: {before_dups - after_dups:,}")

    # Ajout meta légère (optionnel mais utile)
    df["cleaned_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Export
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"[OUT] {OUT_CSV}")
    print(f"[DONE] Lignes finales: {len(df):,} | Colonnes: {list(df.columns)}")


if __name__ == "__main__":
    main()
