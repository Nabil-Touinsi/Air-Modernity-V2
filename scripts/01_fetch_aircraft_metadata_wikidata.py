from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Optional
from io import StringIO

import pandas as pd
import requests

# ============================================================
# Étape 1 — Fetch aircraft metadata (Multi-sources -> Wikidata)
#
# Objectif :
# - Construire un mapping "aircraft_type" -> entry_year (+ manufacturer si possible)
#
# Entrées :
# - data/interim/aircraft_types_in_dataset.csv   (optionnel mais recommandé)
#   ou data/processed/fleet_enriched_v2.csv
# - data/interim/aircraft_type_manual_patch.csv (recommandé)
#   -> permet d'avoir un model_name même si OpenFlights/Wikipedia/JSON ne matchent pas
# - data/raw/aircraftIcaoIata.json (optionnel)
#   -> mapping ICAO/IATA -> description (souvent très utile pour "no_model")
# - data/interim/aircraft_types_to_exclude.csv (optionnel)
#   -> liste de codes aircraft_type à EXCLURE du calcul (ne pas plomber le taux)
#
# Sources (ordre de remplissage model_name) :
# - Patch manuel (curation)                ✅ (prioritaire)
# - aircraftIcaoIata.json (ICAO/IATA)      ✅ (élargit beaucoup les types)
# - OpenFlights planes.dat                 ✅ (rapide)
# - Wikipedia (ICAO type designators)      ✅ (si accessible)
#
# Extraction entry_year :
# - Wikidata API (search + entity claims)  ✅ (P606/P729/P571)
# - Wikipedia "lead heuristic"            ✅ (fallback si QID OK mais année manquante)
#
# Sortie :
# - data/raw/aircraft_models_api.csv
#   colonnes : aircraft_type, model_name, manufacturer, entry_year, source
#
# Usage :
# - python -m scripts.01_fetch_aircraft_metadata_wikidata
#
# Logs :
# - Affiche une progression détaillée + stats (cache hit, not found, etc.)
# ============================================================

TYPES_IN_DATASET = Path("data/interim/aircraft_types_in_dataset.csv")
FLEET_ENRICHED_V2 = Path("data/processed/fleet_enriched_v2.csv")
MANUAL_PATCH = Path("data/interim/aircraft_type_manual_patch.csv")

# ✅ JSON additionnel (ICAO/IATA -> description)
AIRCRAFT_ICAO_IATA_JSON = Path("data/raw/aircraftIcaoIata.json")

# ✅ Exclusion list (codes aircraft_type à ignorer dans ce run)
EXCLUDE_TYPES = Path("data/interim/aircraft_types_to_exclude.csv")

OUT = Path("data/raw/aircraft_models_api.csv")
CACHE = Path("data/raw/aircraft_models_api_cache.csv")

OPENFLIGHTS_PLANES_DAT = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/planes.dat"

# Wikipedia (tables HTML -> mapping designator -> model)
WIKI_ICAO_PAGES = [
    "https://en.wikipedia.org/wiki/List_of_ICAO_aircraft_type_designators",
]

# Wikipedia REST summary (pour le fallback "lead heuristic")
WIKI_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/{}"

WIKIDATA_SEARCH = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"

# Wikidata props:
# - P606 : first flight
# - P729 : service entry
# - P571 : inception (souvent utilisé quand P606/P729 manquent)
P_FIRST_FLIGHT = "P606"
P_SERVICE_ENTRY = "P729"
P_INCEPTION = "P571"

UA = "Air-Modernity/1.0 (student project; contact: none)"

# ------------------------------------------------------------
# Réglages logs / throttling
# ------------------------------------------------------------
LOG_EVERY = 10          # log toutes les N lignes (progress)
SAVE_EVERY = 50         # sauvegarde cache intermédiaire toutes les N lignes
SLEEP_BETWEEN_CALLS = 0.2
TIMEOUT_SEARCH = 30
TIMEOUT_ENTITY = 30
TIMEOUT_WIKI_SUMMARY = 20


def extract_year(value: str) -> Optional[int]:
    """
    Extrait une année 4 chiffres d'une date ISO (ex: 2011-03-01T00:00:00Z)
    ou d'un texte (lead Wikipedia).
    """
    if not value:
        return None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", str(value))
    return int(m.group(1)) if m else None


def normalize_model_name(name: str) -> str:
    """
    Normalisation légère pour améliorer la recherche Wikidata.
    - retire le contenu entre parenthèses
    - nettoie espaces
    - petites corrections fréquentes
    """
    if not name:
        return ""
    s = str(name).strip()

    # retire "(...)" pour éviter des libellés type "Embraer 175 (short wing)"
    s = re.sub(r"\s*\([^)]*\)\s*", " ", s)

    # espaces
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_search_candidates(name: str) -> list[str]:
    """
    Génère plusieurs requêtes (fallback) pour améliorer le taux de QID trouvé.
    """
    base = str(name).strip()
    norm = normalize_model_name(base)

    cands: list[str] = []
    for x in (base, norm):
        if x and x not in cands:
            cands.append(x)

    # Heuristiques simples (variantes courantes)
    if "A330-900" in norm and "Airbus" in norm:
        alt = "Airbus A330neo"
        if alt not in cands:
            cands.append(alt)

    # "Boeing 777-200LR" -> parfois écrit "Boeing 777 200LR"
    if "Boeing" in norm and "-" in norm:
        alt = norm.replace("-", " ").replace("  ", " ").strip()
        if alt not in cands:
            cands.append(alt)

    return cands


def load_types() -> pd.DataFrame:
    print("📥 Chargement des types d'avion (aircraft_type)...")

    if TYPES_IN_DATASET.exists():
        df = pd.read_csv(TYPES_IN_DATASET)
        out = df[["aircraft_type"]].dropna().drop_duplicates()
        out["aircraft_type"] = out["aircraft_type"].astype("string").str.strip()
        print(f"✅ Types chargés depuis {TYPES_IN_DATASET} | distinct={len(out)}")
        return out

    if not FLEET_ENRICHED_V2.exists():
        raise FileNotFoundError("Ni aircraft_types_in_dataset.csv ni fleet_enriched_v2.csv n'existent.")

    df = pd.read_csv(FLEET_ENRICHED_V2)
    if "aircraft_type" not in df.columns:
        raise ValueError("Colonne 'aircraft_type' introuvable dans fleet_enriched_v2.csv")

    out = df[["aircraft_type"]].astype("string").dropna().drop_duplicates()
    out["aircraft_type"] = out["aircraft_type"].astype("string").str.strip()
    print(f"✅ Types extraits depuis {FLEET_ENRICHED_V2} | distinct={len(out)}")
    return out


def load_exclusion_list() -> set[str]:
    """
    Charge une liste de types à exclure du scraping (pour ne pas plomber le taux).

    Format attendu :
    - CSV avec une colonne 'aircraft_type' (recommandé)
    - sinon : on prend la 1ère colonne comme fallback

    Effet :
    - Les types exclus ne sont PAS scrapés
    - Le "Coverage entry_year" est calculé sur les types restants
    """
    print("📥 Chargement de la liste d'exclusion (optionnelle)...")

    if not EXCLUDE_TYPES.exists():
        print(f"⚠️ Liste d'exclusion absente : {EXCLUDE_TYPES} (on continue sans)")
        return set()

    df = pd.read_csv(EXCLUDE_TYPES)
    if df.empty:
        print(f"⚠️ Liste d'exclusion vide : {EXCLUDE_TYPES} (on continue sans)")
        return set()

    col = "aircraft_type" if "aircraft_type" in df.columns else df.columns[0]

    excl = (
        df[[col]]
        .dropna()
        .astype("string")[col]
        .str.strip()
        .str.upper()
        .tolist()
    )

    s = set(excl)
    print(f"✅ Liste d'exclusion chargée | distinct={len(s)}")
    return s


def load_manual_patch() -> pd.DataFrame:
    print("📥 Chargement du patch manuel (aircraft_type -> model_name_manual)...")

    if not MANUAL_PATCH.exists():
        print(f"⚠️ Patch manuel absent : {MANUAL_PATCH} (on continue sans)")
        return pd.DataFrame(columns=["aircraft_type", "model_name_manual"])

    patch = pd.read_csv(MANUAL_PATCH)
    if "aircraft_type" not in patch.columns or "model_name_manual" not in patch.columns:
        raise ValueError("Le patch manuel doit contenir: aircraft_type, model_name_manual")

    patch = patch[["aircraft_type", "model_name_manual"]].dropna().drop_duplicates(subset=["aircraft_type"])
    patch["aircraft_type"] = patch["aircraft_type"].astype("string").str.strip()
    patch["model_name_manual"] = patch["model_name_manual"].astype("string").str.strip()

    print(f"✅ Patch manuel chargé : {MANUAL_PATCH} | mappings={len(patch)}")
    return patch


def load_aircraft_icao_iata_json() -> pd.DataFrame:
    """
    Charge data/raw/aircraftIcaoIata.json

    Structure observée chez toi :
    - icaoCode
    - iataCode
    - description

    On exploite :
    - aircraft_type = icaoCode
    - model_name_json = description
    """
    print("📥 Chargement aircraftIcaoIata.json (ICAO/IATA -> model)...")

    if not AIRCRAFT_ICAO_IATA_JSON.exists():
        print(f"⚠️ Fichier absent : {AIRCRAFT_ICAO_IATA_JSON} (on continue sans)")
        return pd.DataFrame(columns=["aircraft_type", "model_name_json"])

    try:
        raw = AIRCRAFT_ICAO_IATA_JSON.read_text(encoding="utf-8")
        data = json.loads(raw)

        # le fichier peut être une liste d'objets
        if not isinstance(data, list):
            print("⚠️ JSON inattendu (pas une liste) — on continue sans")
            return pd.DataFrame(columns=["aircraft_type", "model_name_json"])

        rows = []
        for it in data:
            if not isinstance(it, dict):
                continue
            icao = str(it.get("icaoCode", "")).strip().upper()
            desc = str(it.get("description", "")).strip()

            if not icao or not desc:
                continue

            # filtre : ICAO généralement 3-4 chars alphanum
            if not re.match(r"^[A-Z0-9]{3,4}$", icao):
                continue

            rows.append({"aircraft_type": icao, "model_name_json": desc})

        df = pd.DataFrame(rows).drop_duplicates(subset=["aircraft_type"])
        print(f"✅ aircraftIcaoIata.json chargé | mappings={len(df)}")
        return df

    except Exception as e:
        print(f"⚠️ JSON illisible/parse impossible : {AIRCRAFT_ICAO_IATA_JSON} | err={e}")
        return pd.DataFrame(columns=["aircraft_type", "model_name_json"])


def fetch_openflights_planes() -> pd.DataFrame:
    print("🌐 Téléchargement OpenFlights planes.dat...")
    r = requests.get(OPENFLIGHTS_PLANES_DAT, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()

    rows = []
    for line in r.text.splitlines():
        line = line.strip()
        if not line:
            continue

        # split CSV naïf (planes.dat est simple, quoted)
        parts = []
        cur = ""
        in_q = False
        for ch in line:
            if ch == '"':
                in_q = not in_q
                continue
            if ch == "," and not in_q:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        parts.append(cur)

        if len(parts) < 3:
            continue

        name = parts[0].strip()
        iata = parts[1].strip()
        icao = parts[2].strip()
        if icao:
            rows.append({"aircraft_type": icao, "model_name_openflights": name, "iata_code": iata})

    df = pd.DataFrame(rows)
    df["aircraft_type"] = df["aircraft_type"].astype("string").str.strip()
    df = df.drop_duplicates(subset=["aircraft_type"])

    print(f"✅ OpenFlights chargé | rows={len(df)} | cols={list(df.columns)}")
    return df


def fetch_wikipedia_icao_designators() -> pd.DataFrame:
    """
    Récupère une table Wikipedia (si accessible) :
    ICAO designator -> aircraft model name

    NOTE:
    - Wikipedia peut renvoyer 403 sur l'URL HTML si la requête ressemble à un bot.
    - On contourne en téléchargeant le HTML via requests + UA, puis pd.read_html(html).
    """
    print("🌐 Téléchargement Wikipedia (ICAO aircraft type designators)...")
    for url in WIKI_ICAO_PAGES:
        try:
            # 1) récupérer le HTML avec requests (UA custom)
            r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()

            # 2) parser les tables depuis le HTML
            tables = pd.read_html(StringIO(r.text))
            best = None

            for t in tables:
                cols = [str(c).strip().lower() for c in t.columns]
                if not cols:
                    continue

                # cherche une colonne ICAO / Designator
                icao_col = None
                for c in t.columns:
                    cl = str(c).strip().lower()
                    if "icao" in cl or "designator" in cl:
                        icao_col = c
                        break
                if icao_col is None:
                    continue

                # cherche une colonne modèle
                model_col = None
                for c in t.columns:
                    cl = str(c).strip().lower()
                    if any(k in cl for k in ["aircraft", "model", "type", "name"]):
                        if c != icao_col:
                            model_col = c
                            break
                if model_col is None:
                    continue

                tmp = t[[icao_col, model_col]].copy()
                tmp.columns = ["aircraft_type", "model_name_wikipedia"]
                tmp["aircraft_type"] = tmp["aircraft_type"].astype("string").str.strip()
                tmp["model_name_wikipedia"] = tmp["model_name_wikipedia"].astype("string").str.strip()

                tmp = tmp[tmp["aircraft_type"].str.match(r"^[A-Z0-9]{3,4}$", na=False)]
                tmp = tmp.dropna().drop_duplicates(subset=["aircraft_type"])

                if best is None or len(tmp) > len(best):
                    best = tmp

            if best is not None and len(best) > 0:
                print(f"✅ Wikipedia chargé | url={url} | mappings={len(best)}")
                return best

            print(f"⚠️ Wikipedia OK mais aucune table exploitable détectée | url={url}")

        except Exception as e:
            print(f"⚠️ Wikipedia inaccessible/parse impossible | url={url} | err={e}")

    print("⚠️ Wikipedia non disponible (on continue sans)")
    return pd.DataFrame(columns=["aircraft_type", "model_name_wikipedia"])


def wikidata_search(q: str) -> Optional[str]:
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "limit": 1,
        "search": q,
    }
    r = requests.get(
        WIKIDATA_SEARCH,
        params=params,
        headers={"User-Agent": UA},
        timeout=TIMEOUT_SEARCH,
    )
    r.raise_for_status()
    data = r.json()
    hits = data.get("search", [])
    if not hits:
        return None
    return hits[0].get("id")


def wikidata_search_best(name: str) -> tuple[Optional[str], str]:
    """
    Essaie plusieurs requêtes, retourne (qid, query_used).
    """
    candidates = build_search_candidates(name)
    for q in candidates:
        qid = wikidata_search(q)
        if qid:
            return qid, q
    return None, ""


def wikidata_get_year_and_enwiki(qid: str) -> tuple[Optional[int], str, str]:
    """
    Retourne (year, source_prop, enwiki_title)

    source_prop = P606 ou P729 ou P571 ou "".
    enwiki_title = titre enwiki si dispo (sitelinks)
    """
    r = requests.get(
        WIKIDATA_ENTITY.format(qid),
        headers={"User-Agent": UA},
        timeout=TIMEOUT_ENTITY,
    )
    r.raise_for_status()
    data = r.json()

    ent = data["entities"].get(qid, {})
    claims = ent.get("claims", {})

    # récupère le titre Wikipedia EN (si présent)
    enwiki_title = ""
    sitelinks = ent.get("sitelinks", {}) or {}
    if "enwiki" in sitelinks and isinstance(sitelinks["enwiki"], dict):
        enwiki_title = str(sitelinks["enwiki"].get("title", "")).strip()

    # 1) first flight -> service entry -> inception
    for prop in (P_FIRST_FLIGHT, P_SERVICE_ENTRY, P_INCEPTION):
        c = claims.get(prop, [])
        if not c:
            continue
        mainsnak = c[0].get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        v = datavalue.get("value", {})
        if isinstance(v, dict) and "time" in v:
            year = extract_year(v["time"])
            if year:
                return year, prop, enwiki_title

    return None, "", enwiki_title


def wikipedia_lead_year(title: str) -> Optional[int]:
    """
    Fallback : essaye d'extraire une année depuis le résumé Wikipedia (lead).
    On cherche des patterns comme :
    - "first flew in 19xx"
    - "entered service in 19xx"
    - "introduced in 19xx"
    - "first flight was in 19xx"
    """
    if not title:
        return None

    # encode minimal : espaces -> underscore
    safe_title = title.replace(" ", "_")

    try:
        url = WIKI_SUMMARY_API.format(safe_title)
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT_WIKI_SUMMARY)

        # Wikipedia REST renvoie parfois 404 si titre pas bon
        if r.status_code != 200:
            return None

        data = r.json()
        text = str(data.get("extract", "")).strip()
        if not text:
            return None

        # patterns ciblés (plus robustes qu'un "premier 19xx" au hasard)
        patterns = [
            r"entered service in (19\d{2}|20\d{2})",
            r"introduced in (19\d{2}|20\d{2})",
            r"first flight (?:was )?in (19\d{2}|20\d{2})",
            r"first flew in (19\d{2}|20\d{2})",
            r"made its first flight in (19\d{2}|20\d{2})",
            r"maiden flight (?:was )?in (19\d{2}|20\d{2})",
        ]

        for p in patterns:
            m = re.search(p, text, flags=re.IGNORECASE)
            if m:
                return int(m.group(1))

        # fallback doux : si rien trouvé mais on a un 19xx/20xx dans le lead
        return extract_year(text)

    except Exception:
        return None


def safe_flush_cache(cache: pd.DataFrame) -> None:
    """
    Sauvegarde intermédiaire du cache pour ne pas perdre la progression.
    """
    try:
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        cache.drop_duplicates(subset=["model_name_norm"], keep="last").to_csv(CACHE, index=False, encoding="utf-8")
        print(f"💾 Cache sauvegardé : {CACHE} (rows={len(cache)})")
    except Exception as e:
        print(f"⚠️ Impossible de sauvegarder le cache : {e}")


def main() -> None:
    print("============================================================")
    print("🚀 START — Fetch metadata (Multi-sources -> Wikidata + Wikipedia lead)")
    print("============================================================")

    OUT.parent.mkdir(parents=True, exist_ok=True)

    types = load_types()

    # ✅ Exclusions (si fichier présent)
    excluded = load_exclusion_list()
    if excluded:
        before = len(types)
        types = types[~types["aircraft_type"].astype("string").str.upper().isin(excluded)].copy()
        after = len(types)
        print(f"🧹 Exclusion appliquée | retirés={before - after} | restants={after}")

    json_map = load_aircraft_icao_iata_json()
    planes = fetch_openflights_planes()
    wiki = fetch_wikipedia_icao_designators()
    patch = load_manual_patch()

    # Merge multi-sources
    df = types.merge(json_map, on="aircraft_type", how="left")
    df = df.merge(planes[["aircraft_type", "model_name_openflights"]], on="aircraft_type", how="left")
    df = df.merge(wiki, on="aircraft_type", how="left")
    df = df.merge(patch, on="aircraft_type", how="left")

    # Priorité : patch manuel -> JSON -> OpenFlights -> Wikipedia
    df["model_name"] = (
        df["model_name_manual"]
        .combine_first(df["model_name_json"])
        .combine_first(df["model_name_openflights"])
        .combine_first(df["model_name_wikipedia"])
    )
    df = df.drop(columns=["model_name_manual", "model_name_json", "model_name_openflights", "model_name_wikipedia"])

    total = len(df)
    missing_model = int(df["model_name"].isna().sum())
    print(f"🔎 Model_name après Patch + JSON + OpenFlights + Wikipedia : total_types={total} | sans model_name={missing_model}")

    # Cache (évite de re-scraper Wikidata/Wikipedia lead)
    cache = pd.DataFrame(columns=["model_name_norm", "entry_year", "source"])
    if CACHE.exists():
        cache = pd.read_csv(CACHE)
        print(f"♻️ Cache existant chargé : {CACHE} (rows={len(cache)})")
    else:
        print("♻️ Aucun cache existant (premier run).")

    cache_map = dict(zip(cache["model_name_norm"].astype(str), cache["entry_year"]))
    cache_src = dict(zip(cache["model_name_norm"].astype(str), cache["source"]))

    out_rows = []

    # Counters
    found = 0
    cache_hits = 0
    no_model = 0
    no_qid = 0
    qid_ok_no_year = 0
    wiki_lead_hits = 0
    errors = 0

    t0 = time.time()

    for i, row in df.iterrows():
        idx = i + 1
        icao = str(row["aircraft_type"])

        name_raw = row.get("model_name", None)
        name_raw = str(name_raw) if pd.notna(name_raw) else ""
        name_norm = normalize_model_name(name_raw)

        entry_year: Optional[int] = None
        source = ""

        if not name_norm:
            no_model += 1
            out_rows.append(
                {
                    "aircraft_type": icao,
                    "model_name": pd.NA,
                    "manufacturer": pd.NA,
                    "entry_year": pd.NA,
                    "source": pd.NA,
                }
            )
            if idx % LOG_EVERY == 0 or idx <= 5:
                print(f"[{idx}/{total}] ❌ {icao} | model_name introuvable (Patch/JSON/OpenFlights/Wikipedia)")
            continue

        # 1) Cache hit (clé = nom normalisé)
        if name_norm in cache_map and pd.notna(cache_map[name_norm]):
            entry_year = int(cache_map[name_norm])
            source = str(cache_src.get(name_norm, "cache"))
            cache_hits += 1

            out_rows.append(
                {
                    "aircraft_type": icao,
                    "model_name": name_raw,
                    "manufacturer": pd.NA,
                    "entry_year": entry_year,
                    "source": source,
                }
            )

            if idx % LOG_EVERY == 0 or idx <= 5:
                elapsed = time.time() - t0
                rate = idx / elapsed if elapsed > 0 else 0
                eta = (total - idx) / rate if rate > 0 else 0
                print(
                    f"[{idx}/{total}] ✅ CACHE {icao} | {name_raw} -> {entry_year} ({source}) "
                    f"| found={found} cache={cache_hits} | ETA~{eta/60:.1f}m"
                )
            continue

        # 2) Wikidata search + entity (+ fallback Wikipedia lead)
        try:
            if idx % LOG_EVERY == 0 or idx <= 5:
                print(f"[{idx}/{total}] 🔎 Wikidata search: {icao} | '{name_raw}'")

            qid, q_used = wikidata_search_best(name_raw)
            time.sleep(SLEEP_BETWEEN_CALLS)

            if not qid:
                no_qid += 1
                if idx % LOG_EVERY == 0 or idx <= 5:
                    extra = f" | norm='{name_norm}'" if name_norm != name_raw else ""
                    print(f"[{idx}/{total}] ⚠️ Aucun QID trouvé pour '{name_raw}'{extra}")
            else:
                if idx % LOG_EVERY == 0 or idx <= 5:
                    used = f" (query='{q_used}')" if q_used and q_used != name_raw else ""
                    print(f"[{idx}/{total}] 🧩 QID={qid}{used} | fetch entity...")

                entry_year, prop, enwiki_title = wikidata_get_year_and_enwiki(qid)
                time.sleep(SLEEP_BETWEEN_CALLS)

                if entry_year:
                    source = f"wikidata:{prop}"
                    found += 1
                    if idx % LOG_EVERY == 0 or idx <= 5:
                        print(f"[{idx}/{total}] ✅ FOUND {icao} | {name_raw} -> {entry_year} ({source})")

                else:
                    # fallback Wikipedia lead
                    qid_ok_no_year += 1
                    if idx % LOG_EVERY == 0 or idx <= 5:
                        print(f"[{idx}/{total}] ⚠️ QID OK mais pas d'année (P606/P729/P571) pour '{name_raw}'")

                    # si enwiki_title dispo, on tente l'extraction sur le lead
                    if enwiki_title:
                        y2 = wikipedia_lead_year(enwiki_title)
                        if y2:
                            entry_year = y2
                            source = "wikipedia:lead"
                            wiki_lead_hits += 1
                            found += 1
                            if idx % LOG_EVERY == 0 or idx <= 5:
                                print(f"[{idx}/{total}] ✅ WIKI_LEAD {icao} | {enwiki_title} -> {entry_year} ({source})")

                # update cache si on a obtenu une année (Wikidata ou lead)
                if entry_year:
                    cache = pd.concat(
                        [cache, pd.DataFrame([{"model_name_norm": name_norm, "entry_year": entry_year, "source": source}])],
                        ignore_index=True,
                    )
                    cache_map[name_norm] = entry_year
                    cache_src[name_norm] = source

        except Exception as e:
            errors += 1
            if idx % LOG_EVERY == 0 or idx <= 5:
                print(f"[{idx}/{total}] ❌ ERREUR sur '{name_raw}' ({icao}) : {e}")

        out_rows.append(
            {
                "aircraft_type": icao,
                "model_name": name_raw,
                "manufacturer": pd.NA,
                "entry_year": entry_year if entry_year else pd.NA,
                "source": source if source else pd.NA,
            }
        )

        # checkpoint save cache
        if idx % SAVE_EVERY == 0:
            print("------------------------------------------------------------")
            print(
                f"📌 CHECKPOINT [{idx}/{total}] "
                f"| found={found} cache={cache_hits} no_model={no_model} no_qid={no_qid} "
                f"qid_no_year={qid_ok_no_year} wiki_lead={wiki_lead_hits} errors={errors}"
            )
            safe_flush_cache(cache)
            print("------------------------------------------------------------")

    # Final export
    out = pd.DataFrame(out_rows)
    out.to_csv(OUT, index=False, encoding="utf-8")

    safe_flush_cache(cache)

    cov = out["entry_year"].notna().mean() if len(out) else 0
    print("============================================================")
    print("✅ FINISHED")
    print(f"Sortie : {OUT}")
    print(f"Coverage entry_year : {cov:.1%} ({out['entry_year'].notna().sum()}/{len(out)})")
    print("--- Stats run ---")
    print(
        f"found={found} | cache_hits={cache_hits} | no_model={no_model} | no_qid={no_qid} | "
        f"qid_ok_no_year={qid_ok_no_year} | wiki_lead_hits={wiki_lead_hits} | errors={errors}"
    )
    print(f"Cache : {CACHE}")
    print("============================================================")


if __name__ == "__main__":
    main()
