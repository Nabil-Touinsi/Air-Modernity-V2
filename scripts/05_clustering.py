from __future__ import annotations

import json
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import StandardScaler

from . import config

# ============================================================
# Étape 5 — Clustering (KMeans + PCA) + Modèle KNN (simulateur)
#
# Objectif :
# - Segmenter les compagnies à partir de features "business" :
#   fleet_size, diversity_score, modernity_index, new_gen_share
# - Produire :
#   - un fichier enrichi (clusters + PCA) pour l'app Streamlit
#   - un KNN entraîné pour prédire le cluster dans le simulateur
#
# Entrée :
# - config.FILE_SCORES  (features filtrées, V2)
#
# Sorties :
# - config.FILE_CLUSTERS           (CSV enrichi avec cluster + PCA)
# - data/out/scaler.pkl            (StandardScaler)
# - data/out/knn_model.pkl         (KNN)
# - data/out/elbow_data.json       (courbe du coude)
# - data/out/knn_metrics.json      (accuracy + confusion matrix)
# ============================================================


def ensure_output_dir() -> None:
    os.makedirs(config.DATA_OUT, exist_ok=True)


def ensure_required_cols(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans FILE_SCORES: {missing}")


def run() -> None:
    print("--- ÉTAPE 3 : Clustering (V2) ---")
    ensure_output_dir()

    path_scores = str(config.FILE_SCORES)
    if not os.path.exists(path_scores):
        print(f"❌ Fichier manquant : {path_scores}")
        return

    df = pd.read_csv(path_scores)

    # Colonnes attendues pour garder le même front
    required = ["fleet_size", "diversity_score", "modernity_index"]
    ensure_required_cols(df, required)

    # Si new_gen_share n'existe plus (V2), on le reconstruit depuis modernity_index
    # → pour garder la structure UI inchangée
    if "new_gen_share" not in df.columns:
        df["new_gen_share"] = df["modernity_index"]

    # Filtre sécurité (normalement déjà fait en step2, mais on garde la robustesse)
    df_clean = df[df["fleet_size"] >= config.MIN_FLEET_SIZE].copy()

    # Features utilisées par l'IA (inchangées pour préserver le front)
    features = ["fleet_size", "diversity_score", "modernity_index", "new_gen_share"]

    # Sécurité : numeric + NaN -> 0
    X = df_clean[features].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Scaling
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 1) KMeans : création des clusters
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)
    df_clean["cluster"] = clusters

    # Coude (elbow)
    inertia = {"k": [], "inertia": []}
    for k in range(1, 10):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(X_scaled)
        inertia["k"].append(k)
        inertia["inertia"].append(float(km.inertia_))

    # 2) KNN : simulateur (prédire le cluster)
    y = clusters
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )

    knn = KNeighborsClassifier(n_neighbors=5)
    knn.fit(X_train, y_train)

    y_pred = knn.predict(X_test)
    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
    }

    # 3) PCA : projection 2D pour la cartographie
    pca = PCA(n_components=2, random_state=42)
    comps = pca.fit_transform(X_scaled)
    df_clean["pca_1"] = comps[:, 0]
    df_clean["pca_2"] = comps[:, 1]

    # 4) Sauvegardes (inchangées)
    df_clean.to_csv(config.FILE_CLUSTERS, index=False, encoding="utf-8")

    joblib.dump(scaler, os.path.join(config.DATA_OUT, "scaler.pkl"))
    joblib.dump(knn, os.path.join(config.DATA_OUT, "knn_model.pkl"))

    with open(os.path.join(config.DATA_OUT, "elbow_data.json"), "w", encoding="utf-8") as f:
        json.dump(inertia, f, ensure_ascii=False, indent=2)

    with open(os.path.join(config.DATA_OUT, "knn_metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    print("✅ Clustering terminé et modèles sauvegardés.")
    print(f"💾 CSV clusters : {config.FILE_CLUSTERS}")
    print(f"💾 Models dir   : {config.DATA_OUT}")


if __name__ == "__main__":
    run()
