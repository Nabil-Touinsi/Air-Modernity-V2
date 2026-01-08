from __future__ import annotations

import json

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ============================================================
# Streamlit App — Air-Modernity (Front inchangé, pipeline V2)
#
# Entrées :
# - data/processed/airlines_clusters.csv   (CSV enrichi : cluster + PCA)
# - data/out/elbow_data.json              (KMeans elbow)
# - data/out/knn_metrics.json             (accuracy + confusion matrix)
# - data/out/knn_model.pkl                (KNN)
# - data/out/scaler.pkl                   (StandardScaler)
#
# Si tu vois "Données introuvables" :
# - exécute d'abord : python main.py
# ============================================================

st.set_page_config(page_title="Air-Modernity AI", layout="wide", page_icon="")


# --- CHARGEMENT ---
@st.cache_data
def load_all_data():
    try:
        df = pd.read_csv("data/processed/airlines_clusters.csv")

        # Artefacts ML / métriques (pipeline V2 -> data/out)
        with open("data/out/elbow_data.json", "r", encoding="utf-8") as f:
            elbow = json.load(f)

        with open("data/out/knn_metrics.json", "r", encoding="utf-8") as f:
            knn_metrics = json.load(f)

        model = joblib.load("data/out/knn_model.pkl")
        scaler = joblib.load("data/out/scaler.pkl")

        return df, elbow, knn_metrics, model, scaler

    except FileNotFoundError:
        return None, None, None, None, None


df, elbow_data, knn_metrics, knn_model, scaler = load_all_data()

if df is None:
    st.error("⚠️ Données introuvables. Lancez 'python main.py' d'abord.")
    st.stop()

# --- SIDEBAR ---
st.sidebar.header(" Filtres")
regions = st.sidebar.multiselect(
    "Régions",
    df["region"].dropna().unique(),
    default=df["region"].dropna().unique(),
)

min_fleet = st.sidebar.slider("Taille de Flotte Min", 5, 200, 5)

df_filt = df[(df["region"].isin(regions)) & (df["fleet_size"] >= min_fleet)]


# --- TITRE ---
st.title(" Air-Modernity : Data Mining Project")

tab1, tab2 = st.tabs([" Dashboard Business", " Laboratoire IA (K-Means/KNN)"])

# ==========================================
# ONGLET 1 : BUSINESS
# ==========================================
with tab1:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Compagnies", len(df_filt))
    c2.metric("Modernité Moyenne", f"{df_filt['modernity_index'].mean():.1%}")
    c3.metric("Flotte Moyenne", int(df_filt["fleet_size"].mean()) if not df_filt.empty else 0)

    if not df_filt.empty:
        top = df_filt.loc[df_filt["modernity_index"].idxmax()]
        c4.metric(" Top Modernité", top["airline_name"], f"{top['modernity_index']:.1%}")

    st.divider()

    col_g1, col_g2 = st.columns([2, 1])
    with col_g1:
        st.subheader(" Cartographie des Clusters (PCA)")
        fig_pca = px.scatter(
            df_filt[df_filt["cluster"] >= 0],
            x="pca_1",
            y="pca_2",
            color="cluster",
            size="fleet_size",
            hover_name="airline_name",
            color_continuous_scale="Viridis",
            template="plotly_dark",
        )
        st.plotly_chart(fig_pca, use_container_width=True)

    with col_g2:
        st.subheader(" Performance par Région")
        reg_score = (
            df_filt.groupby("region")["modernity_index"]
            .mean()
            .reset_index()
            .sort_values("modernity_index")
        )
        fig_bar = px.bar(
            reg_score,
            x="modernity_index",
            y="region",
            orientation="h",
            template="plotly_dark",
            color="modernity_index",
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.divider()
    st.subheader(" Liste Détaillée des Compagnies")

    cols_to_show = ["airline_name", "region", "fleet_size", "modernity_index", "new_gen_share", "cluster"]
    existing_cols = [c for c in cols_to_show if c in df_filt.columns]

    st.dataframe(
        df_filt[existing_cols]
        .sort_values("modernity_index", ascending=False)
        .style.format(
            {
                "modernity_index": "{:.1%}",
                "new_gen_share": "{:.1%}",
                "fleet_size": "{:.0f}",
            }
        )
        .background_gradient(subset=["modernity_index"], cmap="Greens"),
        use_container_width=True,
    )

# ==========================================
# ONGLET 2 : IA
# ==========================================
with tab2:
    st.header(" Analyse Algorithmique")

    col_km, col_knn = st.columns(2)

    # --- K-MEANS + INTERPRÉTATION ---
    with col_km:
        st.subheader(" K-Means (Méthode du Coude)")
        fig_elbow = go.Figure()
        fig_elbow.add_trace(
            go.Scatter(
                x=elbow_data["k"],
                y=elbow_data["inertia"],
                mode="lines+markers",
                marker=dict(color="red"),
            )
        )
        fig_elbow.update_layout(
            xaxis_title="Nombre de Clusters (k)",
            yaxis_title="Inertie",
            template="plotly_dark",
        )
        st.plotly_chart(fig_elbow, use_container_width=True)

        st.info(
            """
        **Interprétation :**
        Ce graphique permet de déterminer le nombre optimal de groupes.
        - L'**Inertie** (axe Y) mesure à quel point les clusters sont compacts.
        - On cherche le point de cassure ("le coude") où la courbe s'aplatit.
        - *Ici, la cassure est visible à k=4, ce qui valide notre choix de 4 groupes.*
        """
        )

    # --- KNN + INTERPRÉTATION ---
    with col_knn:
        st.subheader(f" KNN (Précision : {knn_metrics['accuracy']:.1%})")
        cm = np.array(knn_metrics["confusion_matrix"])
        fig_cm = px.imshow(cm, text_auto=True, color_continuous_scale="Blues", title="Matrice de Confusion")
        st.plotly_chart(fig_cm, use_container_width=True)

        st.info(
            """
        **Interprétation :**
        Cette matrice évalue la performance du modèle supervisé.
        - **Diagonale (Bleu foncé)** : Nombre de compagnies correctement classées.
        - **Hors diagonale** : Erreurs de prédiction.
        - *Un score élevé prouve que les clusters sont bien distincts et que l'IA arrive à les identifier facilement.*
        """
        )

    st.divider()

    # --- PROFILS TYPES (Filtrés sans le -1) ---
    st.subheader(" Cibles pour le Simulateur")
    st.markdown("Les moyennes des **clusters**.")

    clean_clusters = df[df["cluster"] >= 0]
    cols_profile = [c for c in ["fleet_size", "diversity_score", "modernity_index", "new_gen_share"] if c in clean_clusters.columns]

    cluster_profiles = clean_clusters.groupby("cluster")[cols_profile].mean().reset_index()

    st.dataframe(
        cluster_profiles.style.format(
            {
                "fleet_size": "{:.0f}",
                "diversity_score": "{:.2f}",
                "modernity_index": "{:.2f}",
                "new_gen_share": "{:.2f}",
            }
        ).background_gradient(cmap="Blues"),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("Simulateur de Prédiction")

    col_sim1, col_sim2, col_sim3, col_sim4 = st.columns(4)

    val_fleet = col_sim1.number_input("Taille Flotte (0 - 5000)", 0, 5000, 100)
    val_div = col_sim2.slider("Diversité", 0.0, 1.0, 0.5)
    val_mod = col_sim3.slider("Modernité", 0.0, 1.0, 0.4)
    val_ng = col_sim4.slider("Part New Gen", 0.0, 1.0, 0.3)

    if st.button("Prédire le Cluster"):
        user_data = np.array([[val_fleet, val_div, val_mod, val_ng]])
        user_data_scaled = scaler.transform(user_data)
        prediction = knn_model.predict(user_data_scaled)[0]

        st.success(f"Résultat de l'analyse IA : CLUSTER {prediction}")
        st.info(f"Cette compagnie appartient au Cluster {prediction}.")
