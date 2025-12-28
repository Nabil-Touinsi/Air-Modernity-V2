from __future__ import annotations

import importlib


# ============================================================
# Main — Orchestration du pipeline Air-Modernity (V2)
#
# Objectif :
# - Enchaîner les scripts V2 qui génèrent les fichiers utilisés
#   par l'app Streamlit (même visuel, nouvelles données).
#
# Étapes exécutées :
# 03_generate_features.py  -> features par compagnie
# 04_build_scores.py       -> filtrage + export scores
# 05_clustering.py         -> KMeans + PCA + KNN + export clusters + artefacts ML
#
# Entrées (pré-requises) :
# - data/processed/fleet_enriched.csv           (généré par 02_merge...)
# - data/ref/country_region_mapping.csv         (mapping pays -> region)
#
# Sorties :
# - data/processed/airlines_features.csv
# - data/processed/airlines_scores.csv
# - data/processed/airlines_clusters.csv
# - data/out/scaler.pkl
# - data/out/knn_model.pkl
# - data/out/elbow_data.json
# - data/out/knn_metrics.json
#
# Usage :
# 1) python main.py
# 2) streamlit run app.py
# ============================================================


def run_step(module_name: str) -> None:
    """
    Charge dynamiquement un module scripts.<module_name> puis exécute :
    - main() si présent
    - sinon run() si présent
    """
    mod = importlib.import_module(f"scripts.{module_name}")

    if hasattr(mod, "main"):
        mod.main()
        return
    if hasattr(mod, "run"):
        mod.run()
        return

    raise AttributeError(f"{module_name}.py doit exposer main() ou run().")


if __name__ == "__main__":
    print(">>> DÉMARRAGE PROJET AIR-MODERNITY (V2) <<<")
    try:
        run_step("03_generate_features")
        run_step("04_build_scores")
        run_step("05_clustering")

        print("\n>>> TRAITEMENT TERMINÉ. LANCEZ 'streamlit run app.py' <<<")
    except Exception as e:
        print(f"\n❌ ERREUR : {e}")
