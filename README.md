# 📘 README — Projet Air-Modernity
**Chaîne complète d’agrégation, analyse et visualisation de données**

## 1. Contexte du projet
Ce projet s’inscrit dans le cadre de l’évaluation transversale mobilisant les compétences en **Agrégation de données**, **Bases de données**, **Python** et **Data Visualisation**.

L’objectif est de concevoir une **chaîne complète de traitement de données**, depuis la collecte de données hétérogènes (web, fichiers, API) jusqu’à la production d’indicateurs analytiques et de visualisations exploitables.

Le cas d’étude retenu porte sur l’analyse de la **modernité des flottes aériennes mondiales**, à partir de données issues de plusieurs sources ouvertes.

---

## 2. Partie I — Récupération et agrégation des données

### 2.1 Web Scraping
- Scraping via **Wikidata API**, **Wikipedia** (fallback HTML) et **OpenFlights**
- Extraction des dates d’entrée en service des aéronefs
- Mise en cache et normalisation

Script principal :
```
scripts/01_fetch_aircraft_metadata_wikidata.py
```

### 2.2 Import de fichiers externes
Sources utilisées :
- XLSX : données Flightradar24
- CSV / JSON : métadonnées et mappings

Script de nettoyage :
```
scripts/00_clean_flightradar24.py
```

### 2.3 Consommation d’API
- API Wikidata (publique)
- Gestion du throttling et fallback

**Livrables Partie I**
- Scripts de scraping
- Scripts de nettoyage
- CSV consolidé

---

## 3. Partie II — Intégration et structuration des données

### 3.1 Modélisation
Structuration relationnelle logique (Airline, Aircraft, Country, Region).

### 3.2 Ingestion
Fusion et enrichissement via :
```
scripts/02_merge_enriched_and_add_region.py
```

Dataset final :
```
data/processed/fleet_enriched_v2.csv
```

---

## 4. Partie III — Analyses et indicateurs

Scripts :
```
scripts/03_generate_features.py
scripts/04_compute_scores.py
```

Indicateurs :
- Fleet size
- Année moyenne d’entrée en service
- Indice de modernité
- Diversité de flotte

---

## 5. Partie IV — Visualisation (Dataviz)

Outils :
- Streamlit
- Plotly
- Matplotlib

Dashboard :
```
streamlit run app.py
```

---

## 6. Livrables finaux
- Code complet
- Dataset consolidé
- Dashboard interactif
- Documentation technique

---

## 7. Limites et améliorations
- Base SQL persistante
- API REST
- Données environnementales

---

**Projet conforme aux exigences pédagogiques de l’évaluation.**

---

## 8. Guide d’installation et de prise en main (pour le groupe)

### 8.1 Prérequis
Assurez-vous d’avoir installé :
- **Python 3.10 ou supérieur**
- **pip** (gestionnaire de paquets Python)
- Un terminal (PowerShell, Bash, Terminal macOS)
- (Optionnel) Git

Vérification :
```bash
python --version
pip --version
```

---

### 8.2 Récupération du projet
Deux possibilités :

**Option A — via Git**
```bash
git clone <URL_DU_DEPOT>
cd Air-Modernity
```

**Option B — via archive ZIP**
- Télécharger l’archive du projet
- Extraire le dossier
- Ouvrir un terminal à la racine du projet

---

### 8.3 Création d’un environnement virtuel (recommandé)

```bash
python -m venv .venv
```

Activation :
- **Windows**
```bash
.venv\Scripts\activate
```

- **macOS / Linux**
```bash
source .venv/bin/activate
```

---

### 8.4 Installation des dépendances

```bash
pip install -r requirements.txt
```

---

### 8.5 Mise en place des données nécessaires

Vérifier la présence des fichiers suivants :

```
data/
├─ raw/
│  └─ flightradar24_raw.xlsx
├─ ref/
│  └─ country_region_mapping.csv
├─ interim/
│  └─ aircraft_type_manual_patch.csv   (optionnel)
```

⚠️ Les scripts de scraping utilisent Internet (Wikidata / Wikipedia).  
Une connexion active est nécessaire.

---

### 8.6 Lancement du pipeline complet

Depuis la racine du projet :
```bash
python main.py
```

Le pipeline exécute automatiquement :
1. Nettoyage des données brutes
2. Scraping et enrichissement avion
3. Fusion et ajout des régions
4. Calcul des features et scores
5. Clustering et réduction de dimension

Les fichiers générés apparaissent dans :
```
data/processed/
data/out/
```

---

### 8.7 Lancement du dashboard

```bash
streamlit run app.py
```

Ouvrir ensuite l’URL affichée dans le terminal (généralement http://localhost:8501).

---

### 8.8 Problèmes courants

- **Erreur `ModuleNotFoundError`**
  → Vérifier que l’environnement virtuel est activé.

- **Erreur réseau lors du scraping**
  → Relancer le script (le cache évite de tout re-scraper).

- **Données vides dans le dashboard**
  → Vérifier que `python main.py` s’est terminé sans erreur.

---

📌 Cette procédure garantit une **reproductibilité complète du projet**, conformément aux exigences pédagogiques.
