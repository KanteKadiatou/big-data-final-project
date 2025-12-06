# big-data-final-project
Projet Master 1 DIT 2025

# Plateforme Big Data pour l’analyse des comportements d’apprentissage en ligne

## 🎯 Objectif

Ce projet met en place une plateforme Big Data complète permettant de **collecter**, **stocker**, **traiter**, **analyser** et **visualiser** les comportements d’apprentissage en ligne à partir de données réelles (YouTube, Kaggle) et simulées.

L’objectif est d’extraire des **KPIs d’engagement**, de suivi et de performance des apprenants.

---

# 🏗️ Architecture globale du projet

```
BIG-DATA-FINAL-PROJECT
│
├── airflow_dags/               → Pipelines Airflow (DAGs)
│
├── data/
│   ├── raw/                    → Données brutes (Kaggle, YouTube, données simulées)
│   │   ├── kaggle/
│   │   ├── simulated/
│   │   └── youtube/
│   ├── processed/              → Données nettoyées / enrichies
│   └── curated/                → Données finales prêtes pour l’analyse
│
├── infrastructure/
│   └── minio/
│       ├── docker-compose.yml  → Lancement du serveur MinIO
│       └── minio_data/         → Répertoires utilisés comme Data Lake MinIO
│           ├── datalake-raw
│           ├── datalake-clean
│           ├── datalake-curated
│           └── processed
│
├── notebooks/
│   └── exploration.ipynb       → Analyse exploratoire et tests
│
├── src/
│   ├── collectors/             → Scripts d’ingestion (YouTube API, Kaggle, Faker)
│   ├── processing/             → Nettoyage, transformation (PySpark)
│   ├── analytics/              → KPIs, agrégations, modèles
│   └── ingestion.py            → Script principal d’ingestion
│
└── README.md
```

---

# 📥 Phase 1 — Collecte des données

Sources intégrées :

### ✔ Kaggle (datasets publics)

* `students_adaptability_level_online_education.csv`
* `StudentsPerformance.csv`

### ✔ Simulations (Faker)

* `students_synthetic.csv`

### ✔ API YouTube

Collecte de métadonnées (vues, likes, durée, titres...) dans :

```
data/raw/youtube/videos.json
```

Scripts situés dans :
`src/collectors/`

---

# 🗄️ Phase 2 — Stockage (Data Lake MinIO)

Le Data Lake suit une organisation industrielle en zones :

```
minio_data/
├── datalake-raw/       → Données brutes
├── datalake-clean/     → Données nettoyées
├── datalake-curated/   → Données finales
└── processed/          → Transformations intermédiaires
```

Lancement de MinIO :

```
cd infrastructure/minio
docker-compose up -d
```

---

# ⚙️ Phase 3 — Traitement & Analyse (PySpark)

Les pipelines réalisent :

* Nettoyage (nulls, formats, colonnes inutiles)
* Normalisation des jeux de données
* Fusion multi-sources
* Calcul d’indicateurs pédagogiques

Principaux KPIs extraits :

* Taux d’engagement
* Taux de complétion
* Temps passé
* Rétention
* Popularité des cours
* Activité temporelle

Scripts situés dans :
`src/processing/` et `src/analytics/`

---

# 🔄 Phase 4 — Orchestration (Apache Airflow)

Automatisation des workflows via :

```
airflow_dags/
```

DAGs inclus :

* ingestion quotidienne des données
* nettoyage PySpark
* calcul des KPIs
* dépôt dans `/curated`

---

# 📊 Phase 5 — Visualisation (Streamlit)

Dashboard interactif affichant :

* Taux d’engagement
* Taux de complétion
* Activité horaire/journalière
* Cours les plus suivis
* Comparaison Kaggle vs Simulé vs YouTube

Lancement :

```
streamlit run dashboard/app.py
```

---

# ▶️ Installation

### 1. Cloner le projet

```
git clone https://github.com/KanteKadiatou/big-data-final-project.git
cd BIG-DATA-FINAL-PROJECT
```

### 2. Installer les dépendances

```
pip install -r requirements.txt
```

### 3. Lancer MinIO (facultatif mais recommandé)

```
cd infrastructure/minio
docker-compose up -d
```

### 4. Lancer les notebooks

Ouvrir :

```
notebooks/exploration.ipynb
```

---

# 🛠️ Technologies utilisées

* Python 3.10+
* PySpark
* Apache Airflow
* Streamlit
* MinIO (S3 compatible)
* Pandas / NumPy
* YouTube Data API
* Faker
* Docker / Docker Compose

---

# 👤 Auteur

Projet réalisé par **[Kadiatou Kanté / Mouctar Diallo]** — 2025.
