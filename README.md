Pipeline Big Data complet pour l'analyse du marché des cryptomonnaies en utilisant CoinGecko API, MinIO, Snowflake, Apache Airflow et Tableau Desktop.

---

# 📌 Architecture

CoinGecko API
↓
Bronze (JSON - MinIO)
↓
Silver (Parquet - MinIO)
↓
Gold (Modèle Dimensionnel - Parquet)
↓
Snowflake Data Warehouse
↓
Tableau Dashboard

---

# 🏗️ Architecture Medallion

## 🥉 Bronze Layer
- Source : CoinGecko API
- Format : JSON
- Stockage : MinIO
- Objectif : conserver les données brutes.

Exemple :

crypto-bronze/YYYY/MM/DD/raw.json

---

## 🥈 Silver Layer
- Lecture des fichiers JSON depuis MinIO
- Nettoyage avec Pandas
- Normalisation des colonnes (snake_case)
- Format : Parquet

Exemple :

crypto-silver/crypto_silver.parquet

---

## 🥇 Gold Layer
Implémentation d'un modèle dimensionnel de type Star Schema.

### DIM_CRYPTO
| Colonne | Type |
|---------|------|
| CRYPTO_ID | STRING |
| NAME | STRING |
| SYMBOL | STRING |

### DIM_DATE
| Colonne | Type |
|---------|------|
| DATE_ID | NUMBER |
| FULL_DATE | DATE |
| YEAR | NUMBER |
| MONTH | NUMBER |
| DAY | NUMBER |

### FACT_MARKET
| Colonne | Type |
|---------|------|
| FACT_ID | NUMBER |
| CRYPTO_ID | STRING |
| DATE_ID | NUMBER |
| CURRENT_PRICE | FLOAT |
| HIGH_24H | FLOAT |
| LOW_24H | FLOAT |
| TOTAL_VOLUME | FLOAT |
| MARKET_CAP | FLOAT |
| PRICE_CHANGE_PERCENTAGE_24H | FLOAT |

---

# ❄️ Snowflake Data Warehouse

Chargement des tables :

- DIM_CRYPTO
- DIM_DATE
- FACT_MARKET

Relations :

FACT_MARKET.CRYPTO_ID → DIM_CRYPTO.CRYPTO_ID

FACT_MARKET.DATE_ID → DIM_DATE.DATE_ID

---

# 🔄 Orchestration Apache Airflow

DAG :

ingest_bronze
>>
transform_silver
>>
build_gold_model
>>
load_snowflake

Fonctionnalités :

- Scheduling quotidien
- Gestion des erreurs
- Retries automatiques
- Logs d'exécution

---

# 📊 Dashboard Tableau

## KPI
- Prix actuel
- Variation 24h
- Volume total
- Market Cap

## Visualisations
- Évolution du prix dans le temps
- Top 10 cryptos par volume
- Heatmap des variations journalières
- Corrélation volume vs variation de prix
- Dashboard détail par cryptomonnaie

## Filtres globaux
- Crypto
- Date
- Indicateur à afficher

---

# 📁 Structure du projet

```bash
project/
│
├── bronze/
│   └── ingest_bronze.py
│
├── silver/
│   └── transform_silver.py
│
├── gold/
│   └── build_gold.py
│
├── snowflake/
│   └── load_snowflake.py
│
├── airflow/
│   └── cryptopipeline_dag.py
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .env
```

---

# ⚙️ Technologies utilisées

- Python
- Pandas
- CoinGecko API
- MinIO
- Snowflake
- Apache Airflow
- Docker
- Tableau Desktop

---

# ▶️ Exécution du projet

## 1. Cloner le projet

```bash
git clone https://github.com/votre-username/pipeline-big-data-crypto.git
cd pipeline-big-data-crypto
```

## 2. Installer les dépendances

```bash
pip install -r requirements.txt
```

## 3. Lancer Docker

```bash
docker compose up -d
```

## 4. Exécuter le pipeline

```bash
python bronze/ingest_bronze.py
python silver/transform_silver.py
python gold/build_gold.py
python snowflake/load_snowflake.py
```

## 5. Lancer Airflow

```bash
http://localhost:8080
```

## 6. Ouvrir MinIO

```bash
http://localhost:9000
```

---

# 📷 Captures à ajouter

- Architecture du pipeline
- MinIO Bronze/Silver/Gold
- Graph View Airflow
- Tables Snowflake
- Dashboard Tableau Principal
- Dashboard Détail

---

# 👩‍💻 Auteur

Projet réalisé dans le cadre de la formation Data Analyst / Data Engineer.

# 📊 Dashboard
<img width="986" height="794" alt="image" src="https://github.com/user-attachments/assets/fe5ca089-aabc-4973-b4ee-479e283f8bb5" />

<img width="994" height="786" alt="image" src="https://github.com/user-attachments/assets/6322e797-fa1d-412f-9482-6f45085dc28f" />


