# Toy Store E-Commerce - Data Pipeline

Pipeline de bout en bout (ingestion -> lac de donnees medaillon -> analyse Spark
-> dashboard) sur le jeu de donnees **Toy Store E-Commerce (Maven Fuzzy
Factory)**.

## Architecture

```
                 ┌────────────┐
   CSV bruts --> │  MinIO     │  bronze/  (donnees brutes, telles quelles)
   (volume)      │  (S3)      │
                 └────────────┘
                        │
                        ▼
   ┌───────────────────────────────────────────────┐
   │           Spark cluster (master + worker)      │
   │  spark://spark-master:7077                     │
   │                                                 │
   │  silver/  = CSV bruts nettoyes + types + dedup  │
   │             (Parquet, volume partage)           │
   │                                                 │
   │  gold/    = tables d'agregation repondant aux   │
   │             insights metier (Parquet + CSV)     │
   └───────────────────────────────────────────────┘
                        │
                        ▼
              dashboard HTML (output/index.html)
              servi sur http://localhost:8080
```

Services docker-compose :

| Service       | Role                                                      |
|---------------|------------------------------------------------------------|
| `minio`       | Stockage objet (S3-compatible), buckets `bronze/silver/gold` |
| `minio-init`  | Cree les buckets au demarrage                              |
| `spark-master`| Master Spark reel (`spark://spark-master:7077`)             |
| `spark-worker`| Worker Spark (2 cores / 2G)                                 |
| `pipeline`    | Ingestion, jobs Spark, generation du dashboard, puis le sert |

## Medaillon

1. **Bronze** (`ingest_bronze.py`) - copie les CSV bruts tels quels dans MinIO
   (`s3://bronze/toy_store/raw/`), avant toute transformation.
2. **Silver** (`transform_silver.py`, job Spark) - lit les CSV bruts, applique
   un schema explicite, type les dates, deduplique, ecrit en Parquet sur un
   volume partage (`silver/<table>`).
3. **Gold** (`build_gold.py`, job Spark) - agrege les tables silver pour
   repondre aux 4 questions metier (`insights.txt`) : tendance
   sessions/commandes, taux de conversion, performance des canaux marketing,
   evolution du panier moyen (AOV). Ecrit en Parquet + CSV.
4. **Dashboard** (`generate_dashboard.py`) - lit les tables gold (pandas),
   genere les graphiques (matplotlib) et produit un unique fichier HTML
   autonome (`output/index.html`) avec le texte de reponse a chaque insight.

## Lancer le pipeline

Une seule commande, aucune intervention manuelle entre le lancement et le
dashboard :

```bash
docker compose up --build
```

ou, via le Makefile (equivalent, avec un chemin de donnees personnalisable) :

```bash
make run DATA_PATH=/chemin/vers/les/csv
```

Par defaut `DATA_PATH=./data/raw` : placez-y les CSV bruts du dataset
(`orders.csv`, `order_items.csv`, `order_item_refunds.csv`, `products.csv`,
`website_sessions.csv`, `website_pageviews.csv`) avant de lancer la commande.
Ces fichiers ne sont pas versionnes (voir `.gitignore`).

A la fin du pipeline, le dashboard est servi sur **http://localhost:8080**.
La console MinIO est accessible sur **http://localhost:9001**
(`minioadmin` / `minioadmin`), l'UI Spark master sur **http://localhost:8081**.

Arreter :

```bash
docker compose down
# ou pour repartir de zero (efface les volumes minio/spark_data + output/):
make clean
```

## Pourquoi pas Airflow ?

Le pipeline est un enchainement sequentiel simple (bronze -> silver -> gold ->
dashboard), execute une seule fois par run, sans recurrence, DAG complexe,
retry/backfill. Un orchestrateur n'apporterait rien ici ; l'enchainement est
fait dans `docker/pipeline/run_pipeline.sh`.

## Structure du repo

```
docker-compose.yml
Makefile
docker/pipeline/        # image du service "pipeline" (spark-submit + python)
src/
  ingest_bronze.py       # bronze : copie brute vers MinIO
  transform_silver.py    # silver : nettoyage/typage (Spark)
  build_gold.py          # gold : agregations / insights (Spark)
  generate_dashboard.py  # dashboard HTML autonome
data/raw/                # CSV bruts (non versionnes)
output/                  # dashboard genere (non versionne)
```
