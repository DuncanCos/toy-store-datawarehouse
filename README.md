# Toy Store E-Commerce - Data Pipeline

Pipeline de bout en bout (ingestion -> lac de donnees medaillon -> analyse Spark
-> dashboard) sur le jeu de donnees **Toy Store E-Commerce (Maven Fuzzy
Factory)**.

## Architecture

Les trois couches du medaillon vivent dans le **stockage objet MinIO**. Apres
l'ingestion bronze, plus rien ne relit le disque local : Spark lit et ecrit
uniquement des chemins `s3a://`.

```
   CSV bruts (volume, lu une seule fois)
          │
          │  ingest_bronze.py (boto3)
          ▼
   ┌──────────────────────────────────────────────┐
   │                  MinIO (S3)                   │
   │                                                │
   │   s3a://bronze/  CSV bruts, tels quels         │
   │        ▲   │                                   │
   │        │   ▼                                   │
   │   s3a://silver/  Parquet nettoye + type        │
   │        ▲   │                                   │
   │        │   ▼                                   │
   │   s3a://gold/    agregats metier (Parquet+CSV) │
   └──────────────────────────────────────────────┘
            ▲   │           lecture / ecriture s3a
            │   ▼
   ┌──────────────────────────────────────────────┐
   │      Spark cluster (master + worker)          │
   │      spark://spark-master:7077                │
   └──────────────────────────────────────────────┘
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

Les trois services Spark partagent **une seule image** (`docker/spark/`) : les
executors du worker ont besoin des jars du connecteur S3A
(`hadoop-aws` + `aws-java-sdk-bundle`, versions alignees sur le Hadoop 3.3.4
embarque dans l'image Spark) exactement comme le driver.

## Pourquoi MinIO plutot que HDFS ?

Le sujet autorise explicitement « base de donnees **ou stockage objet** ».
Le stockage objet a ete retenu parce que :

- il correspond a l'architecture reelle du marche (lakehouse : S3 / ADLS / GCS),
  qui **separe le calcul du stockage** — le cluster Spark est jetable, les
  donnees survivent ;
- MinIO parle l'**API S3** : le meme code tourne contre AWS S3 en changeant
  seulement l'endpoint ;
- un seul conteneur, contre un namenode + des datanodes pour HDFS ;
- les atouts de HDFS (data locality, gros blocs sequentiels) sont sans effet
  ici : ~103 Mo de CSV sur une seule machine.

## Medaillon

1. **Bronze** (`ingest_bronze.py`) - copie les CSV bruts tels quels dans MinIO
   (`s3://bronze/toy_store/raw/`), avant toute transformation. C'est la seule
   etape qui touche le disque local.
2. **Silver** (`transform_silver.py`, job Spark) - lit les CSV **depuis le
   bronze** (`s3a://bronze/...`, source de verite une fois ingeree), applique
   un schema explicite, type les dates, deduplique, ecrit en Parquet dans
   `s3a://silver/toy_store/<table>`.
3. **Gold** (`build_gold.py`, job Spark) - agrege les tables silver pour
   repondre aux 4 questions metier (`insights.txt`) : tendance
   sessions/commandes, taux de conversion, performance des canaux marketing,
   evolution du panier moyen (AOV). Ecrit dans `s3a://gold/toy_store/` en
   Parquet (la table d'entrepot) plus une copie CSV plate a cote, pour que
   le dashboard puisse la lire sans relancer Spark.
4. **Dashboard** (`generate_dashboard.py`) - telecharge les CSV gold depuis
   MinIO (boto3 + pandas), genere les graphiques (matplotlib) et produit un
   unique fichier HTML autonome (`output/index.html`) avec le texte de
   reponse a chaque insight.

### Le rendu : un seul fichier, zero dependance

`output/index.html` est **entierement autonome** — il s'ouvre par un
double-clic, sans serveur et **sans connexion internet** :

- les 5 graphiques sont des PNG generes par matplotlib depuis la couche gold,
  embarques en base64 (aucun fichier image a cote) ;
- les 3 polices (Fredoka, Karla, IBM Plex Mono) sont stockees dans
  `src/fonts/` et inlinees en `data:font/woff2` ;
- aucune balise `<script>`, aucune requete reseau, aucun CDN.

Il se transmet donc tel quel (mail, zip, cle USB) en gardant un rendu identique.

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
# ou pour repartir de zero (efface le volume minio + output/):
make clean
```

## Pourquoi pas Airflow ?

Le pipeline est un enchainement sequentiel simple (bronze -> silver -> gold ->
dashboard), execute une seule fois par run, sans recurrence, DAG complexe,
retry/backfill. Un orchestrateur n'apporterait rien ici ; l'enchainement est
fait dans `docker/spark/run_pipeline.sh`.

## Structure du repo

```
docker-compose.yml
Makefile
docker/spark/            # image commune master / worker / pipeline (+ jars S3A)
src/
  spark_session.py       # fabrique de SparkSession (cluster + config S3A/MinIO)
  ingest_bronze.py       # bronze : copie brute vers MinIO
  transform_silver.py    # silver : nettoyage/typage (Spark, s3a -> s3a)
  build_gold.py          # gold : agregations / insights (Spark, s3a -> s3a)
  generate_dashboard.py  # dashboard HTML autonome (lit le gold depuis MinIO)
  fonts/                 # polices woff2 embarquees en base64 dans le HTML
data/raw/                # CSV bruts (non versionnes)
output/                  # dashboard genere (non versionne)
```
