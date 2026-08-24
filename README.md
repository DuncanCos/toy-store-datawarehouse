# Toy Store E-Commerce - Data Pipeline

Pipeline de bout en bout (ingestion -> lac de donnees medaillon -> analyse Spark
-> dashboard) sur le jeu de donnees **Toy Store E-Commerce (Maven Fuzzy
Factory)**.

## Architecture

Deux systemes de stockage, chacun dans son role :

- **MinIO = le lac de donnees** (bronze / silver / gold en Parquet). Apres
  l'ingestion bronze, plus rien ne relit le disque local : Spark lit et ecrit
  uniquement des chemins `s3a://`.
- **PostgreSQL = l'entrepot**. Les couches silver et gold y sont chargees par
  Spark en JDBC ; le dashboard interroge ensuite l'entrepot en **SQL**.

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
   │   s3a://gold/    agregats metier (Parquet)      │
   └──────────────────────────────────────────────┘
            ▲   │           lecture / ecriture s3a
            │   ▼
   ┌──────────────────────────────────────────────┐
   │      Spark cluster (master + worker)          │
   │      spark://spark-master:7077                │
   └──────────────────────────────────────────────┘
                        │  JDBC (silver + gold)
                        ▼
   ┌──────────────────────────────────────────────┐
   │        PostgreSQL - l'entrepot                │
   │   silver.<table>  detail, analyse ad-hoc SQL  │
   │   gold.<table>    agregats du dashboard       │
   └──────────────────────────────────────────────┘
                        │  SQL
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
| `postgres`    | Entrepot PostgreSQL, schemas `silver` et `gold`             |
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
4. **Entrepot** (`load_warehouse.py`, job Spark) - decouvre les tables
   presentes dans le lac et charge chacune dans PostgreSQL en JDBC :
   `s3a://silver/<t>` vers `silver.<t>`, `s3a://gold/<t>` vers `gold.<t>`.
   Rien n'est code en dur : une nouvelle table apparue dans le lac est
   chargee au run suivant.
5. **Dashboard** (`generate_dashboard.py`) - interroge `gold.*` en **SQL**,
   genere les graphiques (matplotlib) et produit un unique fichier HTML
   autonome (`output/index.html`) avec le texte de reponse a chaque insight.

## Ajouter des donnees : rien a coder

Deposez un CSV dans `data/raw/` et relancez : il est ingere, transforme et
charge en base **sans modifier une ligne de code**.

- `ingest_bronze.py` scanne le repertoire a chaque run ;
- `transform_silver.py` parcourt le bucket bronze : si la table est declaree
  dans `src/conf/tables.json`, son schema et sa cle primaire sont appliques ;
  sinon **le schema est infere** et la premiere colonne sert de cle ;
- `load_warehouse.py` decouvre les tables du lac et les charge dans Postgres.

Declarer une table dans `src/conf/tables.json` est donc **optionnel** : cela
sert uniquement a forcer les types et la cle primaire. Les reglages communs
(valeur consideree comme nulle, colonnes horodatees, format de date) sont
dans le bloc `defaults`.

Verifie sur un fichier `marketing_spend.csv` non declare : ingere, type
automatiquement (`integer`, `timestamp`, `double precision`, `text`) et
charge dans `silver.marketing_spend`, sans toucher au code.

Seule la couche **gold** reste specifique au metier : elle repond aux 4
questions de `insights.txt` et se modifie dans `build_gold.py`.

> A noter : supprimer un CSV source ne supprime pas la table deja chargee
> (les ecritures se font en `overwrite`, pas en synchronisation). Pour
> repartir d'un etat propre : `make clean`.

## Interroger l'entrepot

```bash
docker exec -it toystore-postgres psql -U warehouse -d warehouse
```

```sql
SELECT device_type, count(*) AS sessions
FROM silver.website_sessions GROUP BY device_type ORDER BY sessions DESC;

SELECT * FROM gold.gold_channel_performance ORDER BY revenue DESC;
```

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
(`minioadmin` / `minioadmin`), l'UI Spark master sur **http://localhost:8081**,
et PostgreSQL sur **localhost:5432** (`warehouse` / `warehouse` / base
`warehouse`).

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
docker/spark/            # image commune master / worker / pipeline (jars S3A + JDBC)
docker/postgres/         # init.sql : creation des schemas silver / gold
src/
  spark_session.py       # fabrique de SparkSession (cluster + config S3A/MinIO)
  ingest_bronze.py       # bronze : copie brute vers MinIO
  transform_silver.py    # silver : nettoyage/typage (Spark, s3a -> s3a)
  build_gold.py          # gold : agregations / insights (Spark, s3a -> s3a)
  load_warehouse.py      # lac -> PostgreSQL en JDBC (generique)
  generate_dashboard.py  # dashboard HTML autonome (lit gold.* en SQL)
  storage.py             # helpers MinIO (decouverte des objets)
  warehouse.py           # helpers PostgreSQL (JDBC + psycopg2)
  conf/tables.json       # schemas optionnels ; inference si absent
  fonts/                 # polices woff2 embarquees en base64 dans le HTML
data/raw/                # CSV bruts (non versionnes)
output/                  # dashboard genere (non versionne)
```
