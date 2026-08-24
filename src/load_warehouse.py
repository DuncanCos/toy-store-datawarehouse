"""
Warehouse load - MinIO (lake) -> PostgreSQL (warehouse).

Discovers whatever tables exist in the silver and gold zones and loads each
one into the matching PostgreSQL schema over JDBC. Nothing is hardcoded: a
new table appearing in the lake is loaded on the next run.

  s3a://silver/toy_store/<table>  ->  silver.<table>   (detail, ad-hoc SQL)
  s3a://gold/toy_store/<table>    ->  gold.<table>     (aggregates, dashboard)

Runs on the real Spark cluster (spark://spark-master:7077).
"""
import os

from spark_session import build_spark
from storage import list_tables
from warehouse import jdbc_properties, jdbc_url

SILVER_URI = os.environ.get("SILVER_URI", "s3a://silver/toy_store")
GOLD_URI = os.environ.get("GOLD_URI", "s3a://gold/toy_store")

# Write in chunks so a large table does not build one giant transaction.
BATCH_SIZE = os.environ.get("PG_BATCH_SIZE", "10000")


def load_layer(spark, layer_uri, schema):
    tables = list_tables(layer_uri)
    if not tables:
        print(f"  ({schema}: aucune table trouvee dans {layer_uri})")
        return 0

    props = jdbc_properties()
    props["batchsize"] = BATCH_SIZE

    for name in tables:
        df = spark.read.parquet(f"{layer_uri}/{name}")
        (
            df.write
            .mode("overwrite")
            .option("truncate", "false")
            .jdbc(url=jdbc_url(), table=f"{schema}.{name}", properties=props)
        )
        print(f"  -> {schema}.{name}: {df.count()} lignes chargees")
    return len(tables)


def main():
    spark = build_spark("toystore-warehouse-load")

    total = 0
    total += load_layer(spark, SILVER_URI, "silver")
    total += load_layer(spark, GOLD_URI, "gold")

    spark.stop()
    print(f"Warehouse load done ({total} tables dans PostgreSQL).")


if __name__ == "__main__":
    main()
