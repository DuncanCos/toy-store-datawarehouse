"""
Silver layer - cleaning & typing.

Reads the raw CSV files straight from the bronze bucket in MinIO (not from
the local filesystem: bronze is the single source of truth once ingested),
applies explicit schemas, casts timestamps, drops exact duplicates and rows
missing a primary key, then writes Parquet tables to the silver bucket.
Runs on the real Spark cluster (spark://spark-master:7077).
"""
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

from spark_session import build_spark

BRONZE_URI = os.environ.get("BRONZE_URI", "s3a://bronze/toy_store/raw")
SILVER_URI = os.environ.get("SILVER_URI", "s3a://silver/toy_store")

TS_FMT = "yyyy-MM-dd HH:mm:ss"

TABLES = {
    "orders": {
        "schema": StructType([
            StructField("order_id", IntegerType()),
            StructField("created_at", StringType()),
            StructField("website_session_id", IntegerType()),
            StructField("user_id", IntegerType()),
            StructField("primary_product_id", IntegerType()),
            StructField("items_purchased", IntegerType()),
            StructField("price_usd", DoubleType()),
            StructField("cogs_usd", DoubleType()),
        ]),
        "pk": "order_id",
    },
    "order_items": {
        "schema": StructType([
            StructField("order_item_id", IntegerType()),
            StructField("created_at", StringType()),
            StructField("order_id", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("is_primary_item", IntegerType()),
            StructField("price_usd", DoubleType()),
            StructField("cogs_usd", DoubleType()),
        ]),
        "pk": "order_item_id",
    },
    "order_item_refunds": {
        "schema": StructType([
            StructField("order_item_refund_id", IntegerType()),
            StructField("created_at", StringType()),
            StructField("order_item_id", IntegerType()),
            StructField("order_id", IntegerType()),
            StructField("refund_amount_usd", DoubleType()),
        ]),
        "pk": "order_item_refund_id",
    },
    "products": {
        "schema": StructType([
            StructField("product_id", IntegerType()),
            StructField("created_at", StringType()),
            StructField("product_name", StringType()),
        ]),
        "pk": "product_id",
    },
    "website_sessions": {
        "schema": StructType([
            StructField("website_session_id", IntegerType()),
            StructField("created_at", StringType()),
            StructField("user_id", IntegerType()),
            StructField("is_repeat_session", IntegerType()),
            StructField("utm_source", StringType()),
            StructField("utm_campaign", StringType()),
            StructField("utm_content", StringType()),
            StructField("device_type", StringType()),
            StructField("http_referer", StringType()),
        ]),
        "pk": "website_session_id",
    },
    "website_pageviews": {
        "schema": StructType([
            StructField("website_pageview_id", IntegerType()),
            StructField("created_at", StringType()),
            StructField("website_session_id", IntegerType()),
            StructField("pageview_url", StringType()),
        ]),
        "pk": "website_pageview_id",
    },
}


def main():
    spark = build_spark("toystore-silver")

    for name, cfg in TABLES.items():
        src = f"{BRONZE_URI}/{name}.csv"
        df = (
            spark.read
            .option("header", True)
            .schema(cfg["schema"])
            .csv(src)
        )

        df = df.withColumn("created_at", F.to_timestamp("created_at", TS_FMT))
        df = df.dropDuplicates()
        df = df.filter(F.col(cfg["pk"]).isNotNull())

        out_path = f"{SILVER_URI}/{name}"
        df.write.mode("overwrite").parquet(out_path)

        count = df.count()
        print(f"  -> silver/{name}: {count} rows written to {out_path}")

    spark.stop()
    print("Silver transform done.")


if __name__ == "__main__":
    main()
