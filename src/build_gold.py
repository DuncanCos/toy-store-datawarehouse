"""
Gold layer - business insights.

Reads the silver tables and computes the aggregated tables that answer
the four business questions from insights.txt:

  1. Sessions & orders volume trend
  2. Session -> order conversion rate trend
  3. Marketing channel performance
  4. Average order value (revenue per order) trend

Each gold table is written to the gold bucket in MinIO as Parquet (the
data warehouse itself) and as a single flat CSV object (a convenience copy
consumed by the dashboard generator, which does not need Spark to read it).
"""
import io
import os

import boto3
from botocore.client import Config
from pyspark.sql import functions as F

from spark_session import build_spark

SILVER_URI = os.environ.get("SILVER_URI", "s3a://silver/toy_store")
GOLD_URI = os.environ.get("GOLD_URI", "s3a://gold/toy_store")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
GOLD_CSV_PREFIX = os.environ.get("GOLD_CSV_PREFIX", "toy_store/csv")


# Raw utm values are internal codenames; these make the dashboard readable.
# Anything not listed falls back to the raw value rather than being hidden.
SOURCE_LABELS = {
    "gsearch": "Google",
    "bsearch": "Bing",
    "socialbook": "Socialbook",
}
CAMPAIGN_LABELS = {
    "nonbrand": "acquisition",
    "brand": "marque",
    "desktop_targeted": "ciblage desktop",
    "pilot": "pilote",
}


def label_of(column, mapping):
    """Map known codenames to readable labels, keeping the raw value otherwise."""
    col = F.col(column)
    expr = None
    for code, label in mapping.items():
        cond = col == code
        expr = F.when(cond, F.lit(label)) if expr is None else expr.when(cond, F.lit(label))
    return expr.otherwise(col)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def save(df, name):
    # The warehouse table itself, partition files in the gold bucket.
    df.write.mode("overwrite").parquet(f"{GOLD_URI}/{name}")

    # A single flat CSV object next to it, so the dashboard can read the
    # aggregate with plain pandas instead of spinning up Spark.
    pdf = df.toPandas()
    buf = io.StringIO()
    pdf.to_csv(buf, index=False)
    key = f"{GOLD_CSV_PREFIX}/{name}.csv"
    s3_client().put_object(
        Bucket=GOLD_BUCKET, Key=key, Body=buf.getvalue().encode("utf-8")
    )
    print(f"  -> gold/{name}: {len(pdf)} rows -> {GOLD_URI}/{name} (+ s3://{GOLD_BUCKET}/{key})")
    return pdf


def main():
    spark = build_spark("toystore-gold")

    sessions = spark.read.parquet(f"{SILVER_URI}/website_sessions")
    orders = spark.read.parquet(f"{SILVER_URI}/orders")

    # ---------------------------------------------------------------
    # Insight 1 & 2 & 4 : monthly trend of sessions, orders, revenue,
    # conversion rate and average order value (AOV)
    # ---------------------------------------------------------------
    monthly_sessions = (
        sessions
        .withColumn("year_month", F.date_format("created_at", "yyyy-MM"))
        .groupBy("year_month")
        .agg(F.count("*").alias("sessions"))
    )

    monthly_orders = (
        orders
        .withColumn("year_month", F.date_format("created_at", "yyyy-MM"))
        .groupBy("year_month")
        .agg(
            F.count("*").alias("orders"),
            F.sum("price_usd").alias("revenue"),
        )
    )

    monthly = (
        monthly_sessions
        .join(monthly_orders, on="year_month", how="left")
        .fillna({"orders": 0, "revenue": 0.0})
        .withColumn(
            "conversion_rate_pct",
            F.round(F.col("orders") / F.col("sessions") * 100, 2),
        )
        .withColumn(
            "aov_usd",
            F.round(F.col("revenue") / F.when(F.col("orders") > 0, F.col("orders")), 2),
        )
        .orderBy("year_month")
    )
    save(monthly, "gold_monthly_trend")

    # ---------------------------------------------------------------
    # Insight 3 : marketing channel performance
    #
    # A session belongs to exactly one of three acquisition types:
    #   - utm_source set                    -> paid campaign
    #   - no utm_source but a referrer      -> organic search
    #   - no utm_source and no referrer     -> direct (typed the URL)
    # Grouping on the raw utm columns would lump the last two together,
    # so the type is derived explicitly and the label is made readable.
    # ---------------------------------------------------------------
    sessions_ch = (
        sessions
        .withColumn(
            "channel_type",
            F.when(F.col("utm_source").isNotNull(), F.lit("Payant"))
             .when(F.col("http_referer").isNotNull(), F.lit("Naturel"))
             .otherwise(F.lit("Direct")),
        )
        .withColumn(
            "referer_source",
            F.regexp_extract(F.col("http_referer"), r"https?://(?:www\.)?([^/.]+)", 1),
        )
        .withColumn(
            "channel",
            F.when(
                F.col("channel_type") == "Payant",
                F.concat(
                    label_of("utm_source", SOURCE_LABELS),
                    F.lit(" Ads · "),
                    label_of("utm_campaign", CAMPAIGN_LABELS),
                ),
            ).when(
                F.col("channel_type") == "Naturel",
                F.concat(label_of("referer_source", SOURCE_LABELS), F.lit(" · naturel")),
            ).otherwise(F.lit("Accès direct")),
        )
    )

    channel_sessions = (
        sessions_ch
        .groupBy("channel", "channel_type")
        .agg(F.count("*").alias("sessions"))
    )

    channel_orders = (
        orders.join(
            sessions_ch.select("website_session_id", "channel"),
            on="website_session_id",
            how="inner",
        )
        .groupBy("channel")
        .agg(F.count("*").alias("orders"), F.sum("price_usd").alias("revenue"))
    )

    channel_perf = (
        channel_sessions
        .join(channel_orders, on="channel", how="left")
        .fillna({"orders": 0, "revenue": 0.0})
        .withColumn("conversion_rate_pct", F.round(F.col("orders") / F.col("sessions") * 100, 2))
        .withColumn("revenue_per_session_usd", F.round(F.col("revenue") / F.col("sessions"), 2))
        .select(
            "channel", "channel_type", "sessions", "orders",
            "conversion_rate_pct", "revenue", "revenue_per_session_usd",
        )
        .orderBy(F.col("revenue").desc())
    )
    save(channel_perf, "gold_channel_performance")

    spark.stop()
    print("Gold aggregation done.")


if __name__ == "__main__":
    main()
