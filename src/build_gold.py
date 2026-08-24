"""
Gold layer - business insights.

Reads the silver tables and computes the aggregated tables that answer
the four business questions from insights.txt:

  1. Sessions & orders volume trend
  2. Session -> order conversion rate trend
  3. Marketing channel performance
  4. Average order value (revenue per order) trend

Each gold table is written as Parquet (data warehouse) and as a single
CSV file (consumed by the dashboard generator).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_DIR = os.environ.get("SILVER_DIR", "/opt/spark-data/processed/silver")
GOLD_DIR = os.environ.get("GOLD_DIR", "/opt/spark-data/processed/gold")
MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")


def save(df, name):
    parquet_path = os.path.join(GOLD_DIR, name)
    df.write.mode("overwrite").parquet(parquet_path)

    pdf = df.toPandas()
    os.makedirs(GOLD_DIR, exist_ok=True)
    csv_path = os.path.join(GOLD_DIR, f"{name}.csv")
    pdf.to_csv(csv_path, index=False)
    print(f"  -> gold/{name}: {len(pdf)} rows -> {csv_path}")
    return pdf


def main():
    spark = (
        SparkSession.builder
        .appName("toystore-gold")
        .master(MASTER_URL)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sessions = spark.read.parquet(os.path.join(SILVER_DIR, "website_sessions"))
    orders = spark.read.parquet(os.path.join(SILVER_DIR, "orders"))

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
    # ---------------------------------------------------------------
    sessions_ch = sessions.withColumn(
        "channel",
        F.when(
            F.col("utm_source").isNotNull(),
            F.concat_ws(" / ", F.col("utm_source"), F.coalesce(F.col("utm_campaign"), F.lit("unknown"))),
        ).when(
            F.col("http_referer").isNull(), F.lit("direct (type-in)")
        ).otherwise(
            F.concat(
                F.lit("organic: "),
                F.regexp_extract(F.col("http_referer"), r"https?://(?:www\.)?([^/]+)", 1),
            )
        ),
    )

    channel_sessions = sessions_ch.groupBy("channel").agg(F.count("*").alias("sessions"))

    channel_orders = (
        orders.join(
            sessions_ch.select("website_session_id", "channel"),
            on="website_session_id",
            how="left",
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
        .orderBy(F.col("revenue").desc())
    )
    save(channel_perf, "gold_channel_performance")

    spark.stop()
    print("Gold aggregation done.")


if __name__ == "__main__":
    main()
