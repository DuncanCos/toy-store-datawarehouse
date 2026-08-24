"""
Shared Spark session factory.

Builds a SparkSession connected to the real Spark cluster and configured
to read/write s3a:// paths on MinIO, so every medallion layer
(bronze / silver / gold) lives in the object storage.
"""
import os

from pyspark.sql import SparkSession

MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")


def build_spark(app_name):
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(MASTER_URL)
        # --- S3A / MinIO ------------------------------------------------
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        # MinIO serves buckets as a path (http://host/bucket), not as a
        # virtual host (http://bucket.host), so path-style access is required.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Commit directly into place instead of renaming a _temporary dir,
        # which an object store cannot do cheaply.
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
