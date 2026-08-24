"""
Bronze layer - ingestion.

Copies every raw CSV file exactly as-is (no parsing, no typing, no cleaning)
into the object storage (MinIO/S3) bronze bucket, before any transformation
happens. This is the "persist the raw data as-is" step of the medallion
architecture.
"""
import os
import sys

import boto3
from botocore.client import Config

RAW_DIR = os.environ.get("RAW_DIR", "/opt/spark-data/raw")
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BUCKET = "bronze"
PREFIX = "toy_store/raw"


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    csv_files = sorted(f for f in os.listdir(RAW_DIR) if f.lower().endswith(".csv"))
    if not csv_files:
        print(f"No CSV files found in {RAW_DIR}", file=sys.stderr)
        sys.exit(1)

    for filename in csv_files:
        local_path = os.path.join(RAW_DIR, filename)
        key = f"{PREFIX}/{filename}"
        size = os.path.getsize(local_path)
        print(f"  -> uploading {filename} ({size / 1_000_000:.2f} MB) to s3://{BUCKET}/{key}")
        s3.upload_file(local_path, BUCKET, key)

    print(f"Bronze ingestion done: {len(csv_files)} files persisted as-is in s3://{BUCKET}/{PREFIX}/")


if __name__ == "__main__":
    main()
