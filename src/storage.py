"""
Shared MinIO (S3) helpers.

Keeps object-store discovery in one place so every step can ask "what is
actually in the lake?" instead of relying on a hardcoded list of tables.
"""
import os

import boto3
from botocore.client import Config


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _split_uri(uri):
    """s3a://bucket/some/prefix -> ('bucket', 'some/prefix')"""
    without_scheme = uri.split("://", 1)[1]
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix


def list_bronze_csv():
    """Every CSV object sitting in the bronze zone, as bare file names."""
    bucket, prefix = _split_uri(os.environ.get("BRONZE_URI", "s3a://bronze/toy_store/raw"))
    paginator = s3_client().get_paginator("list_objects_v2")
    names = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.lower().endswith(".csv"):
                names.append(name)
    return sorted(names)


def list_tables(uri):
    """
    Table names stored under a layer URI.

    Spark writes each table as a directory of part files, so the tables are
    the common prefixes one level below the layer prefix.
    """
    bucket, prefix = _split_uri(uri)
    prefix = prefix.rstrip("/") + "/"
    resp = s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    names = [
        cp["Prefix"][len(prefix):].strip("/")
        for cp in resp.get("CommonPrefixes", [])
    ]
    return sorted(n for n in names if n)
