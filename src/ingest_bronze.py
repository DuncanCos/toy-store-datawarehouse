"""
Bronze layer - ingestion.

Copies every raw CSV found in RAW_DIR exactly as-is (no parsing, no typing,
no cleaning) into the bronze zone of the object store, before any
transformation happens.

Nothing is hardcoded: the directory is scanned at each run, so dropping a new
CSV into data/raw/ is enough for it to be ingested, then picked up by the
silver step and loaded into the warehouse.
"""
import os
import sys

from storage import s3_client, _split_uri

RAW_DIR = os.environ.get("RAW_DIR", "/opt/spark-data/raw")
BRONZE_URI = os.environ.get("BRONZE_URI", "s3a://bronze/toy_store/raw")


def main():
    bucket, prefix = _split_uri(BRONZE_URI)
    prefix = prefix.rstrip("/")

    if not os.path.isdir(RAW_DIR):
        print(f"Repertoire introuvable : {RAW_DIR}", file=sys.stderr)
        sys.exit(1)

    csv_files = sorted(f for f in os.listdir(RAW_DIR) if f.lower().endswith(".csv"))
    if not csv_files:
        print(f"Aucun CSV trouve dans {RAW_DIR}", file=sys.stderr)
        sys.exit(1)

    s3 = s3_client()
    for filename in csv_files:
        local_path = os.path.join(RAW_DIR, filename)
        key = f"{prefix}/{filename}"
        size = os.path.getsize(local_path)
        print(f"  -> {filename} ({size / 1_000_000:.2f} Mo) vers s3://{bucket}/{key}")
        s3.upload_file(local_path, bucket, key)

    print(f"Bronze ingestion done: {len(csv_files)} fichiers persistes tels quels.")


if __name__ == "__main__":
    main()
