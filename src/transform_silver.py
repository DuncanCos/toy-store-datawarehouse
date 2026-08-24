"""
Silver layer - cleaning & typing (generic).

Discovers every CSV present in the bronze bucket and transforms it, without
any hardcoded table list: drop a new file into data/raw/ and it flows all the
way to silver on the next run.

For each file:
  - if the table is declared in conf/tables.json, its schema and primary key
    are applied as written;
  - otherwise the schema is inferred by Spark and the first column is used as
    the primary key.
Then: literal "NULL" strings become real nulls, timestamp columns are cast,
exact duplicates are dropped, and the result is written as Parquet to the
silver bucket.

Runs on the real Spark cluster (spark://spark-master:7077).
"""
import json
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, StringType,
    DoubleType, TimestampType, DateType, BooleanType,
)

from spark_session import build_spark
from storage import list_bronze_csv

BRONZE_URI = os.environ.get("BRONZE_URI", "s3a://bronze/toy_store/raw")
SILVER_URI = os.environ.get("SILVER_URI", "s3a://silver/toy_store")
CONF_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf", "tables.json")

SPARK_TYPES = {
    "int": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "string": StringType(),
    "timestamp": TimestampType(),
    "date": DateType(),
    "boolean": BooleanType(),
}


def load_conf():
    with open(CONF_PATH, encoding="utf-8") as fh:
        conf = json.load(fh)
    return conf.get("defaults", {}), conf.get("tables", {})


def declared_schema(columns):
    """Build a Spark schema from the {column: type} mapping in the config."""
    return StructType([
        StructField(name, SPARK_TYPES[type_name], True)
        for name, type_name in columns.items()
    ])


def read_table(spark, src, table_conf, defaults):
    reader = (
        spark.read
        .option("header", True)
        # Source files spell missing values as the literal string "NULL";
        # without this they would survive as 4-character text.
        .option("nullValue", defaults.get("null_value", "NULL"))
    )
    columns = (table_conf or {}).get("columns")
    if columns:
        return reader.schema(declared_schema(columns)).csv(src), "declare"
    return reader.option("inferSchema", True).csv(src), "infere"


def main():
    defaults, tables_conf = load_conf()
    ts_format = defaults.get("timestamp_format", "yyyy-MM-dd HH:mm:ss")
    ts_columns = set(defaults.get("timestamp_columns", []))

    files = list_bronze_csv()
    if not files:
        raise SystemExit(f"Aucun CSV trouve dans {BRONZE_URI}")

    spark = build_spark("toystore-silver")

    for filename in files:
        name = os.path.splitext(filename)[0]
        table_conf = tables_conf.get(name)

        df, origin = read_table(spark, f"{BRONZE_URI}/{filename}", table_conf, defaults)

        # Cast declared timestamp columns that were read as text.
        for col in ts_columns & set(df.columns):
            if dict(df.dtypes)[col] == "string":
                df = df.withColumn(col, F.to_timestamp(col, ts_format))

        if defaults.get("drop_duplicates", True):
            df = df.dropDuplicates()

        # Declared primary key, else fall back to the first column.
        pk = (table_conf or {}).get("primary_key") or df.columns[0]
        if pk in df.columns:
            df = df.filter(F.col(pk).isNotNull())

        out_path = f"{SILVER_URI}/{name}"
        df.write.mode("overwrite").parquet(out_path)
        print(f"  -> silver/{name}: {df.count()} lignes, schema {origin}, pk={pk}")

    spark.stop()
    print(f"Silver transform done ({len(files)} tables).")


if __name__ == "__main__":
    main()
