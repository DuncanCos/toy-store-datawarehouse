#!/bin/bash
set -euo pipefail

echo "=============================================="
echo " Toy Store E-Commerce - Data Pipeline"
echo "=============================================="

echo "[wait] spark-master REST API..."
until curl -sf "http://spark-master:8080" > /dev/null; do
  sleep 2
done
echo "[wait] spark-master is up."

echo ""
echo "----- Step 1/4: Bronze ingestion (raw -> object storage) -----"
python3 /app/src/ingest_bronze.py

echo ""
echo "----- Step 2/4: Silver transform (Spark cluster) -----"
spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --conf spark.driver.host=pipeline \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.shuffle.partitions=8 \
  /app/src/transform_silver.py

echo ""
echo "----- Step 3/4: Gold aggregation - insights (Spark cluster) -----"
spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --conf spark.driver.host=pipeline \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.shuffle.partitions=8 \
  /app/src/build_gold.py

echo ""
echo "----- Step 4/4: Dashboard generation -----"
python3 /app/src/generate_dashboard.py

echo ""
echo "=============================================="
echo " Pipeline done. Dashboard: http://localhost:8080"
echo "=============================================="

cd "${OUTPUT_DIR}"
exec python3 -m http.server 8080
