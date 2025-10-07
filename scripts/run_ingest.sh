#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR_DIR="$REPO_ROOT/jars"
DATA_FILE="$REPO_ROOT/data/raw/covid19.csv"

if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker-compose)
else
  echo "error: docker compose plugin or docker-compose binary is required." >&2
  exit 1
fi

if [[ ! -f "$DATA_FILE" ]]; then
  echo "error: $DATA_FILE not found. Download the dataset before running ingestion." >&2
  exit 1
fi

shopt -s nullglob
jar_paths=("$JAR_DIR"/*.jar)
shopt -u nullglob

IFS=$'\n' jar_paths=($(printf '%s\n' "${jar_paths[@]}" | sort))

if [[ ${#jar_paths[@]} -eq 0 ]]; then
  cat >&2 <<'EOF'
error: no supporting JARs found in the ./jars directory.
Run ./scripts/bootstrap_jars.sh to download the required dependencies first.
EOF
  exit 1
fi

CONTAINER_JAR_DIR="/opt/bitnami/spark/extra-jars"

comma_joined=""
colon_joined=""
for jar_path in "${jar_paths[@]}"; do
  base_name="$(basename "$jar_path")"
  comma_joined+="${CONTAINER_JAR_DIR}/${base_name},"
  colon_joined+="${CONTAINER_JAR_DIR}/${base_name}:"
done
comma_joined=${comma_joined%,}
colon_joined=${colon_joined%:}

"${DOCKER_COMPOSE[@]}" run --rm \
  -e COVID19_RAW_PATH=/data/raw/covid19.csv \
  -e HUDI_OUTPUT_PATH=s3a://hudi-datasets/covid19 \
  spark \
  /opt/bitnami/spark/bin/spark-submit \
    --master local[2] \
    --jars "$comma_joined" \
    --conf spark.driver.extraClassPath="$colon_joined" \
    --conf spark.executor.extraClassPath="$colon_joined" \
    --conf spark.sql.hive.convertMetastoreParquet=false \
    --conf spark.sql.catalogImplementation=hive \
    ingest/spark_hudi_ingest.py
