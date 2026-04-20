#!/bin/bash
set -euo pipefail

# contains the correct packages for decoding protobuf files and connecting to AWS S3 and Snowflake for Spark 3.5.0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${PROJECT_DIR}/.env" ]; then
  set -a
  source "${PROJECT_DIR}/.env"
  set +a
fi

SERVICE_DATE="${1:-${SERVICE_DATE:-}}"
FEED_TYPE="${2:-${RT_FEED_TYPE:-vehicle_positions}}"

if [ -z "${SERVICE_DATE}" ]; then
  echo "Usage: ./run_spark_load_rt.sh <service_date> [feed_type]"
  echo "Example: ./run_spark_load_rt.sh 2026-03-10 vehicle_positions"
  exit 1
fi

spark-submit \
  --packages "${SPARK_PACKAGES:-org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,org.apache.spark:spark-protobuf_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,net.snowflake:spark-snowflake_2.12:3.0.0,net.snowflake:snowflake-jdbc:3.13.30}" \
  "${SCRIPT_DIR}/spark_load_rt.py" --date "${SERVICE_DATE}" --feed-type "${FEED_TYPE}"