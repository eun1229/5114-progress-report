# Local Airflow Automation Guide

This guide covers a full local setup and execution flow for the DAG in this repository, including verification and cleanup.

## 1. What this automation runs

The DAG `mbta_daily_etl_pipeline` runs:

1. Snowflake table creation SQL.
2. Spark static loader.
3. Spark realtime loaders (vehicle positions and alerts).
4. Snowflake raw-to-fact SQL.
5. Snowflake mart SQL.

## 2. Isolated local setup (recommended)

Run from project root:

```bash
cd /Users/duyhn/Downloads/5114-final-project
```

Create and activate virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install Python dependencies:

```bash
pip install --upgrade pip
AIRFLOW_VERSION=3.0.6
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install -r requirements-airflow.txt --constraint "${CONSTRAINT_URL}"
pip install -r requirements-spark.txt

airflow version
python -c "import pyspark; print(pyspark.__version__)"
```

## 3. Configure environment variables

Copy template and edit:

```bash
cp .env.example .env
```

Set at least:

1. `PROJECT_DIR`
2. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
3. `SNOWFLAKE_URL`, `SNOWFLAKE_USER`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`
4. `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_PRIVATE_KEY_PASSWORD`

Load env vars into current shell:

```bash
set -a
source .env
set +a
```

## 4. Required local files

Ensure these files exist:

1. `spark/gtfs-realtime.desc`
2. Snowflake private key file pointed by `SNOWFLAKE_PRIVATE_KEY_PATH`

If descriptor is missing:

```bash
cd spark
curl -L -o gtfs-realtime.proto https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto
protoc --descriptor_set_out=gtfs-realtime.desc --include_imports gtfs-realtime.proto
cd ..
```

## 5. Airflow initialization

Because `.env` sets `AIRFLOW_HOME` and `AIRFLOW__CORE__DAGS_FOLDER`, this stays local to the repo.

Initialize DB:

```bash
airflow db migrate
```

Serialize DAG metadata (required on first setup before `airflow dags list`):

```bash
airflow dags reserialize
```

Airflow 3 uses `SimpleAuthManager` by default, so `airflow users create` is not available.

Default users are controlled by `core.simple_auth_manager_users` (default `admin:admin`).
On first API server startup, a password file is generated at:

```bash
${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated
```

If needed, reset generated passwords and restart the API server:

```bash
rm -f "${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated"
```

## 6. Configure Snowflake Airflow connection

The DAG uses `conn_id="snowflake_default"` for SQL tasks.

Set this environment variable in `.env` (required):

```bash
AIRFLOW_CONN_SNOWFLAKE_DEFAULT='snowflake://USER:PASSWORD@/FINAL_PROJECT_RAW?account=ACCOUNT&database=DB&warehouse=WH'
```

Note: keep this value quoted because `.env` is sourced by bash and the URI contains `&`.

If your Snowflake setup requires a non-default role, append `&role=YOUR_ROLE`.

After loading `.env`, verify it is set:

```bash
echo "$AIRFLOW_CONN_SNOWFLAKE_DEFAULT"
```

## 7. Quick preflight checks

List DAGs:

```bash
airflow dags list | grep mbta_daily_etl_pipeline
```

Spark helper checks:

```bash
bash spark/run_spark_load_static.sh
bash spark/run_spark_load_rt.sh 2026-03-10 vehicle_positions
bash spark/run_spark_load_rt.sh 2026-03-10 alerts
```

Notes:

1. Static loader supports two modes:
- No date: loads latest available static folder (`v_YYYYMMDD_HHMMSS`).
- With date (`YYYYMMDD` or `YYYY-MM-DD`): loads that specific version date.
2. Realtime loader requires a date.

## 8. Run Airflow services

Terminal A:

```bash
source .venv/bin/activate
set -a
source .env
set +a
airflow api-server --port 8080
```

Terminal B:

```bash
source .venv/bin/activate
set -a
source .env
set +a
airflow scheduler
```

Terminal C:

```bash
source .venv/bin/activate
set -a
source .env
set +a
airflow dag-processor
```

Terminal D (recommended for full Airflow 3 local stack):

```bash
source .venv/bin/activate
set -a
source .env
set +a
airflow triggerer
```

Open UI at `http://localhost:8080`.

## 9. Trigger automation

Option A: Single-date test from CLI:

```bash
source .venv/bin/activate
set -a
source .env
set +a
airflow dags test mbta_daily_etl_pipeline 2026-03-10
```

Option B: UI trigger:

1. Unpause `mbta_daily_etl_pipeline`.
2. Trigger DAG.

Option C: Backfill range:

```bash
source .venv/bin/activate
set -a
source .env
set +a
airflow backfill create \
  --dag-id mbta_daily_etl_pipeline \
  --from-date 2026-03-08 \
  --to-date 2026-03-10
```

## 10. Verify outputs

Check task logs in Airflow UI.

Then validate Snowflake table changes for the run date in:

1. RAW schema tables.
2. FACT tables.
3. MART tables.

## 11. Common failure points

1. `TemplateNotFound` for SQL:
- Ensure DAG `template_searchpath` points to `${PROJECT_DIR}/sql`.
- Ensure `.env` has correct `PROJECT_DIR`.

2. Spark cannot access S3:
- Verify AWS credentials in `.env`.
- Verify bucket paths in `S3_RT_PATH_PREFIX` and `S3_STATIC_PATH_PREFIX`.

3. Snowflake key errors:
- Verify `SNOWFLAKE_PRIVATE_KEY_PATH` and `SNOWFLAKE_PRIVATE_KEY_PASSWORD`.

4. Realtime ignores date/feed:
- Ensure you are running the updated script via DAG or helper script.

5. `MFA authentication is required` in Airflow SQL tasks:
- Use key-pair auth in `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` (include `private_key_file=` in the URI query params).
- Avoid password auth for Airflow connection if your account enforces MFA for programmatic access.

6. `Schema ... does not exist or not authorized`:
- Ensure your SQL does not hardcode a different database than your `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` database.
- Ensure schemas exist before table creation (the DAG now runs `ensure_schemas` before fact/mart DDL).

## 12. Cleanup and return to base state

Stop services:

```bash
pkill -f "airflow api-server" || true
pkill -f "airflow scheduler" || true
pkill -f "airflow dag-processor" || true
pkill -f "airflow triggerer" || true
pkill -f "spark-submit" || true
```

Remove local runtime state (safe, local only):

```bash
rm -rf .airflow
rm -rf .venv
```

Unset shell vars in current terminal:

```bash
unset PROJECT_DIR AIRFLOW_HOME AIRFLOW__CORE__DAGS_FOLDER AIRFLOW__CORE__LOAD_EXAMPLES
unset AIRFLOW_CONN_SNOWFLAKE_DEFAULT
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
unset S3_RT_PATH_PREFIX S3_STATIC_PATH_PREFIX REALTIME_SPEC_PATH
unset SNOWFLAKE_URL SNOWFLAKE_USER SNOWFLAKE_DATABASE SNOWFLAKE_WAREHOUSE
unset SNOWFLAKE_RAW_SCHEMA SNOWFLAKE_STATIC_SCHEMA SNOWFLAKE_ROLE
unset SNOWFLAKE_PRIVATE_KEY_PATH SNOWFLAKE_PRIVATE_KEY_PASSWORD
unset SERVICE_DATE RT_FEED_TYPE SPARK_PACKAGES
```

## 13. Security checklist

1. Keep `.env` untracked.
2. Never commit private key files.
3. Rotate any credentials that were ever committed.
4. Prefer short-lived credentials or IAM roles when possible.
