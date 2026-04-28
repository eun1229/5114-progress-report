# 5114 Final Project

**acquisition folder** 
- contains code for AWS Lambda that collects real time updates and static schedule updates. Triggered using Eventbridge rules.


**spark folder** 
- spark_load_rt.py decodes realtime protobuf data from S3 and writes them to raw tables in Snowflake. 
- spark_load_static.py writes static schedule updates from S3 to dimension tables in Snowflake, if an update is available.
- Configurations and packages are pinned for Spark 3.5.0.

**airflow folder**
- DAG orchestration is configured for Apache Airflow 3.0.6.

**sql folder** 
- contains SQL ran in Snowflake for creating all tables and deriving tables from one layer to the next (raw to fact, fact/static to mart)

**mbta-dashboard folder**
- contains code for the Streamlit dashboard for visualizing the metrics

## Runtime versions

- Apache Airflow: 3.0.6
- PySpark: 3.5.0

## Local environment variables (.env)

- Spark loaders now read credentials and secure settings from environment variables using `python-dotenv`.
- Copy `.env.example` to `.env` and fill in your values before running Spark or Airflow.
- Set `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` in `.env`; this is the required Snowflake connection source for Airflow SQL tasks (instead of creating the connection in Airflow UI).
- `.env` should stay untracked (already covered by `.gitignore`).

Install local dependencies in your environment:

```bash
AIRFLOW_VERSION=3.0.6
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install -r requirements-airflow.txt --constraint "${CONSTRAINT_URL}"
pip install -r requirements-spark.txt
```

Comprehensive local Airflow setup and execution guide:

- [LOCAL_AIRFLOW_AUTOMATION_GUIDE.md](LOCAL_AIRFLOW_AUTOMATION_GUIDE.md)
