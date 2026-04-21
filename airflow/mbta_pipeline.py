"""
Airflow DAG for the MBTA daily ETL pipeline.
Runs Spark ingestion for static and realtime feeds, then builds Snowflake fact and mart tables.
"""

from datetime import datetime, timedelta
import os
import shlex
from pathlib import Path
from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "cse5114",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_DIR = os.environ.get("PROJECT_DIR", str(Path(__file__).resolve().parents[1]))
SQL_TEMPLATE_SEARCHPATH = f"{PROJECT_DIR}/sql"
ENV_FILE = f"{PROJECT_DIR}/.env"
SPARK_PACKAGES = os.environ.get(
    "SPARK_PACKAGES",
    (
        "org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,"
        "org.apache.spark:spark-protobuf_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "net.snowflake:spark-snowflake_2.12:3.0.0,"
        "net.snowflake:snowflake-jdbc:3.13.30"
    ),
)


def bash_with_env(command: str) -> str:
    env_file = shlex.quote(ENV_FILE)
    return f"set -a; [ -f {env_file} ] && source {env_file}; set +a; {command}"


@dag(
    dag_id="mbta_daily_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for MBTA performance metrics",
    schedule="@daily",
    start_date=datetime(2026, 3, 2),  # Start date determines where the backfill begins
    catchup=True,  # Set to True to enable automatic backfilling
    max_active_runs=1,  # Throttles the backfill so you don't overload your VM/Spark cluster
    template_searchpath=[SQL_TEMPLATE_SEARCHPATH],
    tags=["mbta", "spark", "snowflake", "daily"],
)
def mbta_daily_etl_pipeline():
    project_dir = PROJECT_DIR
    spark_dir = f"{project_dir}/spark"

    # 1. DDL setup tasks
    ensure_schemas = SQLExecuteQueryOperator(
        task_id="ensure_schemas",
        conn_id="snowflake_default",
        split_statements=True,
        sql=[
            "CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_RAW",
            "CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_STATIC",
            "CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_FACT",
            "CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_MART"
        ],
    )

    ensure_raw_tables = SQLExecuteQueryOperator(
        task_id="ensure_raw_tables",
        conn_id="snowflake_default",
        split_statements=True,
        sql="table_creation_commands/create_raw_tables.sql",
    )

    ensure_static_tables = SQLExecuteQueryOperator(
        task_id="ensure_static_tables",
        conn_id="snowflake_default",
        split_statements=True,
        sql="table_creation_commands/create_static_tables.sql",
    )

    ensure_fact_tables = SQLExecuteQueryOperator(
        task_id="ensure_fact_tables",
        conn_id="snowflake_default",
        split_statements=True,
        sql=[
            "table_creation_commands/create_fact_vehicle_positions.sql",
            "table_creation_commands/create_fact_alerts.sql",
            "table_creation_commands/create_fact_alerts_routes.sql",
            "table_creation_commands/create_fact_alerts_active_periods.sql",
        ],
    )

    ensure_mart_tables = SQLExecuteQueryOperator(
        task_id="ensure_mart_tables",
        conn_id="snowflake_default",
        split_statements=True,
        sql=[
            "table_creation_commands/create_mart_stop_events.sql",
            "table_creation_commands/create_mart_occupancy_route_hour.sql",
            "table_creation_commands/create_mart_alerts_by_day.sql",
            "table_creation_commands/create_mart_alerts_by_day_stops.sql",
        ],
    )

    # 2. Spark Tasks
    run_spark_static = BashOperator(
        task_id="run_spark_static",
        bash_command=bash_with_env(
            f"spark-submit --packages {SPARK_PACKAGES} {spark_dir}/spark_load_static.py --date {{{{ ds }}}}"
        ),
    )

    run_spark_rt_vehicle_positions = BashOperator(
        task_id="run_spark_rt_vehicle_positions",
        bash_command=bash_with_env(
            f"spark-submit --packages {SPARK_PACKAGES} {spark_dir}/spark_load_rt.py --date {{{{ ds }}}} --feed-type vehicle_positions"
        ),
    )

    run_spark_rt_alerts = BashOperator(
        task_id="run_spark_rt_alerts",
        bash_command=bash_with_env(
            f"spark-submit --packages {SPARK_PACKAGES} {spark_dir}/spark_load_rt.py --date {{{{ ds }}}} --feed-type alerts"
        ),
    )

    # 3. Snowflake Fact Tasks
    run_fact_vehicle_positions = SQLExecuteQueryOperator(
        task_id="run_fact_vehicle_positions",
        conn_id="snowflake_default",
        split_statements=True,
        sql="raw_to_fact_commands/vehicle_positions_fact.sql",
    )

    run_fact_alerts = SQLExecuteQueryOperator(
        task_id="run_fact_alerts",
        conn_id="snowflake_default",
        split_statements=True,
        sql="raw_to_fact_commands/alerts_fact.sql",
    )

    run_fact_alerts_routes = SQLExecuteQueryOperator(
        task_id="run_fact_alerts_routes",
        conn_id="snowflake_default",
        split_statements=True,
        sql="raw_to_fact_commands/alerts_routes_fact.sql",
    )

    run_fact_alerts_active_periods = SQLExecuteQueryOperator(
        task_id="run_fact_alerts_active_periods",
        conn_id="snowflake_default",
        split_statements=True,
        sql="raw_to_fact_commands/alerts_active_periods_fact.sql",
    )

    # 4. Snowflake Mart Tasks
    run_mart_occupancy = SQLExecuteQueryOperator(
        task_id="run_mart_occupancy",
        conn_id="snowflake_default",
        split_statements=True,
        sql="mart_commands/mart_occupancy.sql",
    )

    run_mart_stop_events = SQLExecuteQueryOperator(
        task_id="run_mart_stop_events",
        conn_id="snowflake_default",
        split_statements=True,
        sql="mart_commands/mart_stop_events.sql",
    )

    run_mart_alerts_by_day = SQLExecuteQueryOperator(
        task_id="run_mart_alerts_by_day",
        conn_id="snowflake_default",
        split_statements=True,
        sql="mart_commands/mart_alerts_by_day.sql",
    )

    run_mart_alerts_by_day_stops = SQLExecuteQueryOperator(
        task_id="run_mart_alerts_by_day_stops",
        conn_id="snowflake_default",
        split_statements=True,
        sql="mart_commands/mart_alerts_by_day_stops.sql",
    )

    # --- Execution Graph ---
    # Run Schemas and DDLs first
    ensure_schemas >> [ensure_fact_tables, ensure_mart_tables, ensure_static_tables, ensure_raw_tables]
    [ensure_fact_tables, ensure_mart_tables, ensure_static_tables, ensure_raw_tables] >> run_spark_static

    # Once static data is loaded, process real-time feeds
    run_spark_static >> [run_spark_rt_vehicle_positions, run_spark_rt_alerts]

    # Load facts after their respective RAW tables are populated
    run_spark_rt_vehicle_positions >> run_fact_vehicle_positions
    run_spark_rt_alerts >> [
        run_fact_alerts,
        run_fact_alerts_routes,
        run_fact_alerts_active_periods,
    ]

    # Build Marts from facts
    run_fact_vehicle_positions >> [run_mart_occupancy, run_mart_stop_events]
    run_fact_alerts >> run_mart_alerts_by_day
    run_fact_alerts_routes >> run_mart_alerts_by_day
    run_fact_alerts_active_periods >> run_mart_alerts_by_day

    run_fact_alerts >> run_mart_alerts_by_day_stops
    run_fact_alerts_routes >> run_mart_alerts_by_day_stops
    run_fact_alerts_active_periods >> run_mart_alerts_by_day_stops
    


# Instantiate the DAG
mbta_daily_etl_pipeline()
