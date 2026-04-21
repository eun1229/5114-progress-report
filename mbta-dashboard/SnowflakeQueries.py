"""
snowflake_queries.py

Handles all Snowflake connection and query logic for the MBTA dashboard.
Separating this from dashboard.py keeps connection logic in one place
and makes it easy to update credentials or query patterns.
"""

import streamlit as st
import snowflake.connector
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


@st.cache_resource
def get_snowflake_conn():
    """
    Create and cache a Snowflake connection using RSA key-pair authentication.
    Uses st.secrets for credentials — never hardcode passwords.

    Returns:
        snowflake.connector.connection: Active Snowflake connection
    """
    with open(st.secrets["SF_PRIVATE_KEY_PATH"], "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=st.secrets["SF_USER"],
        account=st.secrets["SF_ACCOUNT"],
        private_key=private_key_bytes,
        warehouse=st.secrets["SF_WAREHOUSE"],
        database="LEMMING_DB",
        schema="FINAL_PROJECT_MART"
    )


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    """
    Execute a SQL query against Snowflake and return results as a DataFrame.
    Results are cached for 5 minutes (ttl=300) to avoid unnecessary
    warehouse compute costs on repeated identical queries.

    Args:
        sql: SQL query string to execute

    Returns:
        pd.DataFrame: Query results. Empty DataFrame on error.
    """
    try:
        conn = get_snowflake_conn()
        return pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"Snowflake query failed: {e}")
        return pd.DataFrame()


def get_available_routes() -> list:
    """
    Fetch list of all route short names available in the MART tables.

    Returns:
        list: Sorted list of route short name strings
    """
    df = query("""
        SELECT DISTINCT ROUTE_SHORT_NAME
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
        WHERE ROUTE_SHORT_NAME IS NOT NULL
        ORDER BY ROUTE_SHORT_NAME
    """)
    return df["ROUTE_SHORT_NAME"].dropna().tolist() if not df.empty else []


def get_date_range() -> tuple:
    """
    Get the min and max service dates available in the MART tables.

    Returns:
        tuple: (min_date, max_date) as strings
    """
    df = query("""
        SELECT
            MIN(SERVICE_DATE) AS min_date,
            MAX(SERVICE_DATE) AS max_date
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_OVERALL_DAY
    """)
    if not df.empty:
        return df["MIN_DATE"].iloc[0], df["MAX_DATE"].iloc[0]
    return None, None


def get_occupancy_by_route(route_filter: str) -> pd.DataFrame:
    """
    Fetch occupancy metrics aggregated by route.

    Args:
        route_filter: SQL-safe comma-separated quoted route names

    Returns:
        pd.DataFrame: One row per route with occupancy breakdown columns
    """
    return query(f"""
        SELECT
            ROUTE_ID,
            ROUTE_SHORT_NAME,
            AVG_OCCUPANCY_PCT,
            PCT_EMPTY,
            PCT_MANY_SEATS,
            PCT_FEW_SEATS,
            PCT_STANDING_ROOM,
            PCT_CRUSHED_STANDING,
            PCT_FULL,
            SNAPSHOT_COUNT
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
        WHERE ROUTE_SHORT_NAME IN ({route_filter})
        ORDER BY AVG_OCCUPANCY_PCT DESC
    """)


def get_occupancy_by_hour(start_date, end_date, route_filter: str) -> pd.DataFrame:
    """
    Fetch occupancy metrics broken down by hour of day.

    Args:
        start_date: Start of date range
        end_date: End of date range
        route_filter: SQL-safe comma-separated quoted route names

    Returns:
        pd.DataFrame: One row per (service_date, hour, route)
    """
    return query(f"""
        SELECT
            SERVICE_DATE,
            HOUR,
            ROUTE_ID,
            ROUTE_SHORT_NAME,
            AVG_OCCUPANCY_PCT,
            PCT_STANDING_ROOM,
            SNAPSHOT_COUNT
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR
        WHERE SERVICE_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND ROUTE_SHORT_NAME IN ({route_filter})
        ORDER BY SERVICE_DATE, HOUR
    """)


def get_stop_events(start_date, end_date) -> pd.DataFrame:
    """
    Fetch stop-level event aggregations.

    Args:
        start_date: Start of date range
        end_date: End of date range

    Returns:
        pd.DataFrame: Aggregated stop events with trip and vehicle counts
    """
    return query(f"""
        SELECT
            SERVICE_DATE,
            ROUTE_ID,
            STOP_NAME,
            COUNT(*) AS event_count,
            COUNT(DISTINCT TRIP_ID) AS trip_count,
            COUNT(DISTINCT VEHICLE_ID) AS vehicle_count
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_STOP_EVENTS
        WHERE SERVICE_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2, 3
        ORDER BY event_count DESC
        LIMIT 1000
    """)