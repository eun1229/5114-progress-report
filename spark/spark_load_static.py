"""
Spark job for loading versioned GTFS static data from S3 into Snowflake STATIC dimension tables.
"""

import argparse
import re
from datetime import date, timedelta
from pathlib import Path
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, BooleanType, DateType

import os
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
load_dotenv(PROJECT_ROOT / ".env")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

S3_STATIC_PATH_PREFIX = os.getenv(
    "S3_STATIC_PATH_PREFIX", "s3a://5114-transit-project-data/boston/gtfs_static/"
)

SNOWFLAKE_URL = os.getenv("SNOWFLAKE_URL", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
SNOWFLAKE_STATIC_SCHEMA = os.getenv("SNOWFLAKE_STATIC_SCHEMA", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "")
SNOWFLAKE_PRIVATE_KEY_PASSWORD = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSWORD", "").strip()
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()

# If there's an update to the static data, Spark job will load static data into tables in FINAL_PROJECT_STATIC schema in Snowflake


def create_spark_session():
    """
    Initializes and returns a SparkSession
    """
    spark_builder = (
        SparkSession.builder.appName("S3SparkIntegration")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.executor.extraJavaOptions",
            "-Dcom.amazonaws.services.s3.enableV4=true",
        )
        .config(
            "spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true"
        )
    )

    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        spark_builder = (
            spark_builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        )
    else:
        spark_builder = spark_builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    spark = spark_builder.getOrCreate()

    return spark


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load GTFS static feed files from S3 into Snowflake STATIC tables."
    )
    parser.add_argument(
        "--date",
        dest="service_date",
        default=os.getenv("SERVICE_DATE", ""),
        help="Optional service date in YYYYMMDD or YYYY-MM-DD. If omitted, loads latest available version.",
    )
    return parser.parse_args()


def normalize_service_date(service_date: str) -> str:
    return (service_date or "").replace("-", "")


# taken from Assignment 4 starter code
def get_private_key_string(key_path, password=None):
    """Reads a PEM private key and returns the string format required by PySpark."""
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password.encode() if password else None,
            backend=default_backend(),
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # Spark requires the raw key string without headers, footers, or newlines
    pkb_str = pkb.decode("utf-8")
    pkb_str = pkb_str.replace("-----BEGIN PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("-----END PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("\n", "")
    return pkb_str


def get_static_data_directory_if_exists(spark, service_date):
    """
    Static data folders are named v_service_date_timestamp. Search the existing bucket names to check whether
    a new set of static data exists for the logical date.
    """
    pattern = f"{S3_STATIC_PATH_PREFIX}v_{service_date}_*/"

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path.getFileSystem(conf)

    matches = fs.globStatus(path)
    if matches is None or len(matches) == 0:
        return None

    return [m.getPath().toString() for m in matches][0]


def get_latest_static_data_directory(spark):
    """
    Returns the newest static data directory by version name pattern
    v_YYYYMMDD_HHMMSS.
    """
    pattern = f"{S3_STATIC_PATH_PREFIX}v_*_*/"

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path.getFileSystem(conf)

    matches = fs.globStatus(path)
    if matches is None or len(matches) == 0:
        return None

    directories = [m.getPath().toString() for m in matches]
    valid_directories = []
    for directory in directories:
        dir_name = directory.rstrip("/").split("/")[-1]
        if re.match(r"^v_\d{8}_\d{6}$", dir_name):
            valid_directories.append(directory)

    if not valid_directories:
        return None

    return max(valid_directories, key=lambda d: d.rstrip("/").split("/")[-1])


# Since static data files are flat .txt files, converting them to dfs and loading them to Snowflake is fairly straightforward
# The private methods below were written primarily with Claude Sonnet 4.6, with supervision and some manual modifications.
# Main prompt: (with sample .txt files attached) Generate the pyspark code for creating dataframes from the static txt files. I've attached samples of
#               static data files that are present in a static data directory for a particular date. Use gtfs documentation if needed.

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_gtfs_date(col_name: str) -> F.Column:
    """Convert GTFS YYYYMMDD integer-string to a Snowflake DATE."""
    return F.to_date(F.col(col_name).cast("string"), "yyyyMMdd")


def _time_to_seconds(col_name: str) -> F.Column:
    """
    Convert a GTFS HH:MM:SS time string to total seconds since midnight.
    Handles times >= 24:00:00 (trips crossing midnight) correctly.
    Returns NULL when the source column is null or empty.
    """
    parts = F.split(F.col(col_name), ":")
    hour = parts.getItem(0).cast(IntegerType())
    return F.when(F.col(col_name).isNull() | (F.col(col_name) == ""), None).otherwise(
        hour * 3600
        + parts.getItem(1).cast(IntegerType()) * 60
        + parts.getItem(2).cast(IntegerType())
    )


def _read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a GTFS .txt (CSV) file with header inference."""
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")  # all strings initially; we cast explicitly
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")  # feed_version strings contain commas
        .csv(path)
    )


def _write_to_snowflake(df: DataFrame, table: str, sf_options: dict) -> None:
    """
    MERGE semantics are handled via Snowflake pre/post actions.
    We delete the feed_start_date partition being loaded, then append —
    making each load idempotent even if Spark is rerun for the same date.
    """
    feed_start_date = df.select("FEED_START_DATE").first()[0]

    options = {
        **sf_options,
        "dbtable": table,
        "preactions": f"DELETE FROM {table} WHERE FEED_START_DATE = '{feed_start_date}'",
    }

    (df.write.format("snowflake").options(**options).mode("append").save())
    print(
        f"  Loaded {df.count()} rows into {table} (feed_start_date={feed_start_date})"
    )


# ---------------------------------------------------------------------------
# Per-table transform functions
# ---------------------------------------------------------------------------


def _build_feed_info(
    spark: SparkSession, directory: str, collected_at: str, collection_date: str
) -> DataFrame:
    """
    dim_static_versions — one row per feed.
    collected_at and collection_date are parsed from the S3 directory name
    by the caller and passed in as strings.
    """
    return _read_csv(spark, f"{directory}feed_info.txt").select(
        _parse_gtfs_date("feed_start_date").alias("FEED_START_DATE"),
        _parse_gtfs_date("feed_end_date").alias("FEED_END_DATE"),
        F.col("feed_version").alias("FEED_VERSION"),
        F.col("feed_publisher_name").alias("FEED_PUBLISHER_NAME"),
        F.col("feed_publisher_url").alias("FEED_PUBLISHER_URL"),
        F.col("feed_lang").alias("FEED_LANG"),
        F.col("feed_contact_email").alias("FEED_CONTACT_EMAIL"),
        F.col("feed_id").alias("FEED_ID"),
        F.to_date(F.lit(collection_date), "yyyyMMdd").alias("COLLECTION_DATE"),
        F.to_timestamp(F.lit(collected_at), "yyyyMMdd_HHmmss").alias("COLLECTED_AT"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def _build_agency(
    spark: SparkSession, directory: str, feed_start_date: str
) -> DataFrame:
    """dim_agency"""
    return _read_csv(spark, f"{directory}agency.txt").select(
        F.lit(feed_start_date).cast(DateType()).alias("FEED_START_DATE"),
        F.col("agency_id").alias("AGENCY_ID"),
        F.col("agency_name").alias("AGENCY_NAME"),
        F.col("agency_url").alias("AGENCY_URL"),
        F.col("agency_timezone").alias("AGENCY_TIMEZONE"),
        F.col("agency_lang").alias("AGENCY_LANG"),
        F.col("agency_phone").alias("AGENCY_PHONE"),
        F.col("agency_fare_url").alias("AGENCY_FARE_URL"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def _build_routes(
    spark: SparkSession, directory: str, feed_start_date: str
) -> DataFrame:
    """dim_routes"""
    return _read_csv(spark, f"{directory}routes.txt").select(
        F.lit(feed_start_date).cast(DateType()).alias("FEED_START_DATE"),
        F.col("route_id").alias("ROUTE_ID"),
        F.col("agency_id").alias("AGENCY_ID"),
        F.col("route_short_name").alias("ROUTE_SHORT_NAME"),
        F.col("route_long_name").alias("ROUTE_LONG_NAME"),
        F.col("route_desc").alias("ROUTE_DESC"),
        F.col("route_type").cast(IntegerType()).alias("ROUTE_TYPE"),
        F.col("route_url").alias("ROUTE_URL"),
        F.col("route_color").alias("ROUTE_COLOR"),
        F.col("route_text_color").alias("ROUTE_TEXT_COLOR"),
        F.col("route_sort_order").cast(IntegerType()).alias("ROUTE_SORT_ORDER"),
        F.col("route_fare_class").alias("ROUTE_FARE_CLASS"),
        F.col("line_id").alias("LINE_ID"),
        F.col("listed_route").alias("LISTED_ROUTE"),
        F.col("network_id").alias("NETWORK_ID"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def _build_stops(
    spark: SparkSession, directory: str, feed_start_date: str
) -> DataFrame:
    """dim_stops"""
    return _read_csv(spark, f"{directory}stops.txt").select(
        F.lit(feed_start_date).cast(DateType()).alias("FEED_START_DATE"),
        F.col("stop_id").alias("STOP_ID"),
        F.col("stop_code").alias("STOP_CODE"),
        F.col("stop_name").alias("STOP_NAME"),
        F.col("stop_desc").alias("STOP_DESC"),
        F.col("platform_code").alias("PLATFORM_CODE"),
        F.col("platform_name").alias("PLATFORM_NAME"),
        F.col("stop_lat").cast(FloatType()).alias("STOP_LAT"),
        F.col("stop_lon").cast(FloatType()).alias("STOP_LON"),
        F.col("zone_id").alias("ZONE_ID"),
        F.col("stop_url").alias("STOP_URL"),
        F.col("level_id").alias("LEVEL_ID"),
        F.col("location_type").cast(IntegerType()).alias("LOCATION_TYPE"),
        F.col("parent_station").alias("PARENT_STATION"),
        F.col("wheelchair_boarding").cast(IntegerType()).alias("WHEELCHAIR_BOARDING"),
        F.col("stop_address").alias("STOP_ADDRESS"),
        F.col("municipality").alias("MUNICIPALITY"),
        F.col("on_street").alias("ON_STREET"),
        F.col("at_street").alias("AT_STREET"),
        F.col("vehicle_type").cast(IntegerType()).alias("VEHICLE_TYPE"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def _build_trips(
    spark: SparkSession, directory: str, feed_start_date: str
) -> DataFrame:
    """dim_trips"""
    return _read_csv(spark, f"{directory}trips.txt").select(
        F.lit(feed_start_date).cast(DateType()).alias("FEED_START_DATE"),
        F.col("trip_id").alias("TRIP_ID"),
        F.col("route_id").alias("ROUTE_ID"),
        F.col("service_id").alias("SERVICE_ID"),
        F.col("trip_headsign").alias("TRIP_HEADSIGN"),
        F.col("trip_short_name").alias("TRIP_SHORT_NAME"),
        F.col("direction_id").cast(IntegerType()).alias("DIRECTION_ID"),
        F.col("block_id").alias("BLOCK_ID"),
        F.col("shape_id").alias("SHAPE_ID"),
        F.col("wheelchair_accessible")
        .cast(IntegerType())
        .alias("WHEELCHAIR_ACCESSIBLE"),
        F.col("bikes_allowed").cast(IntegerType()).alias("BIKES_ALLOWED"),
        F.col("trip_route_type").cast(IntegerType()).alias("TRIP_ROUTE_TYPE"),
        F.col("route_pattern_id").alias("ROUTE_PATTERN_ID"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def _build_stop_times(
    spark: SparkSession, directory: str, feed_start_date: str
) -> DataFrame:
    """
    dim_stop_times — largest file.
    arrival_seconds and departure_seconds are derived here
    so Snowflake never needs to parse the raw time strings.
    """
    return _read_csv(spark, f"{directory}stop_times.txt").select(
        F.lit(feed_start_date).cast(DateType()).alias("FEED_START_DATE"),
        F.col("trip_id").alias("TRIP_ID"),
        F.col("stop_sequence").cast(IntegerType()).alias("STOP_SEQUENCE"),
        F.col("stop_id").alias("STOP_ID"),
        F.col("arrival_time").alias("ARRIVAL_TIME"),
        F.col("departure_time").alias("DEPARTURE_TIME"),
        _time_to_seconds("arrival_time").alias("ARRIVAL_SECONDS"),
        _time_to_seconds("departure_time").alias("DEPARTURE_SECONDS"),
        F.col("stop_headsign").alias("STOP_HEADSIGN"),
        F.col("pickup_type").cast(IntegerType()).alias("PICKUP_TYPE"),
        F.col("drop_off_type").cast(IntegerType()).alias("DROP_OFF_TYPE"),
        F.col("timepoint").cast(IntegerType()).alias("TIMEPOINT"),
        F.col("checkpoint_id").alias("CHECKPOINT_ID"),
        F.col("continuous_pickup").cast(IntegerType()).alias("CONTINUOUS_PICKUP"),
        F.col("continuous_drop_off").cast(IntegerType()).alias("CONTINUOUS_DROP_OFF"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def _build_calendar(
    spark: SparkSession, directory: str, feed_start_date: str
) -> DataFrame:
    """dim_calendar"""
    return _read_csv(spark, f"{directory}calendar.txt").select(
        F.lit(feed_start_date).cast(DateType()).alias("FEED_START_DATE"),
        F.col("service_id").alias("SERVICE_ID"),
        F.col("monday").cast(BooleanType()).alias("MONDAY"),
        F.col("tuesday").cast(BooleanType()).alias("TUESDAY"),
        F.col("wednesday").cast(BooleanType()).alias("WEDNESDAY"),
        F.col("thursday").cast(BooleanType()).alias("THURSDAY"),
        F.col("friday").cast(BooleanType()).alias("FRIDAY"),
        F.col("saturday").cast(BooleanType()).alias("SATURDAY"),
        F.col("sunday").cast(BooleanType()).alias("SUNDAY"),
        _parse_gtfs_date("start_date").alias("START_DATE"),
        _parse_gtfs_date("end_date").alias("END_DATE"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def _build_calendar_dates(
    spark: SparkSession, directory: str, feed_start_date: str
) -> DataFrame:
    """dim_calendar_dates"""
    return _read_csv(spark, f"{directory}calendar_dates.txt").select(
        F.lit(feed_start_date).cast(DateType()).alias("FEED_START_DATE"),
        F.col("service_id").alias("SERVICE_ID"),
        _parse_gtfs_date("date").alias("DATE"),
        F.col("exception_type").cast(IntegerType()).alias("EXCEPTION_TYPE"),
        F.col("holiday_name").alias("HOLIDAY_NAME"),
        F.current_timestamp().alias("INGESTED_AT"),
    )


def load_static_data_to_snowflake(
    spark,
    static_data_directory,
    sf_options,
):
    """
    Reads all GTFS static files from static_data_directory and loads them
    into the FINAL_PROJECT_STATIC dimension tables in Snowflake.

    static_data_directory is the full S3 path to the versioned static folder
    e.g. s3a://bucket/boston/static/v_20260324_020012/
    """

    # Parse version metadata from the directory name
    dir_name = static_data_directory.rstrip("/").split("/")[-1]  # "v_20260324_020012"
    parts = dir_name.split("_")  # ["v", "20260324", "020012"]
    collection_date = parts[1]  # "20260324"
    collected_at_str = f"{parts[1]}_{parts[2]}"  # "20260324_020012"

    # Read feed_info.txt first, as it contains the feed_start_date that is added to the other dimension tables
    # to aid in correctly linking the correct static feed version to realtime data.
    feed_info_df = _build_feed_info(
        spark, static_data_directory, collected_at_str, collection_date
    )
    feed_start_date = feed_info_df.select("FEED_START_DATE").first()[
        0
    ]  # Python date object
    feed_start_date_str = feed_start_date.strftime("%Y-%m-%d")

    print(f"\nLoading static data:")
    print(f"  directory      : {static_data_directory}")
    print(f"  collection_date: {collection_date}")
    print(f"  feed_start_date: {feed_start_date_str}")

    # Build all dataframes
    tables = [
        (feed_info_df, "DIM_STATIC_VERSIONS"),
        (
            _build_agency(spark, static_data_directory, feed_start_date_str),
            "DIM_AGENCY",
        ),
        (
            _build_routes(spark, static_data_directory, feed_start_date_str),
            "DIM_ROUTES",
        ),
        (_build_stops(spark, static_data_directory, feed_start_date_str), "DIM_STOPS"),
        (_build_trips(spark, static_data_directory, feed_start_date_str), "DIM_TRIPS"),
        (
            _build_calendar(spark, static_data_directory, feed_start_date_str),
            "DIM_CALENDAR",
        ),
        (
            _build_calendar_dates(spark, static_data_directory, feed_start_date_str),
            "DIM_CALENDAR_DATES",
        ),
        (
            _build_stop_times(spark, static_data_directory, feed_start_date_str),
            "DIM_STOP_TIMES",
        ),
    ]

    for df, table in tables:
        print(f"\n  -> {table}")
        _write_to_snowflake(df, table, sf_options)

    print(f"\nStatic load complete for feed_start_date={feed_start_date_str}")


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    pkb_string = get_private_key_string(
        SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY_PASSWORD
    )

    sfOptions = {
        "sfURL": SNOWFLAKE_URL,
        "sfUser": SNOWFLAKE_USER,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_STATIC_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "pem_private_key": f"{pkb_string}",
    }

    if SNOWFLAKE_ROLE:
        sfOptions["sfRole"] = SNOWFLAKE_ROLE

    service_date_to_load = (
        date.fromisoformat(args.service_date) - timedelta(days=1)
    ).isoformat()
    service_date = normalize_service_date(service_date_to_load)
    if service_date:
        static_data_directory = get_static_data_directory_if_exists(spark, service_date)
        not_found_message = f"No static data update for date {service_date}."
    else:
        static_data_directory = get_latest_static_data_directory(spark)
        not_found_message = "No static data update found in S3."

    if static_data_directory is None:
        print(not_found_message)
    else:
        load_static_data_to_snowflake(
            spark, static_data_directory.rstrip("/") + "/", sfOptions
        )

    spark.stop()
