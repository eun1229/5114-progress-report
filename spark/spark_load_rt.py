"""
Spark job for loading GTFS realtime feeds from S3 into Snowflake RAW tables.
"""

import argparse
from datetime import date, timedelta
from pathlib import Path
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, DataFrame
from functools import reduce
from pyspark.sql.functions import (
    explode,
    col,
    lit,
    reduce,
    input_file_name,
    regexp_extract,
    current_timestamp,
    to_timestamp,
    to_date,
    row_number,
    desc,
    concat,
    pmod,
    hash,
    from_utc_timestamp,
)
from pyspark.sql.window import Window
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.errors import AnalysisException
from pyspark.sql.types import ArrayType, StructType, StructField
import os
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import time


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
load_dotenv(PROJECT_ROOT / ".env")


REALTIME_SPEC_PATH = os.getenv(
    "REALTIME_SPEC_PATH", str(BASE_DIR / "gtfs-realtime.desc")
)  # generated using protoc and gtfs-realtime.proto (https://gtfs.org/documentation/realtime/proto/)

# feed types for configuration
VEHICLE_POSITION = "vehicle_positions"
TRIP_UPDATE = "trip_updates"
ALERT = "alerts"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

S3_RT_PATH_PREFIX = os.getenv(
    "S3_RT_PATH_PREFIX", "s3a://5114-transit-project-data/boston/rt/"
)

SNOWFLAKE_URL = os.getenv("SNOWFLAKE_URL", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
SNOWFLAKE_RAW_SCHEMA = os.getenv("SNOWFLAKE_RAW_SCHEMA", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "")
SNOWFLAKE_PRIVATE_KEY_PASSWORD = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSWORD", "").strip()
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()

# Spark job will load realtime data into tables in the FINAL_PROJECT_RAW schema


def create_spark_session():
    """
    Initializes and returns a SparkSession
    """
    spark_builder = (
        SparkSession.builder.appName("S3SparkIntegration")
        .config("spark.sql.session.timeZone", "UTC")
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
        description="Load GTFS realtime feeds from S3 into Snowflake RAW tables."
    )
    parser.add_argument(
        "--date",
        dest="service_date",
        default=os.getenv("SERVICE_DATE", ""),
        help="Service date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--feed-type",
        dest="feed_type",
        default=os.getenv("RT_FEED_TYPE", VEHICLE_POSITION),
        choices=[VEHICLE_POSITION, ALERT, TRIP_UPDATE],
        help="Realtime feed type to load.",
    )
    return parser.parse_args()


def require_service_date(service_date: str) -> str:
    if service_date:
        return service_date
    raise ValueError(
        "service_date is required. Provide --date YYYY-MM-DD or set SERVICE_DATE in .env"
    )


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


def to_eastern(col_name):
    """
    Convert a Unix epoch integer column to a TIMESTAMP_NTZ in America/New_York
    local time. The IANA time zone database (America/New_York) handles daylight savings.
    """
    return from_utc_timestamp(
        to_timestamp(col(col_name)), "America/New_York"  # epoch int -> UTC timestamp
    )


# Credits to Claude Sonnet 4.6: Gave the schema returned by df.printSchema() and prompted "Write a function to extract flattened columns for the Vehicle Position entity and the corresponding SQL table"
def extract_vehicle_position_cols(df):
    # Explode and alias columns according to GTFS vehicle position feed schema
    vehicle_df = (
        df.select(
            "service_date",
            "hour",
            "ingested_at",  # when this Spark job ran
            col("feed.header.timestamp").alias(
                "snapshot_timestamp"
            ),  # when the API was called
            col("feed.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
            explode("feed.entity").alias("entity"),
        )
        .where(col("entity.vehicle").isNotNull())
        .select(
            col("service_date"),
            col("hour"),
            col("snapshot_timestamp"),
            col("ingested_at"),
            col("gtfs_realtime_version"),
            # Entity level
            col("entity.id").alias("entity_id"),
            col("entity.is_deleted").alias("is_deleted"),
            # Trip descriptor
            col("entity.vehicle.trip.trip_id").alias("trip_id"),
            col("entity.vehicle.trip.route_id").alias("route_id"),
            col("entity.vehicle.trip.direction_id").alias("direction_id"),
            col("entity.vehicle.trip.start_time").alias("trip_start_time"),
            col("entity.vehicle.trip.start_date").alias("trip_start_date"),
            col("entity.vehicle.trip.schedule_relationship").alias("trip_schedule_rel"),
            # Vehicle descriptor
            col("entity.vehicle.vehicle.id").alias("vehicle_id"),
            col("entity.vehicle.vehicle.label").alias("vehicle_label"),
            # Only appears in light rail entities. Keep as an array of structs.
            col("entity.vehicle.multi_carriage_details").alias(
                "multi_carriage_details"
            ),
            # Position
            col("entity.vehicle.position.latitude").alias("latitude"),
            col("entity.vehicle.position.longitude").alias("longitude"),
            col("entity.vehicle.position.bearing").alias("bearing"),
            col("entity.vehicle.position.odometer").alias("odometer"),
            col("entity.vehicle.position.speed").alias("speed"),
            # Stop state
            col("entity.vehicle.current_stop_sequence").alias("current_stop_sequence"),
            col("entity.vehicle.stop_id").alias("stop_id"),
            col("entity.vehicle.current_status").alias("current_status"),
            col("entity.vehicle.congestion_level").alias("congestion_level"),
            col("entity.vehicle.occupancy_status").alias("occupancy_status"),
            col("entity.vehicle.occupancy_percentage").alias("occupancy_percentage"),
            # Vehicle position timestamp (when the GPS reading was made, distinct from snapshot_timestamp)
            col("entity.vehicle.timestamp").alias("position_timestamp"),
        )
        # Cast types
        .withColumn("snapshot_timestamp", to_eastern("snapshot_timestamp"))
        .withColumn("position_timestamp", to_eastern("position_timestamp"))
        .withColumn("trip_start_date", to_date(col("trip_start_date"), "yyyyMMdd"))
    )

    return vehicle_df


# Credits to Claude Sonnet 4.6 (similar to the extract_vehicle_position_cols query)
def extract_alert_cols(df):
    alert_df = (
        df.select(
            "service_date",
            "hour",
            "ingested_at",
            col("feed.header.timestamp").alias("snapshot_timestamp"),
            col("feed.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
            explode("feed.entity").alias("entity"),
        )
        .where(col("entity.alert").isNotNull())
        .select(
            col("service_date"),
            col("hour"),
            col("snapshot_timestamp"),
            col("ingested_at"),
            col("gtfs_realtime_version"),
            # Entity level
            col("entity.id").alias("entity_id"),
            col("entity.is_deleted").alias("is_deleted"),
            # Alert scalars
            col("entity.alert.cause").alias("cause"),
            col("entity.alert.effect").alias("effect"),
            col("entity.alert.severity_level").alias("severity_level"),
            # Keep as arrays of structs, flatten in Snowflake
            col("entity.alert.active_period").alias("active_period"),
            col("entity.alert.informed_entity").alias("informed_entity"),
            # Localized text fields, each is a struct with a translation array
            col("entity.alert.url").alias("url"),
            col("entity.alert.header_text").alias("header_text"),
            col("entity.alert.description_text").alias("description_text"),
            col("entity.alert.tts_header_text").alias("tts_header_text"),
            col("entity.alert.tts_description_text").alias("tts_description_text"),
            col("entity.alert.cause_detail").alias("cause_detail"),
            col("entity.alert.effect_detail").alias("effect_detail"),
        )
        .withColumn("snapshot_timestamp", to_eastern("snapshot_timestamp"))
    )

    return alert_df


def dedupe_alerts_to_latest_snapshot(df):
    """
    For each alert entity_id on a given service date, keep only the row
    from the most recent snapshot_timestamp.
    """
    window = Window.partitionBy("service_date", "entity_id").orderBy(
        desc("snapshot_timestamp")
    )
    return (
        df.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def load_data_from_realtime_s3_to_df(spark, feed_type, service_date):
    """
    pb_files_for_date goes down to the feed type being processed.
    example: s3a://5114-transit-project-data/boston/rt/vehicle_positions/
    """
    # Trip updates return the entire network per snapshot
    # They are processed by the hour, as the data is too large to glob per day
    if feed_type == TRIP_UPDATE:
        return load_trip_updates_data_from_realtime_s3_to_df(
            spark, feed_type, service_date
        )

    pb_files_for_date = f"{S3_RT_PATH_PREFIX}{feed_type}/dt={service_date}/hour=*/*.pb"

    raw_feed_df = (
        spark.read.format("binaryFile")
        .load(pb_files_for_date)
        .withColumn("service_date", lit(service_date))
        .withColumn(
            "hour",
            lit(regexp_extract(input_file_name(), r"hour=(\d{2})", 1).cast("int")),
        )
        .withColumn("ingested_at", lit(current_timestamp()))
        .select(
            col("service_date"),
            col("hour"),
            col("ingested_at"),
            from_protobuf(
                "content",
                "transit_realtime.FeedMessage",
                descFilePath=REALTIME_SPEC_PATH,
            ).alias("feed"),
        )
    )

    if feed_type == VEHICLE_POSITION:
        return extract_vehicle_position_cols(raw_feed_df)
    # elif feed_type == TRIP_UPDATE:
    #    TODO
    elif feed_type == ALERT:
        alert_df = extract_alert_cols(raw_feed_df)
        return dedupe_alerts_to_latest_snapshot(alert_df)
    else:
        raise ValueError(
            "Feed type must be one of VEHICLE_POSITION, ALERT, TRIP_UPDATE"
        )


def write_raw_df_to_snowflake(df, table_name, service_date, hour=None):
    pkb_string = get_private_key_string(
        SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY_PASSWORD
    )
    delete_query = f"DELETE FROM {SNOWFLAKE_RAW_SCHEMA}.{table_name} WHERE service_date = '{service_date}'"

    if hour is not None:
        delete_query = f"DELETE FROM {SNOWFLAKE_RAW_SCHEMA}.{table_name} WHERE service_date = '{service_date}' AND hour = {hour}"

    sfOptions = {
        "sfURL": SNOWFLAKE_URL,
        "sfUser": SNOWFLAKE_USER,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_RAW_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "pem_private_key": f"{pkb_string}",
        "preactions": delete_query,  # idempotency guard
    }

    if SNOWFLAKE_ROLE:
        sfOptions["sfRole"] = SNOWFLAKE_ROLE

    df.write.format("net.snowflake.spark.snowflake").options(**sfOptions).option(
        "dbtable", table_name
    ).mode("append").save()
    print(f"Finished writing to {table_name}")


# --------------------------------------------------------------------------------------------------------------------------
#   Trip update related
# --------------------------------------------------------------------------------------------------------------------------

def extract_trip_update_cols(df):
    trip_update_df = (df
        .select(
            "service_date",
            "hour",
            "ingested_at",
            col("feed.header.timestamp").alias("snapshot_timestamp"),
            col("feed.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
            explode("feed.entity").alias("entity")
        )
        .where(col("entity.trip_update").isNotNull())
        .select(
            col("service_date"),
            col("hour"),
            col("snapshot_timestamp"),
            col("ingested_at"),
            col("gtfs_realtime_version"),

            # Entity level
            col("entity.id").alias("entity_id"),
            col("entity.is_deleted").alias("is_deleted"),

            # Trip descriptor (scalars flattened)
            col("entity.trip_update.trip.trip_id").alias("trip_id"),
            col("entity.trip_update.trip.route_id").alias("route_id"),
            col("entity.trip_update.trip.direction_id").alias("direction_id"),
            col("entity.trip_update.trip.start_time").alias("trip_start_time"),
            col("entity.trip_update.trip.start_date").alias("trip_start_date"),
            col("entity.trip_update.trip.schedule_relationship").alias("trip_schedule_rel"),
            # modified_trip is a nested struct — keep as VARIANT
            col("entity.trip_update.trip.modified_trip").alias("modified_trip"),

            # Vehicle descriptor (scalars flattened)
            col("entity.trip_update.vehicle.id").alias("vehicle_id"),
            col("entity.trip_update.vehicle.label").alias("vehicle_label"),
            col("entity.trip_update.vehicle.license_plate").alias("vehicle_license_plate"),
            col("entity.trip_update.vehicle.wheelchair_accessible").alias("wheelchair_accessible"),

            # Top-level trip_update scalars
            col("entity.trip_update.timestamp").alias("trip_update_timestamp"),
            col("entity.trip_update.delay").alias("delay"),

            # Keep as VARIANT — flatten in Snowflake
            col("entity.trip_update.stop_time_update").alias("stop_time_update"),
            col("entity.trip_update.trip_properties").alias("trip_properties"),
        )
        .withColumn("snapshot_timestamp", to_eastern("snapshot_timestamp"))
        .withColumn("trip_update_timestamp", to_eastern("trip_update_timestamp"))
        .withColumn("trip_start_date", to_date(col("trip_start_date"), "yyyyMMdd"))
    )

    return trip_update_df


def dedupe_trip_updates(df):
    """
    For each trip instance on a given service date, keep only the final
    snapshot it appeared in (a completed trip will disappear
    from the feed, so its last appearance reflects its terminal state).
    """
    window = Window.partitionBy("service_date", "trip_id", "trip_start_date").orderBy(
        desc("snapshot_timestamp")
    )

    return (
        df.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def load_trip_updates_data_from_realtime_s3_to_df(spark, feed_type, service_date, snapshot_stride=5):
    for hour in range(24):
        pb_files_from_hour = f"{S3_RT_PATH_PREFIX}{feed_type}/dt={service_date}/hour={str(hour).zfill(2)}/*.pb"
        files_df = (
            spark.read.format("binaryFile")
            .load(pb_files_from_hour)
            .select("path")
            .orderBy("path")
        )

        file_paths = [row["path"] for row in files_df.collect()]

        if not file_paths:
            print(f"No files found for {pb_files_from_hour}")
            continue

        if snapshot_stride > 1:
            file_paths = file_paths[::snapshot_stride]

        try:        
            raw_feed_df = spark.read.format("binaryFile") \
            .load(file_paths) \
            .withColumn("service_date", lit(service_date)) \
            .withColumn("hour", lit(regexp_extract(input_file_name(), r"hour=(\d{2})", 1).cast("int"))) \
            .withColumn("ingested_at", lit(current_timestamp())) \
            .select(
                    col("service_date"),
                    col("hour"),
                    col("ingested_at"),
                    from_protobuf(
                        "content",
                        "transit_realtime.FeedMessage",
                        descFilePath=REALTIME_SPEC_PATH
                    ).alias("feed")
                )
            trip_update_df = extract_trip_update_cols(raw_feed_df)
            trip_update_df = trip_update_df.repartition(200, "trip_id", "trip_start_date")

            print(f"Handling hour {hour}")
            deduped_trip_update_df = dedupe_trip_updates(trip_update_df)
            
            write_raw_df_to_snowflake(deduped_trip_update_df, f"RAW_{rt_feed_option.upper()}", service_date, hour)

            
        except AnalysisException as e:
            # Directory for hour=02 does not exist on daylight savings
            print(e)
            print(f"Skipping path: {pb_files_from_hour}")
            continue

# --------------------------------------------------------------------------------------------------------------------------
#   End of trip update related
# --------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # configurations to be set by pipeline
    service_date = (
        date.fromisoformat(require_service_date(args.service_date)) - timedelta(days=1)
    ).isoformat()
    rt_feed_option = args.feed_type

    # load selected rt data for a day into a single df and write to table in RAW schema in Snowflake
    raw_rt_df = load_data_from_realtime_s3_to_df(spark, rt_feed_option, service_date)
    if raw_rt_df is not None:  # (feed option trip updates returns None as of now)
        write_raw_df_to_snowflake(
            raw_rt_df, f"RAW_{rt_feed_option.upper()}", service_date
        )

    spark.stop()