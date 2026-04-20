-- Creates RAW and baseline project schemas/tables for GTFS realtime ingestion.
-- Using Claude Sonnet 4.6: Gave pyspark code for extracting dataframe columns 
-- and asked "write the sql queries to create the corresponding tables for this dataframe in Snowflake" 
-- then made manual modifications for more accurate column types

CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_RAW;
CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_STATIC;
CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_FACT;
CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_CURATED;

-- Creating the tables
CREATE TABLE IF NOT EXISTS FINAL_PROJECT_RAW.RAW_VEHICLE_POSITIONS (
    -- Partitioning / ingestion metadata
    service_date              DATE,
    hour                      INTEGER,
    snapshot_timestamp        TIMESTAMP_NTZ,
    ingested_at               TIMESTAMP_NTZ,
    gtfs_realtime_version     VARCHAR,

    -- Entity level
    entity_id                 VARCHAR,
    is_deleted                BOOLEAN,

    -- Trip descriptor
    trip_id                   VARCHAR,
    route_id                  VARCHAR,
    direction_id              INTEGER,
    trip_start_time           VARCHAR,        -- HH:MM:SS string from GTFS
    trip_start_date           DATE,
    trip_schedule_rel         VARCHAR,        

    -- Vehicle descriptor
    vehicle_id                VARCHAR,
    vehicle_label             VARCHAR,

    -- Multi-carriage (light rail only, array of structs → VARIANT)
    multi_carriage_details    VARIANT,

    -- Position
    latitude                  FLOAT,
    longitude                 FLOAT,
    bearing                   FLOAT,
    odometer                  FLOAT,
    speed                     FLOAT,

    -- Stop state
    current_stop_sequence     INTEGER,
    stop_id                   VARCHAR,
    current_status            VARCHAR,      
    congestion_level          VARCHAR,
    occupancy_status          VARCHAR,
    occupancy_percentage      INTEGER,

    -- GPS fix time (distinct from snapshot_timestamp)
    position_timestamp        TIMESTAMP_NTZ
);


CREATE TABLE IF NOT EXISTS FINAL_PROJECT_RAW.RAW_ALERTS (
    -- Partitioning / ingestion metadata
    service_date        DATE            NOT NULL,
    hour                INTEGER,
    snapshot_timestamp  TIMESTAMP_NTZ,
    ingested_at         TIMESTAMP_NTZ   NOT NULL,
    gtfs_realtime_version VARCHAR,

    -- Entity level
    entity_id           VARCHAR         NOT NULL,
    is_deleted          BOOLEAN,

    -- Alert scalars
    cause               VARCHAR,
    effect              VARCHAR,
    severity_level      VARCHAR,

    -- Complex types — use LATERAL FLATTEN downstream
    active_period       VARIANT,        -- array of {start, end} structs
    informed_entity     VARIANT,        -- array of {agency_id, route_id, route_type, trip, stop_id, direction_id}

    -- Localized text structs — use LATERAL FLATTEN + filter by language downstream
    url                 VARIANT,
    header_text         VARIANT,
    description_text    VARIANT,
    tts_header_text     VARIANT,
    tts_description_text VARIANT,
    cause_detail        VARIANT,
    effect_detail       VARIANT
);


CREATE TABLE IF NOT EXISTS FINAL_PROJECT_RAW.RAW_TRIP_UPDATES (
    -- Partitioning / metadata
    service_date DATE,
    hour INT,
    snapshot_timestamp TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ,
    gtfs_realtime_version STRING,

    -- Entity level
    entity_id STRING,
    is_deleted BOOLEAN,

    -- Trip descriptor
    trip_id STRING,
    route_id STRING,
    direction_id INT,
    trip_start_time STRING,
    trip_start_date DATE,
    trip_schedule_rel STRING,
    modified_trip VARIANT,

    -- Vehicle descriptor
    vehicle_id STRING,
    vehicle_label STRING,
    vehicle_license_plate STRING,
    wheelchair_accessible STRING,

    -- Trip update fields
    trip_update_timestamp TIMESTAMP_NTZ,
    delay INT,

    -- Nested / repeated fields
    stop_time_update VARIANT,
    trip_properties VARIANT
);


