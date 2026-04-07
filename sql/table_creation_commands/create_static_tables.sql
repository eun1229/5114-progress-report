-- (Similar to create_raw_tables.sql) Using Claude Sonnet 4.6: Gave pyspark code for extracting dataframe columns for 
-- static data and asked "write the sql queries to create the corresponding dimension tables for these dataframes in Snowflake"
-- then made manual modifications for more accurate column types

CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT_STATIC;
USE SCHEMA FINAL_PROJECT_STATIC;


-- -------------------------------------------------------------
-- dim_static_versions
-- One row per collected static feed. The anchor table for all
-- version-keyed joins. Written first before any other dim table.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_static_versions (
    feed_start_date         DATE          NOT NULL,  -- true effective date, PK
    feed_end_date           DATE          NOT NULL,
    feed_version            VARCHAR       NOT NULL,  -- full version string from feed_info.txt
    feed_publisher_name     VARCHAR,
    feed_publisher_url      VARCHAR,
    feed_lang               VARCHAR(10),
    feed_contact_email      VARCHAR,
    feed_id                 VARCHAR,
    collection_date         DATE          NOT NULL,  -- S3 folder date (from file name)
    collected_at            TIMESTAMP_TZ  NOT NULL,  -- datetime from file name (e.g. v_20260324_020012)
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_static_versions PRIMARY KEY (feed_start_date)
);


-- -------------------------------------------------------------
-- dim_agency
-- Small table for Boston (two agencies, MBTA + Cape Cod RTA).
-- Versioned because agency metadata could change across feeds.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_agency (
    feed_start_date         DATE          NOT NULL,
    agency_id               VARCHAR       NOT NULL,
    agency_name             VARCHAR       NOT NULL,
    agency_url              VARCHAR,
    agency_timezone         VARCHAR,
    agency_lang             VARCHAR(10),
    agency_phone            VARCHAR,
    agency_fare_url         VARCHAR,
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_agency PRIMARY KEY (feed_start_date, agency_id),
    CONSTRAINT fk_agency_version FOREIGN KEY (feed_start_date)
        REFERENCES dim_static_versions (feed_start_date)
);


-- -------------------------------------------------------------
-- dim_routes
-- Includes all MBTA-specific extension columns.
--
-- route_type reference:
--   0  Tram / light rail (Mattapan)
--   1  Subway / metro (Red, Orange, Blue, Green)
--   2  Commuter rail
--   3  Bus
--   4  Ferry
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_routes (
    feed_start_date         DATE          NOT NULL,
    route_id                VARCHAR       NOT NULL,
    agency_id               VARCHAR,
    route_short_name        VARCHAR,      -- e.g. "1", "39", "SL1"
    route_long_name         VARCHAR,      -- e.g. "Red Line"
    route_desc              VARCHAR,      -- e.g. "Rapid Transit", "Local Bus"
    route_type              INT           NOT NULL, -- GTFS route_type integer
    route_url               VARCHAR,
    route_color             VARCHAR(6),   -- hex without #
    route_text_color        VARCHAR(6),
    route_sort_order        INT,
    -- MBTA extensions
    route_fare_class        VARCHAR,      -- e.g. "Rapid Transit", "Local Bus", "Express Bus"
    line_id                 VARCHAR,      -- e.g. "line-Red", "line-39"
    listed_route            VARCHAR,      -- flag for public-facing routes
    network_id              VARCHAR,      -- e.g. "rapid_transit", "local_bus"
    
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_routes PRIMARY KEY (feed_start_date, route_id),
    CONSTRAINT fk_routes_version FOREIGN KEY (feed_start_date)
        REFERENCES dim_static_versions (feed_start_date)
);

COMMENT ON COLUMN dim_routes.route_type IS
    '0=light rail, 1=subway, 2=commuter rail, 3=bus, 4=ferry';
COMMENT ON COLUMN dim_routes.route_fare_class IS
    'MBTA extension — fare category, e.g. Local Bus, Express Bus, Rapid Transit';


-- -------------------------------------------------------------
-- dim_stops
-- Includes MBTA-specific extension columns.
--
-- location_type reference:
--   0  Stop (default) — a boarding/alighting point
--   1  Station — a physical building grouping stops
--   2  Entrance/exit
--   3  Generic node (pathway)
--   4  Boarding area
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_stops (
    feed_start_date         DATE          NOT NULL,
    stop_id                 VARCHAR       NOT NULL,
    stop_code               VARCHAR,      -- passenger-facing code (same as stop_id for MBTA)
    stop_name               VARCHAR,
    stop_desc               VARCHAR,
    platform_code           VARCHAR,      -- e.g. "1", "A"
    platform_name           VARCHAR,      -- e.g. "Track 1"
    stop_lat                FLOAT,
    stop_lon                FLOAT,
    zone_id                 VARCHAR,      -- fare zone
    stop_url                VARCHAR,
    level_id                VARCHAR,
    location_type           INT,          -- see comment above
    parent_station          VARCHAR,      -- stop_id of parent station if location_type > 0
    wheelchair_boarding     INT,          -- 0=no info, 1=accessible, 2=not accessible
    -- MBTA extensions
    stop_address            VARCHAR,
    municipality            VARCHAR,
    on_street               VARCHAR,
    at_street               VARCHAR,
    vehicle_type            INT,          -- route_type of vehicles serving this stop
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_stops PRIMARY KEY (feed_start_date, stop_id),
    CONSTRAINT fk_stops_version FOREIGN KEY (feed_start_date)
        REFERENCES dim_static_versions (feed_start_date)
);

COMMENT ON COLUMN dim_stops.location_type IS
    '0=stop, 1=station, 2=entrance/exit, 3=generic node, 4=boarding area';
COMMENT ON COLUMN dim_stops.vehicle_type IS
    'MBTA extension — route_type of vehicles using this stop';


-- -------------------------------------------------------------
-- dim_trips
-- Includes MBTA-specific extension columns.
-- Join to dim_routes on (feed_start_date, route_id) to get
-- route_type for bus filtering.
--
-- direction_id: 0 = outbound, 1 = inbound (agency-defined)
-- wheelchair_accessible: 0=no info, 1=accessible, 2=not accessible
-- bikes_allowed: 0=no info, 1=allowed, 2=not allowed
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_trips (
    feed_start_date         DATE          NOT NULL,
    trip_id                 VARCHAR       NOT NULL,
    route_id                VARCHAR       NOT NULL,
    service_id              VARCHAR       NOT NULL, -- FK to dim_calendar / dim_calendar_dates
    trip_headsign           VARCHAR,
    trip_short_name         VARCHAR,
    direction_id            INT,          -- 0=outbound, 1=inbound
    block_id                VARCHAR,
    shape_id                VARCHAR,
    wheelchair_accessible   INT,
    bikes_allowed           INT,
    -- MBTA extensions
    trip_route_type         INT,          -- override route_type at trip level (rarely populated)
    route_pattern_id        VARCHAR,      -- e.g. "1-_-0"
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_trips PRIMARY KEY (feed_start_date, trip_id),
    CONSTRAINT fk_trips_version FOREIGN KEY (feed_start_date)
        REFERENCES dim_static_versions (feed_start_date)
);

COMMENT ON COLUMN dim_trips.service_id IS
    'Links to dim_calendar and dim_calendar_dates to determine active service dates';
COMMENT ON COLUMN dim_trips.trip_route_type IS
    'MBTA extension — trip-level override of route_type; use dim_routes.route_type when null';
COMMENT ON COLUMN dim_trips.route_pattern_id IS
    'MBTA extension — identifies the route pattern/variant, e.g. "1-_-0"';


-- -------------------------------------------------------------
-- dim_stop_times
-- Largest static table. One row per (trip_id, stop_sequence).
-- arrival_seconds and departure_seconds are derived integer
-- columns (seconds since midnight) to handle times > 24:00:00
-- and to make delay arithmetic efficient without string parsing.
--
-- pickup_type / drop_off_type:
--   0  Regularly scheduled
--   1  No pickup/drop off
--   2  Must phone agency
--   3  Must coordinate with driver
--
-- timepoint:
--   0  Approximate (interpolated)
--   1  Exact (timepoint)
--
-- continuous_pickup / continuous_drop_off:
--   0  Continuous — passengers may board/alight anywhere
--   1  No continuous pickup/drop off
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_stop_times (
    feed_start_date         DATE          NOT NULL,
    trip_id                 VARCHAR       NOT NULL,
    stop_sequence           INT           NOT NULL,
    stop_id                 VARCHAR       NOT NULL,
    arrival_time            VARCHAR(8),   -- raw HH:MM:SS string; may exceed 24:00:00
    departure_time          VARCHAR(8),
    arrival_seconds         INT,          -- derived: total seconds since midnight
    departure_seconds       INT,          -- derived: total seconds since midnight
    stop_headsign           VARCHAR,
    pickup_type             INT,
    drop_off_type           INT,
    -- MBTA extensions
    timepoint               INT,          -- 0=approximate, 1=exact
    checkpoint_id           VARCHAR,      -- MBTA internal stop checkpoint code
    continuous_pickup        INT,
    continuous_drop_off      INT,
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_stop_times PRIMARY KEY (feed_start_date, trip_id, stop_sequence),
    CONSTRAINT fk_stop_times_version FOREIGN KEY (feed_start_date)
        REFERENCES dim_static_versions (feed_start_date)
);

COMMENT ON COLUMN dim_stop_times.arrival_time IS
    'Raw GTFS string HH:MM:SS — may be >= 24:00:00 for trips crossing midnight';
COMMENT ON COLUMN dim_stop_times.arrival_seconds IS
    'Derived: total seconds since service-day midnight. Use this for all delay arithmetic.';
COMMENT ON COLUMN dim_stop_times.departure_seconds IS
    'Derived: total seconds since service-day midnight. Use this for all delay arithmetic.';
COMMENT ON COLUMN dim_stop_times.checkpoint_id IS
    'MBTA extension — internal checkpoint code used in operations tracking';
COMMENT ON COLUMN dim_stop_times.timepoint IS
    '0=interpolated time, 1=exact scheduled timepoint';


-- -------------------------------------------------------------
-- dim_calendar
-- One row per service_id defining its weekly pattern and
-- date range. Used with dim_calendar_dates to determine which
-- trips are scheduled on a given service_date.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_calendar (
    feed_start_date         DATE          NOT NULL,
    service_id              VARCHAR       NOT NULL,
    monday                  BOOLEAN       NOT NULL,
    tuesday                 BOOLEAN       NOT NULL,
    wednesday               BOOLEAN       NOT NULL,
    thursday                BOOLEAN       NOT NULL,
    friday                  BOOLEAN       NOT NULL,
    saturday                BOOLEAN       NOT NULL,
    sunday                  BOOLEAN       NOT NULL,
    start_date              DATE          NOT NULL,  -- first date service is active
    end_date                DATE          NOT NULL,  -- last date service is active
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_calendar PRIMARY KEY (feed_start_date, service_id),
    CONSTRAINT fk_calendar_version FOREIGN KEY (feed_start_date)
        REFERENCES dim_static_versions (feed_start_date)
);

COMMENT ON COLUMN dim_calendar.service_id IS
    'Defines the base weekly operating pattern for a set of trips';


-- -------------------------------------------------------------
-- dim_calendar_dates
-- Exception overrides on top of dim_calendar.
-- exception_type = 1 means service IS added on this date
-- (even if the weekday pattern says no).
-- exception_type = 2 means service IS removed on this date
-- (even if the weekday pattern says yes).
--
-- To determine if a service_id runs on a given date:
--   1. Check dim_calendar for the weekday + date range match
--   2. Override with any dim_calendar_dates exception for that date
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_calendar_dates (
    feed_start_date         DATE          NOT NULL,
    service_id              VARCHAR       NOT NULL,
    date                    DATE          NOT NULL,
    exception_type          INT           NOT NULL, -- 1=added, 2=removed
    -- MBTA extension
    holiday_name            VARCHAR,                -- e.g. "Patriots' Day", "Memorial Day"
    ingested_at             TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_calendar_dates PRIMARY KEY (feed_start_date, service_id, date),
    CONSTRAINT fk_calendar_dates_version FOREIGN KEY (feed_start_date)
        REFERENCES dim_static_versions (feed_start_date)
);

COMMENT ON COLUMN dim_calendar_dates.exception_type IS
    '1 = service added on this date, 2 = service removed on this date';
COMMENT ON COLUMN dim_calendar_dates.holiday_name IS
    'MBTA extension — populated for holiday exceptions, e.g. Patriots Day, Memorial Day';


-- =============================================================
-- CLUSTERING KEYS
-- dim_stop_times is the only table large enough to need one.
-- Clustering on feed_start_date first (version filter is always
-- applied), then trip_id (most joins come from fact tables
-- filtering by trip).
-- =============================================================
ALTER TABLE dim_stop_times
    CLUSTER BY (feed_start_date, trip_id);