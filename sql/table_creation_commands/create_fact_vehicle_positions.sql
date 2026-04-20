-- Mainly the same as raw_vehicle_positions, but with the addition of the static_version_date
-- for linking the correct static version data

CREATE TABLE IF NOT EXISTS FINAL_PROJECT_FACT.FACT_VEHICLE_POSITIONS (
    -- Partitioning / ingestion metadata
    service_date                DATE          NOT NULL,
    hour                        INTEGER,
    snapshot_timestamp          TIMESTAMP_NTZ,
    ingested_at                 TIMESTAMP_NTZ,
    gtfs_realtime_version       VARCHAR,

    -- Entity level
    entity_id                   VARCHAR,
    is_deleted                  BOOLEAN,

    -- Trip descriptor
    trip_id                     VARCHAR,
    route_id                    VARCHAR,
    direction_id                INTEGER,
    trip_start_time             VARCHAR,        -- HH:MM:SS string from GTFS
    trip_start_date             DATE,
    trip_schedule_rel           VARCHAR,

    -- Vehicle descriptor
    vehicle_id                  VARCHAR,
    vehicle_label               VARCHAR,

    -- Multi-carriage details (light rail only)
    multi_carriage_details      VARIANT,

    -- Position
    latitude                    FLOAT,
    longitude                   FLOAT,
    bearing                     FLOAT,
    odometer                    FLOAT,
    speed                       FLOAT,

    -- Stop state
    current_stop_sequence       INTEGER,
    stop_id                     VARCHAR,
    current_status              VARCHAR,
    congestion_level            VARCHAR,
    occupancy_status            VARCHAR,
    occupancy_percentage        INTEGER,

    -- GPS fix time (distinct from snapshot_timestamp)
    position_timestamp          TIMESTAMP_NTZ,

    -- MAX(FINAL_PROJECT_STATIC.dim_static_versions.feed_start_date) WHERE FINAL_PROJECT_STATIC.dim_static_versions.feed_start_date <= service_date
    -- to link the correct static version
    static_version_date         DATE          NOT NULL,

    CONSTRAINT pk_fact_vehicle_positions PRIMARY KEY (service_date, entity_id, snapshot_timestamp)
)
CLUSTER BY (service_date, route_id);
