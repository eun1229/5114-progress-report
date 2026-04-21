CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_FACT.FACT_TRIP_UPDATES (
    service_date                DATE          NOT NULL,
    hour                        INTEGER,
    snapshot_timestamp          TIMESTAMP_NTZ NOT NULL,
    ingested_at                 TIMESTAMP_NTZ,
    gtfs_realtime_version       VARCHAR,

    entity_id                   VARCHAR       NOT NULL,
    is_deleted                  BOOLEAN,

    trip_id                     VARCHAR       NOT NULL,
    route_id                    VARCHAR,
    direction_id                INTEGER,
    trip_start_time             VARCHAR, -- nullable, aligns with GTFS spec
    trip_start_date             DATE          NOT NULL,
    trip_schedule_rel           VARCHAR,
    modified_trip               VARIANT,

    vehicle_id                  VARCHAR,
    vehicle_label               VARCHAR,
    vehicle_license_plate       VARCHAR,
    wheelchair_accessible       VARCHAR,

    trip_update_timestamp       TIMESTAMP_NTZ,
    delay                       INTEGER,

    trip_properties             VARIANT,

    static_version_date         DATE          NOT NULL,

    CONSTRAINT pk_fact_trip_updates
        PRIMARY KEY (trip_start_date, trip_id)
)
CLUSTER BY (service_date, route_id);

