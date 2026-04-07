-- Used for getting on time performance metric (as well as predicted vs actual arrival, if we decide to include this metric)
-- Built from:
--   FACT.FACT_VEHICLE_POSITIONS  (we'll get rows where current_status = STOPPED_AT (when vehicles actually stop at stop_sequence for the route))
--   STATIC.DIM_STOP_TIMES        (scheduled times (expected stop times))
--   STATIC.DIM_TRIPS             (needed for direction_id)
--   STATIC.DIM_ROUTES            (needed for route_type)

CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_MART.STOP_EVENTS (
    service_date                DATE          NOT NULL,

    -- Trip / route identifiers
    trip_id                     VARCHAR       NOT NULL,
    route_id                    VARCHAR,
    route_type                  INTEGER,      -- 3 = bus
    direction_id                INTEGER,

    -- Stop identifiers
    stop_id                     VARCHAR,
    stop_sequence               INTEGER       NOT NULL,
    stop_name                   VARCHAR,      

    -- Vehicle
    vehicle_id                  VARCHAR,
    vehicle_label               VARCHAR,

    -- Earliest STOPPED_AT position_timestamp
    -- for this (trip_id, stop_sequence) on this service_date
    actual_arrival_ts           TIMESTAMP_NTZ,

    -- Scheduled arrival from stop_times
    -- Stored as both the raw string and derived seconds so queries can use whichever is convenient
    scheduled_arrival_time      VARCHAR(8),   -- HH:MM:SS (may exceed 24:00:00)
    scheduled_arrival_seconds   INTEGER,      -- seconds since midnight on service_date

    -- Delay in seconds (positive = late, negative = early)
    -- actual arrival seconds - scheduled arrival seconds
    arrival_delay_seconds       INTEGER,

    -- On-time flag:
    --   on time = arrived no earlier than 2 min before scheduled AND no later than 4 min after scheduled
    --   (-120 <= arrival_delay_seconds <= 240)
    -- The reasoning for this flag's boundaries was inspired by WMATA (Washington DC) performance measure definitions
    is_on_time                  BOOLEAN,

    -- Occupancy at the moment of stop
    occupancy_status            VARCHAR,
    occupancy_percentage        INTEGER,

    -- Static version linkage
    static_version_date         DATE          NOT NULL,

    CONSTRAINT pk_stop_events PRIMARY KEY (service_date, trip_id, stop_sequence)
)
CLUSTER BY (service_date, route_id);