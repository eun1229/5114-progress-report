-- Used for getting on time performance metric by calculating the % of stop events with is_on_time = TRUE
-- Built from:
--   FACT.FACT_VEHICLE_POSITIONS  (we'll get rows where current_status = STOPPED_AT (when vehicles ACTUALLY stop at the stop_id for the trip))
--   STATIC.DIM_STOP_TIMES        (scheduled times (EXPECTED stop times))
--   STATIC.DIM_TRIPS             (needed for direction_id)
--   STATIC.DIM_ROUTES            (needed for route_type and route name)

CREATE TABLE IF NOT EXISTS FINAL_PROJECT_MART.METRIC_ON_TIME (
    service_date                DATE          NOT NULL,
    trip_start_date             DATE,
    hour                        INTEGER,

    -- Trip / route identifiers
    trip_id                     VARCHAR       NOT NULL,
    route_id                    VARCHAR,
    route_name                  VARCHAR,
    route_type                  INTEGER,      -- 3 = bus
    direction_id                INTEGER,

    -- Stop identifiers
    stop_id                     VARCHAR,
    stop_sequence               INTEGER       NOT NULL,    

    -- Earliest STOPPED_AT position_timestamp
    -- for this (trip_id, stop_sequence) on this trip_start_date
    actual_arrival_ts           TIMESTAMP_NTZ,
    actual_arrival_seconds      INTEGER,      -- seconds since midnight on trip_start_date

    -- Scheduled arrival from stop_times
    -- Stored as both the raw string and derived seconds so queries can use whichever is convenient
    scheduled_arrival_time      VARCHAR(8),   -- HH:MM:SS (may exceed 24:00:00)
    scheduled_arrival_seconds   INTEGER,      -- seconds since midnight on service_date

    -- Delay in seconds (positive = late, negative = early)
    -- actual arrival seconds - scheduled arrival seconds
    arrival_delay_seconds       INTEGER,

    -- On-time flag:
    --   on time = arrived no earlier than 2.5 min before scheduled AND no later than 5 min after scheduled
    --   (-150 <= arrival_delay_seconds <= 300)
    -- The reasoning for this flag's boundaries was inspired by WMATA (Washington DC) performance measure definitions
    is_on_time                  BOOLEAN,

    -- Occupancy at the moment of stop
    occupancy_status            VARCHAR,
    occupancy_percentage        INTEGER,

    -- Static version linkage
    static_version_date         DATE          NOT NULL,

    CONSTRAINT pk_on_time PRIMARY KEY (service_date, trip_id, stop_sequence)
)
CLUSTER BY (service_date, route_name); -- aligns with columns for visualization