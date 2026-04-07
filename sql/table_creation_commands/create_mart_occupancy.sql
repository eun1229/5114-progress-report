-- Used for getting occupancy metrics. This metric should be filterable by route_id, service_date, and hour
-- Grain choices:
--   route_hour  — one row per (service_date, route_id, hour)
--   route_day   — one row per (service_date, route_id)
--                 daily rollup, derived from route_hour
--   overall_day — one row per service_date across all bus routes
--
-- All three grains are written to separate tables so the
-- dashboard can query the right grain without re-aggregating

-- Used Claude Sonnet 4.6 with manual modifications to generate the query based on the vehicle_positions table and the above context 

-- Route/hour grain
CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR (
    service_date                DATE          NOT NULL,
    hour                        INTEGER       NOT NULL,
    route_id                    VARCHAR       NOT NULL,
    route_short_name            VARCHAR,

    -- Snapshot count used in the average (denominator)
    snapshot_count              INTEGER,

    -- Percentage-based occupancy (where reported)
    avg_occupancy_pct           FLOAT,        -- NULL if no vehicles reported pct
    pct_snapshots_reporting     FLOAT,        -- % of snapshots that had a pct value

    -- Status-based occupancy distribution
    -- Each column is the % of snapshots in that status bucket
    pct_empty                   FLOAT,
    pct_many_seats              FLOAT,
    pct_few_seats               FLOAT,
    pct_standing_room           FLOAT,
    pct_crushed_standing         FLOAT,
    pct_full                    FLOAT,
    pct_no_data_occupancy       FLOAT,        -- snapshots with NULL occupancy_status

    CONSTRAINT pk_occ_route_hour PRIMARY KEY (service_date, hour, route_id)
)
CLUSTER BY (service_date);

-- Route/day grain (daily rollup)
CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY (
    service_date                DATE          NOT NULL,
    route_id                    VARCHAR       NOT NULL,
    route_short_name            VARCHAR,
    snapshot_count              INTEGER,
    avg_occupancy_pct           FLOAT,
    pct_snapshots_reporting     FLOAT,
    pct_empty                   FLOAT,
    pct_many_seats              FLOAT,
    pct_few_seats               FLOAT,
    pct_standing_room           FLOAT,
    pct_crushed_standing         FLOAT,
    pct_full                    FLOAT,
    pct_no_data_occupancy       FLOAT,

    CONSTRAINT pk_occ_route_day PRIMARY KEY (service_date, route_id)
)
CLUSTER BY (service_date);

-- Daily rollup (all bus routes combined)
CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_OVERALL_DAY (
    service_date                DATE          NOT NULL,
    snapshot_count              INTEGER,
    avg_occupancy_pct           FLOAT,
    pct_snapshots_reporting     FLOAT,
    pct_empty                   FLOAT,
    pct_many_seats              FLOAT,
    pct_few_seats               FLOAT,
    pct_standing_room           FLOAT,
    pct_crushed_standing         FLOAT,
    pct_full                    FLOAT,
    pct_no_data_occupancy       FLOAT,

    CONSTRAINT pk_occ_overall_day PRIMARY KEY (service_date)
);