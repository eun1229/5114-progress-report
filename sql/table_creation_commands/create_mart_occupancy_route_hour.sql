-- Used for getting occupancy metrics (avg occupancy of buses by route and hour, can be rolled up by day or just by hour in the visualization)
-- This uses vehicle entities that are both in motion to and stopped at a stop, so it's representation of the actual occupancy of the bus throughout the trip, not just at a stop.
-- Built from:
--   FACT.FACT_VEHICLE_POSITIONS  (we'll get rows where current_status = STOPPED_AT (when vehicles ACTUALLY stop at the stop_id for the trip))
--   STATIC.DIM_ROUTES            (needed for route_type and route_name)

-- Used Claude Sonnet 4.6 with manual modifications to generate the query based on the vehicle_positions table and the above context 

CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR (
    service_date                DATE          NOT NULL,
    hour                        INTEGER       NOT NULL,
    route_id                    VARCHAR       NOT NULL,
    route_name                  VARCHAR,

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
    pct_crushed_standing        FLOAT,
    pct_full                    FLOAT,
    pct_no_data_occupancy       FLOAT,        -- snapshots with NULL occupancy_status

    PRIMARY KEY (service_date, hour, route_id)
);