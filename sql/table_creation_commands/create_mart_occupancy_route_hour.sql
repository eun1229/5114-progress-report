-- Route/hour grain for occupancy metrics.
CREATE TABLE IF NOT EXISTS FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR (
    service_date                DATE          NOT NULL,
    hour                        INTEGER       NOT NULL,
    route_id                    VARCHAR       NOT NULL,
    route_short_name            VARCHAR,

    -- Snapshot count used in the average (denominator)
    snapshot_count              INTEGER,

    -- Percentage-based occupancy (where reported)
    avg_occupancy_pct           FLOAT,
    pct_snapshots_reporting     FLOAT,

    -- Status-based occupancy distribution
    pct_empty                   FLOAT,
    pct_many_seats              FLOAT,
    pct_few_seats               FLOAT,
    pct_standing_room           FLOAT,
    pct_crushed_standing         FLOAT,
    pct_full                    FLOAT,
    pct_no_data_occupancy       FLOAT,

    CONSTRAINT pk_occ_route_hour PRIMARY KEY (service_date, hour, route_id)
)
CLUSTER BY (service_date);
