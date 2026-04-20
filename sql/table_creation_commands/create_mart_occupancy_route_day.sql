-- Route/day grain for occupancy metrics.
CREATE TABLE IF NOT EXISTS FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY (
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
