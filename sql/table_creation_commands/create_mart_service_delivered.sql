CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_MART.METRIC_SERVICE_DELIVERED (
    service_date                DATE          NOT NULL,
    trip_start_date             DATE          NOT NULL,
    route_id                    VARCHAR       NOT NULL,
    route_name                  VARCHAR,
    direction_id                INTEGER,
 
    scheduled_trips             INTEGER,      -- denominator: planned trips for the day
    delivered_trips             INTEGER,      -- ran as scheduled (no CANCELED)
    canceled_trips              INTEGER,      -- explicitly CANCELED in RT feed
    no_rt_data_trips            INTEGER,      -- scheduled but never appeared in RT feed
    added_trips                 INTEGER,      -- ADDED in RT, not in static (excluded from pct)
 
    pct_delivered               FLOAT,        -- delivered_trips / scheduled_trips * 100
 
    static_version_date         DATE,

    PRIMARY KEY (trip_start_date, route_id, direction_id)
)
CLUSTER BY (service_date, route_id);