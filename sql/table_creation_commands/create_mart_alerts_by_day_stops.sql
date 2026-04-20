-- included stop latitude and longitude because this metric would be best viewed as a map view

-- using stop name instead of the stop id here. There are some stops with multiple stop ids for different parts of the stop (i.e. stairs, elevator, although this mainly applies to light rail stations). Grouping by stop_name would be more helpful for identifying problematic stops overall, rather than a specific mechanical part of a stop.

CREATE TABLE IF NOT EXISTS FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY_STOPS (
    alert_date       DATE,
    stop_name        STRING,
    stop_lat         FLOAT,
    stop_lon         FLOAT,
    alert_count      INT,
    severe_count     INT,
    warning_count    INT,
    info_count       INT,
    PRIMARY KEY (alert_date, stop_name)
);