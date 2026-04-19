-- One row per calendar date × route × effect type. The calendar date comes from the active period. 
-- This metric could help highlight if there are periods of time (i.e. during winter, around public holidays) that 
-- result in more alerts and disruptions than usual

CREATE TABLE IF NOT EXISTS LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY (
    alert_date       DATE,
    route_id         STRING,
    alert_count      INT,
    severe_count     INT,
    warning_count    INT,
    info_count       INT,
    PRIMARY KEY (alert_date, route_id)
);