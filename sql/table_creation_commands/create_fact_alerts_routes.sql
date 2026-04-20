-- This table explodes the informed_entities array for each alert entity
-- so that there's one row per unique entity_id, route_id combination.
CREATE TABLE IF NOT EXISTS FINAL_PROJECT_FACT.FACT_ALERTS_ROUTES (
    entity_id    STRING  NOT NULL,
    snapshot_timestamp   TIMESTAMP_NTZ NOT NULL,
    route_id     STRING,
    stop_id      STRING,
    route_type   INT,
    direction_id INT,
    agency_id    STRING,
    PRIMARY KEY (entity_id, route_id, stop_id)
);