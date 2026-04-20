-- Creates FACT_ALERTS_ACTIVE_PERIODS table for alert active time windows.
CREATE TABLE IF NOT EXISTS FINAL_PROJECT_FACT.FACT_ALERTS_ACTIVE_PERIODS (
    entity_id      STRING        NOT NULL,
    snapshot_timestamp   TIMESTAMP_NTZ NOT NULL,
    period_index   INT           NOT NULL,
    active_start   TIMESTAMP_NTZ NOT NULL,
    active_end     TIMESTAMP_NTZ,          -- NULL = open-ended / still active
    duration_hours FLOAT,                  -- NULL when open-ended
    is_open_ended  BOOLEAN       NOT NULL,
    PRIMARY KEY (entity_id, period_index)
);