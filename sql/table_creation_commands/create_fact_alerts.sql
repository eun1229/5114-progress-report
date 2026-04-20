-- This table only contains info about the alert itself, not the affected entities or its duration

CREATE TABLE IF NOT EXISTS FINAL_PROJECT_FACT.FACT_ALERTS (
    entity_id            STRING        NOT NULL,
    snapshot_timestamp   TIMESTAMP_NTZ NOT NULL,
    cause                STRING,
    effect               STRING,
    severity_level       STRING,
    effect_detail        STRING,
    cause_detail         STRING,
    header_text          STRING,
    description_text     STRING,
    url                  STRING,
    gtfs_realtime_version STRING,
    ingested_at          TIMESTAMP_NTZ,
    static_version_date  DATE          NOT NULL,
    PRIMARY KEY (entity_id)
);