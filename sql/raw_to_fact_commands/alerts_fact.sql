-- Alert entities in RAW are deduped per day, not across days
-- We only want the most recent snapshot of an alert entity, which is why we use MERGE INTO when updating this fact table.
-- We keep the most recent alert snapshot even when doing backfills of earlier dates with the condition WHEN MATCHED AND source.snapshot_timestamp > target.snapshot_timestamp
-- Used Claude Sonnet 4.6 for help with merging logic

-- Getting the correct static version to add to the static_version_date column
SET static_version_date = (
    SELECT MAX(feed_start_date)
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_STATIC_VERSIONS
    WHERE feed_start_date <= '2026-03-11'
);

MERGE INTO LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS AS target
USING (
    SELECT
        entity_id,
        snapshot_timestamp,
        PARSE_JSON(effect_detail):translation[0]:text::STRING    AS effect_detail,
        PARSE_JSON(cause_detail):translation[0]:text::STRING     AS cause_detail,
        PARSE_JSON(header_text):translation[0]:text::STRING      AS header_text,
        PARSE_JSON(description_text):translation[0]:text::STRING AS description_text,
        PARSE_JSON(url):translation[0]:text::STRING              AS url,
        cause,
        effect,
        severity_level,
        gtfs_realtime_version,
        ingested_at
    FROM LEMMING_DB.FINAL_PROJECT_RAW.RAW_ALERTS
    WHERE service_date = '2026-03-11'
      AND is_deleted   = FALSE
) AS source
ON target.entity_id = source.entity_id
WHEN MATCHED AND source.snapshot_timestamp > target.snapshot_timestamp THEN UPDATE SET
    snapshot_timestamp    = source.snapshot_timestamp,
    cause                 = source.cause,
    effect                = source.effect,
    severity_level        = source.severity_level,
    effect_detail         = source.effect_detail,
    cause_detail          = source.cause_detail,
    header_text           = source.header_text,
    description_text      = source.description_text,
    url                   = source.url,
    ingested_at           = source.ingested_at,
    static_version_date   = $static_version_date
WHEN NOT MATCHED THEN INSERT (
    entity_id, snapshot_timestamp, cause, effect, severity_level,
    effect_detail, cause_detail, header_text, description_text,
    url, gtfs_realtime_version, ingested_at, static_version_date
) VALUES (
    source.entity_id, source.snapshot_timestamp, source.cause, source.effect,
    source.severity_level, source.effect_detail, source.cause_detail,
    source.header_text, source.description_text, source.url,
    source.gtfs_realtime_version, source.ingested_at, $static_version_date
);
