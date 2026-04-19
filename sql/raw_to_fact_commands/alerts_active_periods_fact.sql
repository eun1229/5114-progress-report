-- replace '2026-03-11' with :service date!!

MERGE INTO LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS_ACTIVE_PERIODS AS target
USING (
    SELECT
        entity_id,
        snapshot_timestamp,
        ap.index                              AS period_index,
        TO_TIMESTAMP(ap.value:start::INT)     AS active_start,
        TO_TIMESTAMP(ap.value:end::INT)       AS active_end,
        CASE
            WHEN ap.value:end IS NOT NULL
            THEN DATEDIFF('minute',
                     TO_TIMESTAMP(ap.value:start::INT),
                     TO_TIMESTAMP(ap.value:end::INT)
                 ) / 60.0
        END                                   AS duration_hours,
        (ap.value:end IS NULL)                AS is_open_ended
    FROM LEMMING_DB.FINAL_PROJECT_RAW.RAW_ALERTS,
    LATERAL FLATTEN(input => PARSE_JSON(active_period), outer => TRUE) ap
    WHERE service_date = '2026-03-11'
      AND is_deleted   = FALSE
) AS source
ON  target.entity_id    = source.entity_id
AND target.period_index = source.period_index
WHEN MATCHED AND source.snapshot_timestamp > target.snapshot_timestamp THEN UPDATE SET   -- periods can gain an end time as alert resolves
    active_end     = source.active_end,
    duration_hours = source.duration_hours,
    is_open_ended  = source.is_open_ended,
    snapshot_timestamp = source.snapshot_timestamp
WHEN NOT MATCHED THEN INSERT (
    entity_id, snapshot_timestamp, period_index, active_start, active_end, duration_hours, is_open_ended
) VALUES (
    source.entity_id, source.snapshot_timestamp, source.period_index, source.active_start,
    source.active_end, source.duration_hours, source.is_open_ended
);