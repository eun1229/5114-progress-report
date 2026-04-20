-- Loads flattened alert active periods from RAW_ALERTS into FACT_ALERTS_ACTIVE_PERIODS.
-- Active periods are flattened from the JSON array and keyed by (entity_id, period_index).
-- Merge keeps period updates (for example, end time added later) without regressing to older snapshots.
-- Uses the Airflow execution date as the source slice.
SET target_service_date = TO_DATE('{{ ds }}');

MERGE INTO FINAL_PROJECT_FACT.FACT_ALERTS_ACTIVE_PERIODS AS target
USING (
    SELECT
        entity_id,
        snapshot_timestamp,
        ap.index                              AS period_index,
        TO_TIMESTAMP(ap.value:start::INT)     AS active_start,
        TO_TIMESTAMP(ap.value:end::INT)       AS active_end,
        CASE
            WHEN ap.value:end IS NOT NULL
            THEN DATEDIFF(
                'minute',
                TO_TIMESTAMP(ap.value:start::INT),
                TO_TIMESTAMP(ap.value:end::INT)
            ) / 60.0
        END                                   AS duration_hours,
        (ap.value:end IS NULL)                AS is_open_ended
    FROM FINAL_PROJECT_RAW.RAW_ALERTS,
    LATERAL FLATTEN(input => PARSE_JSON(active_period), outer => TRUE) ap
    WHERE service_date = $target_service_date
      AND is_deleted = FALSE
) src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY entity_id, period_index
    ORDER BY snapshot_timestamp DESC
) = 1
ON target.entity_id = src.entity_id
AND target.period_index = src.period_index
WHEN MATCHED AND src.snapshot_timestamp > target.snapshot_timestamp THEN UPDATE SET
    snapshot_timestamp = src.snapshot_timestamp,
    active_start = src.active_start,
    active_end = src.active_end,
    duration_hours = src.duration_hours,
    is_open_ended = src.is_open_ended
WHEN NOT MATCHED THEN INSERT (
    entity_id,
    snapshot_timestamp,
    period_index,
    active_start,
    active_end,
    duration_hours,
    is_open_ended
) VALUES (
    src.entity_id,
    src.snapshot_timestamp,
    src.period_index,
    src.active_start,
    src.active_end,
    src.duration_hours,
    src.is_open_ended
);