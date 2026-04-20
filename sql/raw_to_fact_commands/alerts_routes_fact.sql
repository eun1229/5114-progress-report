-- Loads alert informed-entity route/stop mappings from RAW_ALERTS into FACT_ALERTS_ROUTES.
-- One row per unique (entity_id, route_id, stop_id), keeping the newest snapshot when duplicates exist.
-- Merge avoids regression when older backfill dates are rerun after newer data has already loaded.
-- Used Claude Sonnet 4.6 for help with the merging logic.
SET target_service_date = TO_DATE('{{ ds }}');

MERGE INTO FINAL_PROJECT_FACT.FACT_ALERTS_ROUTES AS target
USING (
    SELECT DISTINCT
        entity_id,
        snapshot_timestamp,
        ie.value:route_id::STRING   AS route_id,
        ie.value:stop_id::STRING    AS stop_id,
        ie.value:route_type::INT    AS route_type,
        ie.value:direction_id::INT  AS direction_id,
        ie.value:agency_id::STRING  AS agency_id
    FROM FINAL_PROJECT_RAW.RAW_ALERTS,
    LATERAL FLATTEN(input => PARSE_JSON(informed_entity)) ie
    WHERE service_date = $target_service_date
      AND is_deleted = FALSE
      AND ie.value:route_id::STRING IS NOT NULL
      AND ie.value:stop_id::STRING IS NOT NULL
) src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY entity_id, route_id, stop_id
    ORDER BY snapshot_timestamp DESC
) = 1
ON target.entity_id = src.entity_id
AND target.route_id = src.route_id
AND target.stop_id = src.stop_id
WHEN MATCHED AND src.snapshot_timestamp > target.snapshot_timestamp THEN UPDATE SET
    snapshot_timestamp = src.snapshot_timestamp,
    route_type = src.route_type,
    direction_id = src.direction_id,
    agency_id = src.agency_id
WHEN NOT MATCHED THEN INSERT (
    entity_id,
    snapshot_timestamp,
    route_id,
    stop_id,
    route_type,
    direction_id,
    agency_id
) VALUES (
    src.entity_id,
    src.snapshot_timestamp,
    src.route_id,
    src.stop_id,
    src.route_type,
    src.direction_id,
    src.agency_id
);