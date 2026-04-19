-- Used Claude Sonnet 4.6 for help with merging logic
-- replace '2026-03-11' with :service_date!!!!

MERGE INTO LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS_ROUTES AS target
USING (
    SELECT DISTINCT
        entity_id,
        snapshot_timestamp,
        ie.value:route_id::STRING   AS route_id,
        ie.value:stop_id::STRING    AS stop_id,
        ie.value:route_type::INT    AS route_type,
        ie.value:direction_id::INT  AS direction_id,
        ie.value:agency_id::STRING  AS agency_id,
    FROM LEMMING_DB.FINAL_PROJECT_RAW.RAW_ALERTS,
    LATERAL FLATTEN(input => PARSE_JSON(informed_entity)) ie
    WHERE service_date = '2026-03-11'
      AND is_deleted   = FALSE
      AND ie.value:route_id::STRING is not null
      AND ie.value:stop_id::STRING is not null
) AS source
ON  target.entity_id = source.entity_id
AND target.route_id  = source.route_id
AND target.stop_id   = source.stop_id
WHEN NOT MATCHED THEN INSERT (
    entity_id, snapshot_timestamp, route_id, stop_id, route_type, direction_id, agency_id
) VALUES (
    source.entity_id, source.snapshot_timestamp, source.route_id, source.stop_id,
    source.route_type, source.direction_id, source.agency_id
);