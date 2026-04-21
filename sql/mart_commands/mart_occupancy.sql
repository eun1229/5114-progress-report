-- Builds occupancy metrics by route-hour from fact vehicle positions.
SET target_service_date = TO_DATE('{{ ds }}') - 2;

-- Used Claude Sonnet 4.6 (from the same context as create_mart_occupancy.sql)

-- Idempotency guard
DELETE FROM FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR  WHERE service_date = $target_service_date;

-- Route/hour grain
INSERT INTO FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR
WITH bus_snapshots AS (
    SELECT
        f.service_date,
        f.hour,
        f.route_id,
        r.route_short_name              AS route_name,
        f.occupancy_percentage,
        f.occupancy_status
    FROM FINAL_PROJECT_FACT.FACT_VEHICLE_POSITIONS f
    JOIN FINAL_PROJECT_STATIC.DIM_ROUTES r
      ON  f.route_id            = r.route_id
      AND f.static_version_date = r.feed_start_date
    WHERE f.service_date = $target_service_date
      AND r.route_type   = 3       -- bus only
      AND f.current_stop_sequence <> 1 -- don't include the mass of snapshots given when the bus GPS is on before the trip en route
)
SELECT
    service_date,
    hour,
    route_id,
    MAX(route_name)                                             AS route_name,
    COUNT(*)                                                    AS snapshot_count,


    AVG(occupancy_percentage)                                   AS avg_occupancy_pct,
    ROUND(
        COUNT(occupancy_percentage) / COUNT(*) * 100.0, 2
    )                                                           AS pct_snapshots_reporting,

    -- Status distribution
    ROUND(COUNT_IF(occupancy_status = 'EMPTY')
        / COUNT(*) * 100.0, 2)                                 AS pct_empty,
    ROUND(COUNT_IF(occupancy_status = 'MANY_SEATS_AVAILABLE')
        / COUNT(*) * 100.0, 2)                                 AS pct_many_seats,
    ROUND(COUNT_IF(occupancy_status = 'FEW_SEATS_AVAILABLE')
        / COUNT(*) * 100.0, 2)                                 AS pct_few_seats,
    ROUND(COUNT_IF(occupancy_status = 'STANDING_ROOM_ONLY')
        / COUNT(*) * 100.0, 2)                                 AS pct_standing_room,
    ROUND(COUNT_IF(occupancy_status = 'CRUSHED_STANDING_ROOM_ONLY')
        / COUNT(*) * 100.0, 2)                                 AS pct_crushed_standing,
    ROUND(COUNT_IF(occupancy_status = 'FULL')
        / COUNT(*) * 100.0, 2)                                 AS pct_full,
    ROUND(COUNT_IF(occupancy_status IS NULL)
        / COUNT(*) * 100.0, 2)                                 AS pct_no_data_occupancy

FROM bus_snapshots
GROUP BY service_date, hour, route_id;