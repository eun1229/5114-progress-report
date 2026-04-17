-- Used '2026-03-10' as the service date for testing purposes
-- Would need to replace instances of '2026-03-10' and use jinja templating when using Airflow

-- Used Claude Sonnet 4.6 (from the same context as create_mart_occupancy.sql)

-- Idempotency guard
DELETE FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR  WHERE service_date = '2026-03-10';
DELETE FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY   WHERE service_date = '2026-03-10';
DELETE FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_OVERALL_DAY WHERE service_date = '2026-03-10';

-- Route/hour grain
INSERT INTO LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR
WITH bus_snapshots AS (
    SELECT
        f.service_date,
        f.hour,
        f.route_id,
        r.route_short_name,
        f.occupancy_percentage,
        f.occupancy_status
    FROM LEMMING_DB.FINAL_PROJECT_FACT.FACT_VEHICLE_POSITIONS f
    JOIN LEMMING_DB.FINAL_PROJECT_STATIC.DIM_ROUTES r
      ON  f.route_id            = r.route_id
      AND f.static_version_date = r.feed_start_date
    WHERE f.service_date = '2026-03-10'
      AND r.route_type   = 3       -- bus only
      AND f.current_stop_sequence <> 1 -- don't include the mass of snapshots given when the bus GPS is on before the trip en route
)
SELECT
    service_date,
    hour,
    route_id,
    MAX(route_short_name)                                       AS route_short_name,
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


-- Route/day grain — aggregate from the hour grain
INSERT INTO LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
SELECT
    service_date,
    route_id,
    MAX(route_short_name)                                       AS route_short_name,
    SUM(snapshot_count)                                         AS snapshot_count,

    -- Weighted average across hours (weight by snapshot_count per hour)
    SUM(avg_occupancy_pct * pct_snapshots_reporting / 100.0 * snapshot_count)
        / NULLIF(SUM(pct_snapshots_reporting / 100.0 * snapshot_count), 0)
                                                                AS avg_occupancy_pct,
    SUM(pct_snapshots_reporting * snapshot_count)
        / NULLIF(SUM(snapshot_count), 0)                       AS pct_snapshots_reporting,
    SUM(pct_empty            * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_empty,
    SUM(pct_many_seats       * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_many_seats,
    SUM(pct_few_seats        * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_few_seats,
    SUM(pct_standing_room    * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_standing_room,
    SUM(pct_crushed_standing * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_crushed_standing,
    SUM(pct_full             * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_full,
    SUM(pct_no_data_occupancy * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_no_data_occupancy

FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR
WHERE service_date = '2026-03-10'
GROUP BY service_date, route_id;


-- Network wide day grain — aggregate all bus routes
INSERT INTO LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_OVERALL_DAY
SELECT
    service_date,
    SUM(snapshot_count)                                         AS snapshot_count,
    SUM(avg_occupancy_pct * pct_snapshots_reporting / 100.0 * snapshot_count)
        / NULLIF(SUM(pct_snapshots_reporting / 100.0 * snapshot_count), 0)
                                                                AS avg_occupancy_pct,
    SUM(pct_snapshots_reporting * snapshot_count)
        / NULLIF(SUM(snapshot_count), 0)                       AS pct_snapshots_reporting,
    SUM(pct_empty            * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_empty,
    SUM(pct_many_seats       * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_many_seats,
    SUM(pct_few_seats        * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_few_seats,
    SUM(pct_standing_room    * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_standing_room,
    SUM(pct_crushed_standing * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_crushed_standing,
    SUM(pct_full             * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_full,
    SUM(pct_no_data_occupancy * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_no_data_occupancy

FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
WHERE service_date = '2026-03-10'
GROUP BY service_date;