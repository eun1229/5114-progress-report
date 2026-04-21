-- trip_start_date being processed = ds - 2. This lags the Spark RT offset
-- (ds - 1) by one extra day so RAW has landed snapshots collected both on
-- the trip's own day AND the following day, which can carry late-finalizing
-- updates for overnight trips or status changes published after the fact.

SET target_service_date = TO_DATE('{{ ds }}') - 2;

SET static_version_date = (
    SELECT MAX(feed_start_date)
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_STATIC_VERSIONS
    WHERE feed_start_date <= $target_service_date
);

DELETE FROM LEMMING_DB.FINAL_PROJECT_FACT.FACT_TRIP_UPDATES
WHERE trip_start_date = $target_service_date;

INSERT INTO LEMMING_DB.FINAL_PROJECT_FACT.FACT_TRIP_UPDATES (
    service_date,
    hour,
    snapshot_timestamp,
    ingested_at,
    gtfs_realtime_version,
    entity_id,
    is_deleted,
    trip_id,
    route_id,
    direction_id,
    trip_start_time,
    trip_start_date,
    trip_schedule_rel,
    modified_trip,
    vehicle_id,
    vehicle_label,
    vehicle_license_plate,
    wheelchair_accessible,
    trip_update_timestamp,
    delay,
    trip_properties,
    static_version_date
)
WITH ranked_trip_updates AS (
    SELECT
        r.service_date,
        r.hour,
        r.snapshot_timestamp,
        r.ingested_at,
        r.gtfs_realtime_version,
        r.entity_id,
        r.is_deleted,
        r.trip_id,
        r.route_id,
        r.direction_id,
        r.trip_start_time,
        r.trip_start_date,
        r.trip_schedule_rel,
        r.modified_trip,
        r.vehicle_id,
        r.vehicle_label,
        r.vehicle_license_plate,
        r.wheelchair_accessible,
        r.trip_update_timestamp,
        r.delay,
        r.trip_properties,
        ROW_NUMBER() OVER (
            PARTITION BY r.trip_id
            ORDER BY
                r.snapshot_timestamp DESC,
                r.ingested_at DESC,
                r.entity_id DESC
        ) AS rn
    FROM LEMMING_DB.FINAL_PROJECT_RAW.RAW_TRIP_UPDATES r
    WHERE r.trip_start_date = $target_service_date
      -- Allow snapshots collected on the trip's own day OR the day after.
      -- The extra day pairs with the -2 offset: we wait one extra day so
      -- late-finalizing updates (overnight trips, late CANCELED/ADDED
      -- announcements) are available, without letting multi-day-stale
      -- lingering entries from the feed pollute the "most recent" pick.
      AND r.service_date BETWEEN $target_service_date
                             AND DATEADD(DAY, 1, $target_service_date)
      AND r.trip_id IS NOT NULL
      AND (r.is_deleted IS NULL OR r.is_deleted = FALSE)
)
SELECT
    service_date,
    hour,
    snapshot_timestamp,
    ingested_at,
    gtfs_realtime_version,
    entity_id,
    is_deleted,
    trip_id,
    route_id,
    direction_id,
    trip_start_time,
    trip_start_date,
    trip_schedule_rel,
    modified_trip,
    vehicle_id,
    vehicle_label,
    vehicle_license_plate,
    wheelchair_accessible,
    trip_update_timestamp,
    delay,
    trip_properties,
    $static_version_date AS static_version_date
FROM ranked_trip_updates
WHERE rn = 1;