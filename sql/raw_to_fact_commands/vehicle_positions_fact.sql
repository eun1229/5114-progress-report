-- Loads curated FACT_VEHICLE_POSITIONS rows from RAW_VEHICLE_POSITIONS for a target service date.
-- A pipeline run triggered on day X at midnight will load FACT rows for trips with start date X-2 for completeness.
SET target_service_date = TO_DATE('{{ ds }}') - 2;

-- Idempotency guard
DELETE FROM FINAL_PROJECT_FACT.FACT_VEHICLE_POSITIONS
WHERE trip_start_date = $target_service_date;


-- Getting the correct static version to add to the static_version_date column
SET static_version_date = (
    SELECT MAX(feed_start_date)
    FROM FINAL_PROJECT_STATIC.DIM_STATIC_VERSIONS
    WHERE feed_start_date <= $target_service_date
);

-- Load data from raw to fact with the static version column addition
INSERT INTO FINAL_PROJECT_FACT.FACT_VEHICLE_POSITIONS (
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
    vehicle_id,
    vehicle_label,
    multi_carriage_details,
    latitude,
    longitude,
    bearing,
    odometer,
    speed,
    current_stop_sequence,
    stop_id,
    current_status,
    congestion_level,
    occupancy_status,
    occupancy_percentage,
    position_timestamp,
    static_version_date
)
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
    r.vehicle_id,
    r.vehicle_label,
    r.multi_carriage_details,
    r.latitude,
    r.longitude,
    r.bearing,
    r.odometer,
    r.speed,
    r.current_stop_sequence,
    r.stop_id,
    r.current_status,
    r.congestion_level,
    r.occupancy_status,
    r.occupancy_percentage,
    r.position_timestamp,
    $static_version_date        AS static_version_date

FROM FINAL_PROJECT_RAW.RAW_VEHICLE_POSITIONS r

WHERE r.trip_start_date = $target_service_date
  -- is_deleted=True means the agency providing the data reccomends this entity to be deleted.
  -- We will not be including entities with is_deleted=True in our metrics.
  AND (r.is_deleted IS NULL OR r.is_deleted = FALSE)
  -- Include rows from the target service_date plus overnight continuations
  -- (next calendar day, early-morning hours 0-5) so late trips that spill past midnight
  -- are captured in the same fact run as the trip they belong to.
  -- also guards against erroneous gps readings where vehicle says it started at a different day mid-day
  AND (
        r.service_date = $target_service_date
     OR (r.service_date = DATEADD(day, 1, $target_service_date) AND r.hour BETWEEN 0 AND 5)
  )