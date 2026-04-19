-- Builds stop event records with scheduled vs actual arrivals for on-time performance analysis.
SET target_service_date = TO_DATE('{{ ds }}');

-- Used Claude Sonnet 4.6 for the QUALIFY and DATEDIFF statements and for aid with join logic.

-- Idempotency guard
DELETE FROM LEMMING_DB.FINAL_PROJECT_MART.STOP_EVENTS
WHERE trip_start_date = $target_service_date;


INSERT INTO LEMMING_DB.FINAL_PROJECT_MART.STOP_EVENTS (
    service_date,
    trip_start_date,
    trip_id,
    route_id,
    route_type,
    direction_id,
    stop_id,
    stop_sequence,
    stop_name,
    vehicle_id,
    vehicle_label,
    actual_arrival_ts,
    scheduled_arrival_time,
    scheduled_arrival_seconds,
    arrival_delay_seconds,
    is_on_time,
    occupancy_status,
    occupancy_percentage,
    static_version_date
)
WITH stopped_at AS (
    SELECT
        f.service_date,
        f.trip_start_date,
        f.trip_id,
        f.route_id,
        f.direction_id,
        f.stop_id,
        f.current_stop_sequence         AS stop_sequence,
        f.vehicle_id,
        f.vehicle_label,
        f.position_timestamp            AS actual_arrival_ts,
        f.occupancy_status,
        f.occupancy_percentage,
        f.static_version_date,

        -- Actual seconds since trip-day midnight
        -- Comparable to dim_stop_times.arrival_seconds
        DATEDIFF(
            'second',
            f.trip_start_date::TIMESTAMP_NTZ,
            f.position_timestamp
        )                               AS actual_arrival_seconds

    FROM LEMMING_DB.FINAL_PROJECT_FACT.FACT_VEHICLE_POSITIONS f

        WHERE f.trip_start_date = $target_service_date
      AND f.current_status = 'STOPPED_AT'

    -- QUALIFY reduces to the earliest snapshot per (trip_id, stop_sequence)
    -- i.e. the actual arrival moment at each stop.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY f.trip_start_date, f.trip_id, f.current_stop_sequence
        ORDER BY f.position_timestamp ASC
    ) = 1
),

-- Join to static dims — all version-keyed on static_version_date
enriched AS (
    SELECT
        s.service_date,
        s.trip_start_date,
        s.trip_id,
        s.route_id,
        r.route_type,
        s.direction_id,
        s.stop_id,
        s.stop_sequence,
        st_dim.stop_name,
        s.vehicle_id,
        s.vehicle_label,
        s.actual_arrival_ts,
        st.arrival_time                 AS scheduled_arrival_time,
        st.arrival_seconds              AS scheduled_arrival_seconds,

        -- Delay: positive = late, negative = early
        (s.actual_arrival_seconds - st.arrival_seconds)
                                        AS arrival_delay_seconds,

        -- On-time: within [-150, +300] seconds of scheduled arrival, as described in table creation command
        IFF(
            (s.actual_arrival_seconds - st.arrival_seconds) BETWEEN -150 AND 300,
            TRUE, FALSE
        )                               AS is_on_time,

        s.occupancy_status,
        s.occupancy_percentage,
        s.static_version_date

    FROM stopped_at s

    -- Scheduled stop time
    JOIN LEMMING_DB.FINAL_PROJECT_STATIC.DIM_STOP_TIMES st
      ON  s.trip_id             = st.trip_id
      AND s.stop_sequence       = st.stop_sequence
      AND s.static_version_date = st.feed_start_date

    -- Stop name 
    JOIN LEMMING_DB.FINAL_PROJECT_STATIC.DIM_STOPS st_dim
      ON  s.stop_id             = st_dim.stop_id
      AND s.static_version_date = st_dim.feed_start_date

    -- Route type, used for bus filter below
    JOIN LEMMING_DB.FINAL_PROJECT_STATIC.DIM_ROUTES r
      ON  s.route_id            = r.route_id
      AND s.static_version_date = r.feed_start_date

    -- Bus only (route_type = 3)
    -- and where stop_sequence != 1 (buses are stopped at stop_sequence = 1 and start giving their GPS 
    -- data way before the start of their trip at stop_sequence = 1)
    WHERE r.route_type = 3
    AND s.stop_sequence <> 1
)

SELECT
    service_date,
    trip_start_date,
    trip_id,
    route_id,
    route_type,
    direction_id,
    stop_id,
    stop_sequence,
    stop_name,
    vehicle_id,
    vehicle_label,
    actual_arrival_ts,
    scheduled_arrival_time,
    scheduled_arrival_seconds,
    arrival_delay_seconds,
    is_on_time,
    occupancy_status,
    occupancy_percentage,
    static_version_date
FROM enriched;

