SET trip_start_date = TO_DATE('{{ ds }}') - 2;

SET static_version_date = (
    SELECT MAX(feed_start_date)
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_STATIC_VERSIONS
    WHERE feed_start_date <= $trip_start_date
);

DELETE FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_SERVICE_DELIVERED
WHERE trip_start_date = $trip_start_date;

INSERT INTO LEMMING_DB.FINAL_PROJECT_MART.METRIC_SERVICE_DELIVERED (
    service_date,
    trip_start_date,
    route_id,
    route_name,
    direction_id,
    scheduled_trips,
    delivered_trips,
    canceled_trips,
    no_rt_data_trips,
    added_trips,
    pct_delivered,
    static_version_date
)
WITH weekday_service_ids AS (
    SELECT
        c.feed_start_date,
        c.service_id
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_CALENDAR c
    WHERE c.feed_start_date = $static_version_date
      AND $trip_start_date BETWEEN c.start_date AND c.end_date
      AND CASE DAYOFWEEK($trip_start_date)
            WHEN 1 THEN c.sunday
            WHEN 2 THEN c.monday
            WHEN 3 THEN c.tuesday
            WHEN 4 THEN c.wednesday
            WHEN 5 THEN c.thursday
            WHEN 6 THEN c.friday
            WHEN 7 THEN c.saturday
          END = TRUE
),

added_service_ids AS (
    SELECT
        cd.feed_start_date,
        cd.service_id
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_CALENDAR_DATES cd
    WHERE cd.feed_start_date = $static_version_date
      AND cd.date = $trip_start_date
      AND cd.exception_type = 1
),

removed_service_ids AS (
    SELECT
        cd.feed_start_date,
        cd.service_id
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_CALENDAR_DATES cd
    WHERE cd.feed_start_date = $static_version_date
      AND cd.date = $trip_start_date
      AND cd.exception_type = 2
),

active_service_ids AS (
    SELECT feed_start_date, service_id
    FROM weekday_service_ids

    UNION

    SELECT feed_start_date, service_id
    FROM added_service_ids
),

final_service_ids AS (
    SELECT a.feed_start_date, a.service_id
    FROM active_service_ids a
    LEFT JOIN removed_service_ids r
      ON a.feed_start_date = r.feed_start_date
     AND a.service_id = r.service_id
    WHERE r.service_id IS NULL
),

first_stop_times AS (
    SELECT
        st.feed_start_date,
        st.trip_id,
        st.departure_time,
        ROW_NUMBER() OVER (
            PARTITION BY st.feed_start_date, st.trip_id
            ORDER BY st.stop_sequence
        ) AS rn
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_STOP_TIMES st
    WHERE st.feed_start_date = $static_version_date
      AND st.departure_time IS NOT NULL
),

scheduled_bus_trips AS (
    SELECT
        $trip_start_date AS service_date,
        $trip_start_date AS trip_start_date,
        t.trip_id,
        t.route_id,
        r.route_short_name AS route_name,
        t.direction_id,
        $static_version_date AS static_version_date
    FROM LEMMING_DB.FINAL_PROJECT_STATIC.DIM_TRIPS t
    JOIN final_service_ids fs
      ON t.feed_start_date = fs.feed_start_date
     AND t.service_id = fs.service_id
    JOIN LEMMING_DB.FINAL_PROJECT_STATIC.DIM_ROUTES r
      ON t.feed_start_date = r.feed_start_date
     AND t.route_id = r.route_id
    JOIN first_stop_times fst
      ON t.feed_start_date = fst.feed_start_date
     AND t.trip_id = fst.trip_id
     AND fst.rn = 1
    WHERE r.route_type = 3
      AND TRY_TO_NUMBER(SPLIT_PART(fst.departure_time, ':', 1)) < 24
),

rt_trips AS (
    SELECT
        f.trip_id,
        f.trip_start_date,
        f.route_id,
        f.direction_id,
        COALESCE(NULLIF(TRIM(f.trip_schedule_rel), ''), 'SCHEDULED') AS rt_schedule_rel
    FROM LEMMING_DB.FINAL_PROJECT_FACT.FACT_TRIP_UPDATES f
    WHERE f.trip_start_date = $trip_start_date
      AND f.static_version_date = $static_version_date
),

joined AS (
    SELECT
        s.service_date,
        s.trip_start_date,
        s.route_id,
        s.route_name,
        s.direction_id,
        s.trip_id,
        s.static_version_date,
        rt.rt_schedule_rel,
        CASE
            WHEN rt.trip_id IS NULL THEN 'NO_RT_DATA'
            WHEN rt.rt_schedule_rel = 'CANCELED' THEN 'CANCELED'
            WHEN rt.rt_schedule_rel = 'ADDED' THEN 'ADDED'
            ELSE 'DELIVERED'
        END AS delivery_status
    FROM scheduled_bus_trips s
    LEFT JOIN rt_trips rt
      ON s.trip_id = rt.trip_id
     AND s.trip_start_date = rt.trip_start_date
)

SELECT
    service_date,
    trip_start_date,
    route_id,
    MAX(route_name) AS route_name,
    direction_id,

    COUNT(*) AS scheduled_trips,
    COUNT_IF(delivery_status = 'DELIVERED') AS delivered_trips,
    COUNT_IF(delivery_status = 'CANCELED') AS canceled_trips,
    COUNT_IF(delivery_status = 'NO_RT_DATA') AS no_rt_data_trips,

    (
        SELECT COUNT(DISTINCT f.trip_id)
        FROM LEMMING_DB.FINAL_PROJECT_FACT.FACT_TRIP_UPDATES f
        WHERE f.trip_start_date = $trip_start_date
          AND f.static_version_date = $static_version_date
          AND COALESCE(NULLIF(TRIM(f.trip_schedule_rel), ''), 'SCHEDULED') = 'ADDED'
          AND f.route_id = joined.route_id
          AND (
                (f.direction_id = joined.direction_id)
                OR (f.direction_id IS NULL AND joined.direction_id IS NULL)
              )
    ) AS added_trips,

    ROUND(
        COUNT_IF(delivery_status = 'DELIVERED') * 100.0
        / NULLIF(COUNT(*), 0),
        2
    ) AS pct_delivered,

    MAX(static_version_date) AS static_version_date
FROM joined
WHERE delivery_status <> 'ADDED'
GROUP BY
    service_date,
    trip_start_date,
    route_id,
    direction_id;