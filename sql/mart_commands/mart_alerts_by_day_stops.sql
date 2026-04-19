-- this metric can be used to view particular stops that are highly affected by alerts 

DELETE FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY_STOPS
WHERE alert_date = '2026-03-11';

INSERT INTO LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY_STOPS
WITH active_on_date AS (
    SELECT
        far.entity_id,
        far.stop_id,
        st_dim.stop_name as stop_name,
        st_dim.stop_lat as stop_lat, 
        st_dim.stop_lon as stop_lon,
        fa.severity_level,
        '2026-03-11' AS alert_date
    FROM LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS_ACTIVE_PERIODS fa_activep
    JOIN LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS_ROUTES far
        ON fa_activep.entity_id = far.entity_id
    JOIN LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS fa
        ON fa_activep.entity_id = fa.entity_id
    JOIN LEMMING_DB.FINAL_PROJECT_STATIC.DIM_STOPS st_dim -- get the stop name, since it's more intuitive for stops
      ON  far.stop_id           = st_dim.stop_id
      AND fa.static_version_date = st_dim.feed_start_date
    WHERE far.route_type = 3
      AND DATE(fa_activep.active_start) <= '2026-03-11'
      AND (fa_activep.active_end IS NULL OR DATE(fa_activep.active_end) >= '2026-03-11')
)
SELECT
    alert_date,
    stop_name,
    AVG(stop_lat)                                                           AS stop_lat, -- for the stop names that get grouped together (as described in table creation), the latitudes and longituded are either exactly the same or different by some order of 10^-5, so taking the average shouldn't affect the accuracy of the visualization 
    AVG(stop_lon)                                                           AS stop_lon,
    COUNT(DISTINCT entity_id)                                               AS alert_count,
    COUNT(DISTINCT CASE WHEN severity_level = 'SEVERE'  THEN entity_id END) AS severe_count,
    COUNT(DISTINCT CASE WHEN severity_level = 'WARNING' THEN entity_id END) AS warning_count,
    COUNT(DISTINCT CASE WHEN severity_level = 'INFO'    THEN entity_id END) AS info_count
FROM active_on_date
GROUP BY 1, 2;