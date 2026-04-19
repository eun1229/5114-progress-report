-- for this metric, there's usually only a few alerts per route id
-- in the visualization, it probably makes the most sense to view this aggregated over all routes for a particular day, as a stacked bar chart for the type of alert counts.
-- we could also use this to show which routes have the most alerts over a specified period of time

-- replace '2026-03-12' :service_date

DELETE FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
WHERE alert_date = '2026-03-12';

INSERT INTO LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
WITH active_on_date AS (
    SELECT
        far.entity_id,
        far.route_id,
        fa.severity_level,
        '2026-03-12' AS alert_date
    FROM LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS_ACTIVE_PERIODS fa_activep
    JOIN LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS_ROUTES far
        ON fa_activep.entity_id = far.entity_id
    JOIN LEMMING_DB.FINAL_PROJECT_FACT.FACT_ALERTS fa
        ON fa_activep.entity_id = fa.entity_id
    WHERE far.route_type = 3
      AND DATE(fa_activep.active_start) <= '2026-03-12'
      AND (fa_activep.active_end IS NULL OR DATE(fa_activep.active_end) >= '2026-03-12')
)
SELECT
    alert_date,
    route_id,
    COUNT(DISTINCT entity_id)                                               AS alert_count,
    COUNT(DISTINCT CASE WHEN severity_level = 'SEVERE'  THEN entity_id END) AS severe_count,
    COUNT(DISTINCT CASE WHEN severity_level = 'WARNING' THEN entity_id END) AS warning_count,
    COUNT(DISTINCT CASE WHEN severity_level = 'INFO'    THEN entity_id END) AS info_count
FROM active_on_date
GROUP BY 1, 2;
