-- for this metric, there's usually only a few alerts per route id
-- in the visualization, it probably makes the most sense to view this aggregated over all routes for a particular day, as a stacked bar chart for the type of alert counts.
-- we could also use this to show which routes have the most alerts over a specified period of time

SET target_service_date = TO_DATE('{{ ds }}');

DELETE FROM FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
WHERE alert_date = $target_service_date;

INSERT INTO FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
WITH active_on_date AS (
    SELECT
        far.entity_id,
        far.route_id,
        fa.severity_level,
        $target_service_date AS alert_date
    FROM FINAL_PROJECT_FACT.FACT_ALERTS_ACTIVE_PERIODS fa_activep
    JOIN FINAL_PROJECT_FACT.FACT_ALERTS_ROUTES far
        ON fa_activep.entity_id = far.entity_id
    JOIN FINAL_PROJECT_FACT.FACT_ALERTS fa
        ON fa_activep.entity_id = fa.entity_id
    WHERE far.route_type = 3
    AND DATE(fa_activep.active_start) <= $target_service_date
    AND (fa_activep.active_end IS NULL OR DATE(fa_activep.active_end) >= $target_service_date)
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
