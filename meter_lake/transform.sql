CREATE TABLE interval_usage AS
WITH boundaries AS (
 SELECT meter_id, observed_at AS interval_end, register_wh,
 lag(observed_at) OVER (PARTITION BY meter_id ORDER BY observed_at) AS interval_start,
 lag(register_wh) OVER (PARTITION BY meter_id ORDER BY observed_at) AS previous_wh
 FROM readings
)
SELECT b.meter_id, m.site, b.interval_start, b.interval_end,
 b.register_wh - b.previous_wh AS delta_wh,
 CAST((b.register_wh-b.previous_wh)*m.multiplier/1000.0 AS DECIMAL(24,6)) AS kwh,
 m.max_kw
FROM boundaries b JOIN meters m USING (meter_id)
WHERE b.previous_wh IS NOT NULL;

CREATE TABLE daily_site_usage AS
SELECT site, CAST(interval_start AS DATE) AS usage_date,
 count(*) AS meter_intervals, sum(kwh) AS kwh
FROM interval_usage GROUP BY site, CAST(interval_start AS DATE);
