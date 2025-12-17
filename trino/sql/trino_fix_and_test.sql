-- 1D rollup across BOTH sources:
--   - Cassandra for last 7 days (fast)
--   - Hive for older history (complete)

USE cassandra.energy;


WITH
-- Speed layer: already aggregated in Cassandra
rt AS (
  SELECT
    scope,
    yyyymm,
    bucket_start,
    avg_global_active_power,
    avg_global_reactive_power,
    avg_voltage,
    avg_global_intensity,
    avg_sub_metering,
    avg_total_sub_metering,
    avg_unmetered_power,
    n,
    'realtime' AS layer
  FROM cassandra.energy.rollup_1d
  WHERE bucket_start >= date_trunc('day', TIMESTAMP '2007-01-01 08:00' - INTERVAL '7' DAY)
),

-- Batch layer: compute only the ALL-scope rollup from raw events in Hive
bt_all AS (
  SELECT
    'ALL' AS scope,
    CAST(year(date_trunc('day', "timestamp")) * 100 + month(date_trunc('day', "timestamp")) AS integer) AS yyyymm,
    date_trunc('day', "timestamp") AS bucket_start,
    avg(global_active_power) AS avg_global_active_power,
    avg(global_reactive_power) AS avg_global_reactive_power,
    avg(voltage) AS avg_voltage,
    avg(global_intensity) AS avg_global_intensity,
    avg(total_sub_metering) AS avg_sub_metering,
    avg(total_sub_metering) AS avg_total_sub_metering,
    avg(unmetered_power) AS avg_unmetered_power,
    count(*) AS n,
    'batch' AS layer
  FROM hive.energy.events_training
  WHERE "timestamp" < date_trunc('day', TIMESTAMP '2007-01-01 08:00' - INTERVAL '7' DAY)
  GROUP BY 1, 2, 3
)

(SELECT * FROM rt limit 1)
UNION ALL
(SELECT * FROM bt_all limit 1)
ORDER BY bucket_start DESC;

SELECT * 
FROM hive.energy."events_training$properties";

SHOW CREATE TABLE hive.energy.events_training;
SELECT * FROM hive.energy."events_training$partitions" LIMIT 10;

CALL hive.system.sync_partition_metadata('energy', 'events_training', 'ADD', false);


SELECT "$path", "$file_size"
FROM hive.energy.events_training
LIMIT 20;

SELECT count(*) AS cnt
FROM hive.energy.events_training;


SHOW CATALOGS;
SHOW SCHEMAS FROM hive;
SHOW TABLES FROM hive.energy;
