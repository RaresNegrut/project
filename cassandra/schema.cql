CREATE KEYSPACE IF NOT EXISTS energy
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Raw events partitioned by day for easy daily export to HDFS
CREATE TABLE IF NOT EXISTS energy.raw_events_by_day (
  day text,                -- yyyy-MM-dd
  ts timestamp,            -- event timestamp
  date_str text,
  time_str text,

  global_active_power double,
  global_reactive_power double,
  voltage double,
  global_intensity double,

  sub_metering_1 double,
  sub_metering_2 double,
  sub_metering_3 double,

  total_sub_metering double,
  unmetered_power double,

  year int,
  month int,

  PRIMARY KEY ((day), ts)
) WITH CLUSTERING ORDER BY (ts ASC);

-- Rollup tables: one per granularity; scope = ALL|SM1|SM2|SM3 (your “rooms”)
CREATE TABLE IF NOT EXISTS energy.rollup_5m (
  scope text,
  yyyymm int,              -- year*100 + month
  bucket_start timestamp,

  avg_global_active_power double,
  avg_global_reactive_power double,
  avg_voltage double,
  avg_global_intensity double,

  avg_sub_metering double,       -- for SMx: that submeter avg; for ALL: avg_total_sub_metering
  avg_total_sub_metering double, -- only populated for ALL
  avg_unmetered_power double,    -- only populated for ALL

  n bigint,

  PRIMARY KEY ((scope, yyyymm), bucket_start)
) WITH CLUSTERING ORDER BY (bucket_start ASC);

CREATE TABLE IF NOT EXISTS energy.rollup_1h (
  scope text,
  yyyymm int,
  bucket_start timestamp,

  avg_global_active_power double,
  avg_global_reactive_power double,
  avg_voltage double,
  avg_global_intensity double,

  avg_sub_metering double,
  avg_total_sub_metering double,
  avg_unmetered_power double,

  n bigint,

  PRIMARY KEY ((scope, yyyymm), bucket_start)
) WITH CLUSTERING ORDER BY (bucket_start ASC);


-- Daily (partition by yyyymm)
CREATE TABLE IF NOT EXISTS energy.rollup_1d (
  scope text,
  yyyymm int,
  bucket_start timestamp,

  avg_global_active_power double,
  avg_global_reactive_power double,
  avg_voltage double,
  avg_global_intensity double,

  avg_sub_metering double,
  avg_total_sub_metering double,
  avg_unmetered_power double,

  n bigint,

  PRIMARY KEY ((scope, yyyymm), bucket_start)
) WITH CLUSTERING ORDER BY (bucket_start ASC);

-- Weekly (partition by year)
CREATE TABLE IF NOT EXISTS energy.rollup_1w (
  scope text,
  yyyy int,
  bucket_start timestamp,

  avg_global_active_power double,
  avg_global_reactive_power double,
  avg_voltage double,
  avg_global_intensity double,

  avg_sub_metering double,
  avg_total_sub_metering double,
  avg_unmetered_power double,

  n bigint,

  PRIMARY KEY ((scope, yyyy), bucket_start)
) WITH CLUSTERING ORDER BY (bucket_start ASC);

-- Monthly (partition by year)
CREATE TABLE IF NOT EXISTS energy.rollup_1mo (
  scope text,
  yyyy int,
  bucket_start timestamp,

  avg_global_active_power double,
  avg_global_reactive_power double,
  avg_voltage double,
  avg_global_intensity double,

  avg_sub_metering double,
  avg_total_sub_metering double,
  avg_unmetered_power double,

  n bigint,

  PRIMARY KEY ((scope, yyyy), bucket_start)
) WITH CLUSTERING ORDER BY (bucket_start ASC);

-- Yearly (partition by scope only)
CREATE TABLE IF NOT EXISTS energy.rollup_1y (
  scope text,
  bucket_start timestamp,

  avg_global_active_power double,
  avg_global_reactive_power double,
  avg_voltage double,
  avg_global_intensity double,

  avg_sub_metering double,
  avg_total_sub_metering double,
  avg_unmetered_power double,

  n bigint,

  PRIMARY KEY ((scope), bucket_start)
) WITH CLUSTERING ORDER BY (bucket_start ASC);

CREATE TABLE IF NOT EXISTS energy.events_by_hour (
  hour text,               -- yyyy-MM-dd-HH
  ts timestamp,            -- event timestamp
  date_str text,
  time_str text,

  global_active_power double,
  global_reactive_power double,
  voltage double,
  global_intensity double,

  sub_metering_1 double,
  sub_metering_2 double,
  sub_metering_3 double,

  --total_sub_metering double,
  --unmetered_power double,

  --year int,
  --month int,

  PRIMARY KEY ((hour), ts)
) WITH CLUSTERING ORDER BY (ts ASC);