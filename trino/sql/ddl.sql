CREATE SCHEMA IF NOT EXISTS hive.energy
WITH (location = 'hdfs://namenode:9000/user/hive/warehouse/energy.db');

-- Training table
CREATE TABLE IF NOT EXISTS hive.energy.events_training (
  date_str varchar,
  time_str varchar,
  timestamp timestamp,
  global_active_power double,
  global_reactive_power double,
  voltage double,
  global_intensity double,
  sub_metering_1 double,
  sub_metering_2 double,
  sub_metering_3 double,
  total_sub_metering double,
  unmetered_power double,
  day integer,
  hour integer,
  dayofweek integer,
  year integer,
  month integer
)
WITH (
  external_location = 'hdfs://namenode:9000/user/hadoop/energy_data/training',
  format = 'PARQUET',
  partitioned_by = ARRAY['year','month']
);

-- Test table
CREATE TABLE IF NOT EXISTS hive.energy.events_test
WITH (
  external_location = 'hdfs://namenode:9000/user/hadoop/energy_data/test',
  format = 'PARQUET',
  partitioned_by = ARRAY['year','month']
) AS
SELECT * FROM hive.energy.events_training WHERE false;  -- clone schema only
