CREATE KEYSPACE IF NOT EXISTS energy
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Raw events partitioned by day for easy daily export to HDFS
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