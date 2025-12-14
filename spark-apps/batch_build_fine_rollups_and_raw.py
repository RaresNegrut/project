from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, count, date_format, year, month, date_trunc,
    unix_timestamp, from_unixtime, floor
)

CASS_HOST = "cassandra"
KEYSPACE = "energy"

def make_spark():
    return (SparkSession.builder
            .appName("BatchBuildFineRollupsAndRaw")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
            .config("spark.cassandra.connection.host", CASS_HOST)
            .getOrCreate())

def write_to_cassandra(df, table):
    (df.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(keyspace=KEYSPACE, table=table)
      .save())

def normalize(df):
    return (df
      .withColumnRenamed("Global_active_power", "global_active_power")
      .withColumnRenamed("Global_reactive_power", "global_reactive_power")
      .withColumnRenamed("Global_intensity", "global_intensity")
      .withColumnRenamed("Sub_metering_1", "sub_metering_1")
      .withColumnRenamed("Sub_metering_2", "sub_metering_2")
      .withColumnRenamed("Sub_metering_3", "sub_metering_3")
      .withColumnRenamed("Total_sub_metering", "total_sub_metering")
      .withColumnRenamed("Unmetered_power", "unmetered_power")
      .withColumnRenamed("Voltage", "voltage")
      .withColumnRenamed("Date", "date_str")
      .withColumnRenamed("Time", "time_str")
    )

def bucket_5m(ts_col):
    return from_unixtime(floor(unix_timestamp(ts_col)/lit(300))*lit(300)).cast("timestamp")

def build_scoped_rollups(base):
    # ALL scope
    all_roll = (base.groupBy("bucket_start").agg(
        avg("global_active_power").alias("avg_global_active_power"),
        avg("global_reactive_power").alias("avg_global_reactive_power"),
        avg("voltage").alias("avg_voltage"),
        avg("global_intensity").alias("avg_global_intensity"),
        avg("total_sub_metering").alias("avg_total_sub_metering"),
        avg("unmetered_power").alias("avg_unmetered_power"),
        count(lit(1)).alias("n")
    )
    .withColumn("scope", lit("ALL"))
    .withColumn("avg_sub_metering", col("avg_total_sub_metering"))
    )

    def sm(colname, scope):
        return (base.groupBy("bucket_start").agg(
            avg(colname).alias("avg_sub_metering"),
            count(lit(1)).alias("n")
        )
        .withColumn("scope", lit(scope))
        .withColumn("avg_global_active_power", lit(None).cast("double"))
        .withColumn("avg_global_reactive_power", lit(None).cast("double"))
        .withColumn("avg_voltage", lit(None).cast("double"))
        .withColumn("avg_global_intensity", lit(None).cast("double"))
        .withColumn("avg_total_sub_metering", lit(None).cast("double"))
        .withColumn("avg_unmetered_power", lit(None).cast("double"))
        )

    sm1 = sm("sub_metering_1", "SM1")
    sm2 = sm("sub_metering_2", "SM2")
    sm3 = sm("sub_metering_3", "SM3")

    cols = ["scope","bucket_start",
            "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
            "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"]

    return (all_roll.select(cols)
            .unionByName(sm1.select(cols))
            .unionByName(sm2.select(cols))
            .unionByName(sm3.select(cols)))

if __name__ == "__main__":
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    training_path = "hdfs://namenode:9000/user/hadoop/energy_data/training"
    test_path = "hdfs://namenode:9000/user/hadoop/energy_data/test"

    df = normalize(spark.read.json(training_path))
    # include test too (so rollups exist for 201010)
    df = df.unionByName(normalize(spark.read.json(test_path)), allowMissingColumns=True)

    df = df.withColumn("timestamp", col("timestamp").cast("timestamp")).filter(col("timestamp").isNotNull())

    # RAW
    raw = (df.withColumn("day", date_format(col("timestamp"), "yyyy-MM-dd"))
           .select(
               "day", col("timestamp").alias("ts"),
               "date_str","time_str",
               "global_active_power","global_reactive_power","voltage","global_intensity",
               "sub_metering_1","sub_metering_2","sub_metering_3",
               "total_sub_metering","unmetered_power",
               year("timestamp").cast("int").alias("year"),
               month("timestamp").cast("int").alias("month"),
           ))
    write_to_cassandra(raw, "raw_events_by_day")

    # 5m rollup
    b5 = df.withColumn("bucket_start", bucket_5m(col("timestamp")))
    base5 = b5.select("bucket_start","global_active_power","global_reactive_power","voltage","global_intensity",
                      "sub_metering_1","sub_metering_2","sub_metering_3","total_sub_metering","unmetered_power")
    r5 = build_scoped_rollups(base5).withColumn("yyyymm", (year("bucket_start")*100 + month("bucket_start")).cast("int"))
    write_to_cassandra(r5.select(
        "scope","yyyymm","bucket_start",
        "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
        "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"
    ), "rollup_5m")

    # 1h rollup
    b1h = df.withColumn("bucket_start", date_trunc("hour", col("timestamp")))
    base1h = b1h.select("bucket_start","global_active_power","global_reactive_power","voltage","global_intensity",
                        "sub_metering_1","sub_metering_2","sub_metering_3","total_sub_metering","unmetered_power")
    r1h = build_scoped_rollups(base1h).withColumn("yyyymm", (year("bucket_start")*100 + month("bucket_start")).cast("int"))
    write_to_cassandra(r1h.select(
        "scope","yyyymm","bucket_start",
        "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
        "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"
    ), "rollup_1h")

    print("Raw + rollup_5m + rollup_1h backfilled.")
    spark.stop()
