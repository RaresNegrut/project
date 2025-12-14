from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, count, year, month, date_trunc
)

CASS_HOST = "cassandra"
KEYSPACE = "energy"

def make_spark():
    return (SparkSession.builder
            .appName("BatchBuildCoarseRollups")
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
    # Handle your mixed-case JSON columns
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
    )

def base_select(df, bucket_start):
    return df.select(
        bucket_start.alias("bucket_start"),
        col("global_active_power"),
        col("global_reactive_power"),
        col("voltage"),
        col("global_intensity"),
        col("sub_metering_1"),
        col("sub_metering_2"),
        col("sub_metering_3"),
        col("total_sub_metering"),
        col("unmetered_power"),
    )

def rollups_all_and_sms(df_bucketed):
    base = df_bucketed

    # ALL scope: full metrics
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

    # SM scopes: only avg_sub_metering populated
    def sm_roll(colname, scope):
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

    sm1 = sm_roll("sub_metering_1", "SM1")
    sm2 = sm_roll("sub_metering_2", "SM2")
    sm3 = sm_roll("sub_metering_3", "SM3")

    cols = [
        "scope","bucket_start",
        "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
        "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"
    ]

    return (all_roll.select(cols)
            .unionByName(sm1.select(cols))
            .unionByName(sm2.select(cols))
            .unionByName(sm3.select(cols)))

if __name__ == "__main__":
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read all available HDFS raw JSON sources for batch truth
    training_path = "hdfs://namenode:9000/user/hadoop/energy_data/training"
    # If you also export raw to HDFS daily, you can include it here:
    raw_export_path = "hdfs://namenode:9000/user/hadoop/energy_data/raw_export"

    df_train = spark.read.json(training_path)
    # raw_export may not exist yet; keep it optional
    try:
        df_export = spark.read.json(raw_export_path)
        df = df_train.unionByName(df_export, allowMissingColumns=True)
    except Exception:
        df = df_train

    df = normalize(df).filter(col("timestamp").isNotNull())

    # 1d
    b1d = base_select(df, date_trunc("day", col("timestamp")))
    r1d = rollups_all_and_sms(b1d).withColumn("yyyymm", (year("bucket_start")*100 + month("bucket_start")).cast("int"))
    write_to_cassandra(r1d.select(
        "scope","yyyymm","bucket_start",
        "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
        "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"
    ), "rollup_1d")

    # 1w
    b1w = base_select(df, date_trunc("week", col("timestamp")))
    r1w = rollups_all_and_sms(b1w).withColumn("yyyy", year("bucket_start").cast("int"))
    write_to_cassandra(r1w.select(
        "scope","yyyy","bucket_start",
        "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
        "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"
    ), "rollup_1w")

    # 1mo
    b1mo = base_select(df, date_trunc("month", col("timestamp")))
    r1mo = rollups_all_and_sms(b1mo).withColumn("yyyy", year("bucket_start").cast("int"))
    write_to_cassandra(r1mo.select(
        "scope","yyyy","bucket_start",
        "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
        "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"
    ), "rollup_1mo")

    # 1y
    b1y = base_select(df, date_trunc("year", col("timestamp")))
    r1y = rollups_all_and_sms(b1y)
    write_to_cassandra(r1y.select(
        "scope","bucket_start",
        "avg_global_active_power","avg_global_reactive_power","avg_voltage","avg_global_intensity",
        "avg_sub_metering","avg_total_sub_metering","avg_unmetered_power","n"
    ), "rollup_1y")

    print("Coarse rollups (1d/1w/1mo/1y) built.")
    spark.stop()
