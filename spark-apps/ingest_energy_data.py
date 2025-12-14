from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat_ws, year, month, dayofmonth, hour, dayofweek, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys

spark = SparkSession.builder \
    .appName("Energy Data Ingestion") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Global_active_power", StringType(), True),
    StructField("Global_reactive_power", StringType(), True),
    StructField("Voltage", StringType(), True),
    StructField("Global_intensity", StringType(), True),
    StructField("Sub_metering_1", StringType(), True),
    StructField("Sub_metering_2", StringType(), True),
    StructField("Sub_metering_3", StringType(), True)
])

print("Reading data...")
df = spark.read.csv("file:///data/household_power_consumption.txt", sep=";", header=True, schema=schema)

print("Cleaning data...")
numeric_columns = ["Global_active_power", "Global_reactive_power", "Voltage", "Global_intensity",
                   "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"]

for col_name in numeric_columns:
    df = df.withColumn(col_name, when(col(col_name) == "?", None).otherwise(col(col_name).cast(DoubleType())))

df = df.withColumn("timestamp", to_timestamp(concat_ws(" ", col("Date"), col("Time")), "d/M/yyyy HH:mm:ss"))

df = df.withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp"))) \
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("dayofweek", dayofweek(col("timestamp")))

df = df.withColumn("Total_sub_metering", col("Sub_metering_1") + col("Sub_metering_2") + col("Sub_metering_3"))
df = df.withColumn("Unmetered_power", (col("Global_active_power") * 1000 / 60) - col("Total_sub_metering"))

df = df.filter(col("timestamp").isNotNull())

print(f"Total records: {df.count()}")

# Split training (before Oct 2010) and test (Oct-Nov 2010)
training_df = df.filter(col("timestamp") < lit("2010-10-01"))
test_df = df.filter(col("timestamp") >= lit("2010-10-01"))

print(f"Training records: {training_df.count()}")
print(f"Test records: {test_df.count()}")

print("Writing to HDFS...")
training_df.write.mode("overwrite").partitionBy("year", "month").option("compression", "snappy").json("hdfs://namenode:9000/user/hadoop/energy_data/training")
test_df.write.mode("overwrite").option("compression", "snappy").json("hdfs://namenode:9000/user/hadoop/energy_data/test")

print("✓ Ingestion complete!")
spark.stop()