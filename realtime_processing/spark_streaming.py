from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, concat, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session
spark = SparkSession.builder \
  .appName("EnergyDataStreaming") \
  .config("spark.cassandra.connection.host", "cassandra") \
  .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
  .getOrCreate()

# Define schema for incoming JSON data
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

# Read from Kafka topic
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "raw-energy-data") \
  .option("startingOffsets", "latest") \
  .load()

# Parse JSON from Kafka value column
parsed_df = df.select(
  from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert to JSON for Kafka output
output_df = parsed_df.select(
    to_json(struct("*")).alias("value")
)

# Write to kafka topic
query = output_df.writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("topic", "processed-energy-data") \
  .option("checkpointLocation", "/opt/spark/app/checkpoint") \
  .start()

#Write to Cassandra
# 1. Parse JSON and normalize existing column names
parsed_df = parsed_df \
  .withColumnRenamed("Date", "date_str") \
  .withColumnRenamed("Time", "time_str") \
  .withColumnRenamed("Global_active_power", "global_active_power") \
  .withColumnRenamed("Global_reactive_power", "global_reactive_power") \
  .withColumnRenamed("Voltage", "voltage") \
  .withColumnRenamed("Global_intensity", "global_intensity") \
  .withColumnRenamed("Sub_metering_1", "sub_metering_1") \
  .withColumnRenamed("Sub_metering_2", "sub_metering_2") \
  .withColumnRenamed("Sub_metering_3", "sub_metering_3") \

# 2. Create the missing Primary Key columns (ts and hour)
# Cassandra expects 'ts' as a timestamp and 'hour' as a string (yyyy-MM-dd-HH)
parsed_df = parsed_df \
  .withColumn("ts", to_timestamp(concat(col("date_str"), lit(" "), col("time_str")), "dd/MM/yyyy HH:mm:ss")) \
  .withColumn("hour", concat(col("date_str"), lit("-"), col("time_str").substr(1, 2)))

# 3. Cast columns to match Cassandra data types
parsed_df = parsed_df \
  .withColumn("global_active_power", col("global_active_power").cast("double")) \
  .withColumn("global_reactive_power", col("global_reactive_power").cast("double")) \
  .withColumn("voltage", col("voltage").cast("double")) \
  .withColumn("global_intensity", col("global_intensity").cast("double")) \
  .withColumn("sub_metering_1", col("sub_metering_1").cast("double")) \
  .withColumn("sub_metering_2", col("sub_metering_2").cast("double")) \
  .withColumn("sub_metering_3", col("sub_metering_3").cast("double")) \
  #.withColumn("total_sub_metering", (col("sub_metering_1") + col("sub_metering_2") + col("sub_metering_3"))) \
  #.withColumn("unmetered_power", (col("global_active_power") * 1000 / 60) - col("total_sub_metering"))

# 4. Write to Cassandra
query = parsed_df.writeStream \
  .format("org.apache.spark.sql.cassandra") \
  .option("keyspace", "energy") \
  .option("table", "events_by_hour") \
  .option("checkpointLocation", "/opt/spark/app/checkpoint") \
  .outputMode("append") \
  .start()

query.awaitTermination()