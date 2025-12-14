from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder \
    .appName("Energy Simulator") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load test data from HDFS
print("Loading test data...")
df = spark.read.json("hdfs://namenode:9000/user/hadoop/energy_data/test")
df = df.orderBy("timestamp")
data_rows = df.collect()

print(f"Loaded {len(data_rows)} records")
print("Starting simulation (publishing every 2 seconds)...")
print("Press Ctrl+C to stop\n")

try:
    for i, row in enumerate(data_rows):
        message = {
            "sensor_id": "household_001",
            "timestamp": str(row["timestamp"]),
            "measurements": {
                "global_active_power": float(row["Global_active_power"]) if row["Global_active_power"] else None,
                "voltage": float(row["Voltage"]) if row["Voltage"] else None,
                "global_intensity": float(row["Global_intensity"]) if row["Global_intensity"] else None
            },
            "simulation_time": datetime.now().isoformat()
        }
        
        producer.send('energy-sensors', value=message)
        
        if (i + 1) % 100 == 0:
            print(f"Published {i + 1} records...")
        
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopped by user")

finally:
    producer.close()
    spark.stop()
    print(f"Total published: {i + 1} records")