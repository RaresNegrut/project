import time
import json
from kafka import KafkaProducer
from simulator import simulator

def load_config(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

try:
  config = load_config('config.json')

  # Connect to Kafka broker (default localhost:9092)
  producer = KafkaProducer(
      bootstrap_servers=[config['kafka_broker']],
      value_serializer=lambda v: json.dumps(v).encode('utf-8')
  )
  
  for item in simulator.stream_csv_to_json(config['csv_file'], delimiter=config['csv_delimiter']):
      time.sleep(config['frequency_seconds']) #wait to simulate streaming

      # Send each row as JSON to topic
      producer.send(config['topic'], value=item)
      #print(f"Sent: {item}")
  
  producer.flush()
  producer.close()   
      
except Exception as e:
  print(e)