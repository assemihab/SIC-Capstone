from json import dumps
from kafka import KafkaProducer
import time
import csv
from datetime import datetime

# to get rows in csv file as list
rows = []
with open('dataset.csv') as csv_file:
  reader = csv.reader(csv_file)

  rows = list(reader)

BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'mindf'

# Create Kafka producer to simulate sensor data
producer = KafkaProducer(
  bootstrap_servers=BROKER_URL,
  value_serializer = lambda x:dumps(x).encode('utf-8')  # to make as json format
)

# add streaming messeges to the kafka topic
for row in rows[1:]: # to skip header row
  
  # to make data able to be parsed as json
  log = {
    "id": row[1],
    "temperature": row[4],
    "date": row[2],
    "hour": row[3],
    "calories": row[6],
    "distance": row[7],
    "bpm": row[9], 
    "steps": row[12],
    "age": row[17],
    "gender": row[18],
    "bmi": row[19],
  }
  
  print(log)

  # producer.send("my-topic", value=log)
  time.sleep(5)
