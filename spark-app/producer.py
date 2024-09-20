from json import dumps
from kafka import KafkaProducer
import time
import csv
from datetime import datetime

# to get rows in csv file as list
rows = []
with open('/opt/spark-app/dataset.csv') as csv_file:
  reader = csv.reader(csv_file)

  rows = list(reader)

# Create Kafka producer to simulate sensor data
producer = KafkaProducer(
  bootstrap_servers = ['kafka:9092'],
  value_serializer = lambda x:dumps(x).encode('utf-8')  # to make as json format
)

date_format = "%Y-%m-%d %H:%M:%S"

# add streaming messeges to the kafka topic
for row in rows[1:]: # to skip header row

  # combine date and hour to match our format
  # Ex. 11.0 => 11:00
  # Ex. 5.0 => 05:00
  my_time = row[2]
  hour = int(row[3][:-2])
  my_time += (" " if hour > 9 else " 0") + str(hour) + ":00:00"
  print(my_time)
  
  # to make data able to be parsed as json
  log = {
    "id": row[1],
    "temperature": float(row[4] or 0),
    "date": str(datetime.strptime(my_time, date_format)),
    "hour": float(row[3]),
    "calories": float(row[6] or 0),
    "distance": float(row[7] or 0),
    "bpm": float(row[9] or 0), 
    "steps": float(row[12] or 0),
    "age": row[17],
    "gender": row[18],
    "bmi": row[19],
  }
  
  producer.send("my-topic", value=log)
  time.sleep(5)
