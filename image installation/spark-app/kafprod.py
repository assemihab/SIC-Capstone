import pandas as pd
import json
import time
from kafka import KafkaProducer
###############preprocessing################
fitdf=pd.read_csv('hourly_fitbit_sema_df_unprocessed.csv')
fitdf = fitdf.dropna(axis=1, thresh=0.5*len(fitdf))
fitdf.head()
unique_id = fitdf['id'].unique()
print('Number of unique id: ', len(unique_id))
fitdf = fitdf.drop('Unnamed: 0', axis=1)




BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'hourlyfit'
producer = KafkaProducer(
    bootstrap_servers=BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)
def generate_subset(df):
    for i in range(len(unique_id)):
        subset = df[df['id'] == unique_id[i]]
        yield subset
for subset in generate_subset(fitdf):
    subsett=subset.to_dict(orient='records')
    producer.send(TOPIC_NAME, value=subsett)
    time.sleep(10)  # Sleep for 1 second
    print('Sent a subset of data to Kafka')
producer.flush()