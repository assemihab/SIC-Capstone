# %%
import pandas as pd
import json
import time
from kafka import KafkaProducer
###############preprocessing################
fitdf=pd.read_csv('hourly_fitbit_sema_df_unprocessed.csv')
fitdf = fitdf.dropna(axis=1, thresh=0.5*len(fitdf))
fitdf.head()
unique_id = fitdf['id'].unique()
# print('Number of unique id: ', len(unique_id))
fitdf = fitdf.drop('Unnamed: 0', axis=1)
fitdf.head()

# %%
# count nulls
# print('Number of missing values in each column:')
# print(fitdf.isnull().sum())


# %%
# get columns date hour and mindfulness_session

# %%
# replace the nulls in mindfulness_session with notRecorded
fitdf['mindfulness_session'] = fitdf['mindfulness_session'].fillna('notRecorded')
# display the data types
# print('Data types of the columns:')
# 2021-05-24 the date column is in this format and there is a nother column called hour its float
# with values of 0-23
fitdf['date'] = pd.to_datetime(fitdf['date'])
fitdf['hour'] = fitdf['hour'].astype(int)
# print(fitdf.dtypes)
# fitdf.head()



# %%
# get columns date hour and mindfulness_session
fitdf = fitdf[['date', 'hour', 'mindfulness_session']]
# fitdf.head()
sorted_df = fitdf.sort_values(by=['date', 'hour'])
# sorted_df.head()

# %%
# concate date and hour
sorted_df['date_hour'] = sorted_df['date'].astype(str) + ' ' + sorted_df['hour'].astype(str)
sorted_df['date_hour'] = pd.to_datetime(sorted_df['date_hour'])
sorted_df = sorted_df.drop(['date', 'hour'], axis=1)
# sorted_df.head()
sorted_df['mindfulness_session'].value_counts()

# %%
sorted_df=sorted_df.reset_index(drop=True)
# sorted_df.head()

# %%
# for each unique date_hour get subset of the data

BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'mindf'
producer = KafkaProducer(
    bootstrap_servers=BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)
uniqueDateHour = sorted_df['date_hour'].unique()
print('Number of unique date_hour: ', len(uniqueDateHour))
for i in range(0, len(uniqueDateHour)):
    # print('Date_hour: ', uniqueDateHour[i])
    subset=sorted_df[sorted_df['date_hour'] == uniqueDateHour[i]]
    subset=subset.drop('date_hour', axis=1)
    subsett=subset.to_dict(orient='records')
    producer.send(TOPIC_NAME, value=subsett)
    time.sleep(1)  # Sleep for 1 second
    print('Sent a subset of data to Kafka')
    

# %%



