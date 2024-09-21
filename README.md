# SIC-Capstone
![New Project](https://github.com/user-attachments/assets/4177a449-fb0c-4eec-ad8b-23f61882b6ba)

## Streaming Project Iterative Plan

1. **Simulate stream code in Python** (assem)
2. ✅**create the .yml container and create the image** (assem)
   
3. ✅**Ingest data using Kafka** (Mark)+
   - ✅Create different Kafka topics (e.g., one for heart disease, one for sleep, etc.)
4. ✅**spark code for processing and store data to <del>cassandra</del> (mongodb) (a3taked keda me4 3arf)** rehab

4. ✅**Process the data and connect it to a BI tool**(mark)
   -✅ Create a multi-facade dashboard for better visualization

5. ✅**Store the data in Cassandra**(yehia)
   - <del>Create different column families for each topic (if necessary)</del>
   - ✅Connect Cassandra to Datastax Cloud _**(Done By Yahya)**_
   - ✅create the schema _**(Done By Yahya)**_

6. **Create the PPT**(rehab)
7. **create gantt chart**
8. **create documentation**
9. ✅connect mongoDB atlas with pyspark



## Project Setup
### Install required Docker Images
We used Docker so that we can use Kafka and spark with cassandra easily with other tools.
Firstly Create ```docker-compose.yml``` to install required images:
```yaml
services:
  zookeeper: # required for Kafka
    image: wurstmeister/zookeeper
    ports:
      - "2181:2181"

  kafka:
    image: wurstmeister/kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT://localhost:9092 # we will use one of them to connect with sprak
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092  # Kafka will listen on all network interfaces
    depends_on:
      - zookeeper

  spark:
    image: bitnami/spark
    environment:
      - SPARK_MODE=master
    ports:
      - "7077:7077"
      - "8080:8080"
    volumes:
      - ./spark-app:/opt/spark-app # to make this directory accessible

  mongodb:
    image: mongo
    ports:
      - "27017:27017"

  cassandra:
    image: cassandra
    ports:
      - "9042:9042"
```



### Connect Kafka with spark
You need to put this configration your sparkSession
```python
spark = SparkSession.builder \
   .appName("Healthcare Monitoring") \
   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_<scala-version>:<spark-version>") \
   .getOrCreate()
```
Take Care of connection string as version matters you can get it properly by following these steps:
1 - Check your spark and scala version:
```
docker exec -it <spark-container-id> spark-submit --version
```
You should see this:
![image](https://github.com/user-attachments/assets/f2cb8809-dbf7-4559-8899-40984e9844c2)
Then when you run ```spark-sumbit``` incluce this package as:
```
docker exec -it finalproject-spark-1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_<scala-version>:<spark-version> /opt/spark-app/health_monitoring.py
```
You can get connection string that match your version from: [Link](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10)

Before you can read kafka messeges you must create a kafka topic:
```
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic my-topic
```

Then create a producer with that topic:
```
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-topic
```

After that you can read from kafka topic as:
```python
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "my-topic") \
  .load()

query = df.writeStream \
  .format("console") \
  .start()

query.awaitTermination()
```

### Connect spark to DataStax to use Cassandra in the cloud
We have to connect cassandra with spark so that after reading messeges from kafka topic we will be able to store our spark DataFrame in existing table in cassandra.

We need to install ```cassandra-driver``` in our spark container to be able to connect to DataStax to use Cassandra in the cloud.
We can install it by running the following command in "Exec" tab in spark container page in docker application as follows:
![image](https://github.com/user-attachments/assets/1981cf8f-8634-41eb-a660-fc768ac0d8ca)


```
pip install cassandra-driver
```
Check the package is successfully installed using:
```
pip list
```

You have to navigate to [Datastax](https://www.datastax.com/) create an account ,create database and download zip bundle file and json file (we will need these to files for connection).

Put these two files in **spark-app** directory, and then your files structure should be like:

![image](https://github.com/user-attachments/assets/197cb1b9-c0f2-493e-bdd8-6e60898b6b3b)

To read json file to be able use:
```python
import json

# This token JSON file is autogenerated when you download your token, 
# if yours is different, update the file name below
with open("/opt/spark-app/yahiamahmoood333@gmail.com-token.json") as f:
  secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]
```

Edit our sparkSession configration
```python
spark = SparkSession.builder \
  .appName("Healthcare Monitoring") \
  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
  .config("spark.cassandra.connection.config.cloud.path", "secure-connect-healthcare-streaming.zip") \
  .config("spark.cassandra.auth.username", CLIENT_ID) \
  .config("spark.cassandra.auth.password", CLIENT_SECRET) \
  .getOrCreate()
```

Then when you run spark docker container include these additional packages:
```
docker exec -it finalproject-spark-1 spark-submit \
--files /opt/spark-app/secure-connect-healthcare-streaming.zip \
--conf spark.cassandra.connection.config.cloud.path=secure-connect-healthcare-streaming.zip \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
/opt/spark-app/health_monitoring.py
```
```--files``` here to make the file accessible to all executors in distributed systems.
Then you can write data to cassandra by using:
```python
df.write \
  .format("org.apache.spark.sql.cassandra") \
  .option("keyspace", "<keyspace-name>") \
  .option("table", "<table-name>") \
  .mode("append") \
  .save()
```

### Connect spark with MongoDB atlas
We need to include spakr-mongodb-connector in our packages, in addition to connection string.

Our sparkSession configration will be like this:
```python
spark = SparkSession.builder \
  .appName("Healthcare Monitoring") \
  .config("spark.cassandra.connection.config.cloud.path", "secure-connect-healthcare-streaming.zip") \
  .config("spark.cassandra.auth.username", CLIENT_ID) \
  .config("spark.cassandra.auth.password", CLIENT_SECRET) \
  .config("spark.mongodb.spark.enabled", "true") \
  .config("spark.mongodb.read.connection.uri", connection_string) \
  .config("spark.mongodb.write.connection.uri", connection_string) \
  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
  .getOrCreate()
```

Then when we run docker container we will contain the package
```
docker exec -it finalproject-spark-1 spark-submit --files /opt/spark-app/secure-connect-healthcare-streaming.zip --conf spark.cassandra.connection.config.cloud.path=secure-connect-healthcare-streaming.zip --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 /opt/spark-app/health_monitoring.py
```

_Make sure that package is combitable with your spark and scala version._

Then you can write to mongoDB atlas as:
```python
df.write.format("mongodb") \
  .option("spark.mongodb.write.connection.uri", connection_string) \
  .option("database", "healthcare") \
  .option("collection", "streaming") \
  .mode("append") \
  .save()
```

In our case we are getting streaming data from kafka topic so we will use writeStream instead:

```python
query = df.writeStream.format("mongodb") \
  .option("spark.mongodb.write.connection.uri", connection_string) \
  .option("database", "healthcare") \
  .option("collection", "streaming") \
  .option("checkpointLocation", "/tmp/checkpoint/old") \
  .outputMode("append") \
  .start()
```

_Notes_:
1. these checkpoint file to understand from where to continue to read from Kafka so it does not skip or miss any message.
2. OutputMode are **append** as mongoDB as there are not any aggregations.
