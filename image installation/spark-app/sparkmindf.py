

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,collect_list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType,FloatType
# from pymongo.mongo_client import MongoClient
import os
connection_string = "mongodb+srv://assem:1231234@cluster0.1kkof.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
spark = SparkSession.builder \
  .appName("mindff") \
  .config("spark.mongodb.spark.enabled", "true") \
  .config("spark.mongodb.read.connection.uri", connection_string) \
  .config("spark.mongodb.write.connection.uri", connection_string) \
  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kafka_bootstrap_servers = "kafka:9093"
kafka_topic = "mindf"

schema=([
    StructField('state',StringType(),True),
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic)\
    .option("failOnDataLoss","False")\
    .load()

kafkaValues=kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df=kafkaValues.selectExpr("explode(from_json(value, 'array<struct<mindfulness_session:string>>')) AS data")
parsed_df=parsed_df.select(col("data.*"))

# now get the value counts of the mindfulness_session
parsed_df=parsed_df.groupBy("mindfulness_session").count()
parsed_df=parsed_df.withColumnRenamed("count","count_of_mindfulness_session")
parsed_df=parsed_df.withColumnRenamed("mindfulness_session","mindfulness_session")



query = parsed_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
