
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,collect_list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType,FloatType
# from pymongo.mongo_client import MongoClient
import os

connection_string = "mongodb+srv://assem:1231234@cluster0.1kkof.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
spark = SparkSession.builder \
  .appName("Healthcare Monitoring") \
  .config("spark.mongodb.spark.enabled", "true") \
  .config("spark.mongodb.read.connection.uri", connection_string) \
  .config("spark.mongodb.write.connection.uri", connection_string) \
  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# id	date	hour	calories	distance	bpm	mindfulness_session	steps	age	gender	bmi

schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("hour", FloatType(), True),
    StructField("calories", FloatType(), True),
    StructField("distance", FloatType(), True),
    StructField("bpm", FloatType(), True),
    StructField("mindfulness_session", BooleanType(), True),
    StructField("steps", FloatType(), True),
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("bmi", StringType(), True)
])

kafka_bootstrap_servers = "kafka:9093"
kafka_topic = "hourlyfit"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic)\
    .option("failOnDataLoss","False")\
    .load()


#read this data 
kafka_values = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = kafka_values.selectExpr("explode(from_json(value, 'array<struct<id:string,date:string,hour:float,calories:float,distance:float,bpm:float,mindfulness_session:boolean,steps:float,age:string,gender:string,bmi:string>>')) AS data")
flattened_df = parsed_df.select(col("data.*"))
#select only col distance and BPM
flattened_df = flattened_df.select('id',"distance", "bpm")
# make the ID the key for the document and the distances are list of values and BPM are list of values and push to monoDB group by ID
flattened_df = flattened_df.groupBy("id").agg(collect_list("distance").alias("distances"), collect_list("bpm").alias("bpms"))
flattened_df = flattened_df.withColumn("distances", col("distances").cast("array<float>"))
flattened_df = flattened_df.withColumn("bpms", col("bpms").cast("array<float>"))


query=flattened_df.writeStream.format("mongodb") \
  .option("checkpointLocation", "/tmp/pyspark/")\
  .option("forceDeleteTempCheckpointLocation", "true")\
  .option("spark.mongodb.connection.uri", connection_string) \
  .option("spark.mongodb.database", "healthcare") \
  .option("spark.mongodb.collection", "userProgress") \
  .outputMode('complete')\
  .start()


query.awaitTermination()


