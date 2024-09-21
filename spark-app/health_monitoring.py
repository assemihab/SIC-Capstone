from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json

connection_string = "mongodb+srv://yahya:wuOCBUNsQ856HP3Z@cluster0.7wbr9.mongodb.net/healthcare.streaming?retryWrites=true&w=majority"

# Create Spark session with Cassandra connection options using the secure connect bundle
spark = SparkSession.builder \
  .appName("Healthcare Monitoring") \
  .master("local[*]") \
  .config("spark.mongodb.spark.enabled", "true") \
  .config("spark.mongodb.read.connection.uri", connection_string) \
  .config("spark.mongodb.write.connection.uri", connection_string) \
  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# SparkContext from sparkSession to reduce written code only
sc = spark.sparkContext

# Schema (This Schema is also the same in Cassandra on DataStax and mongoDB)
schema = StructType([
  StructField("id", StringType(), False),
  StructField("date", TimestampType(), False),
  StructField("hour", DoubleType(), False),
  StructField("temperature", DoubleType(), False),
  StructField("age", StringType(), True),
  StructField("bmi", StringType(), True),
  StructField("bpm", DoubleType(), True),
  StructField("calories", DoubleType(), True),
  StructField("distance", DoubleType(), True),
  StructField("gender", StringType(), True),
  StructField("steps", DoubleType(), True),
])

# Test kafka connection
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "my-topic") \
  .load()


df = df. \
  selectExpr("CAST(value AS STRING)"). \
  select(from_json("value",schema).alias("tmp")). \
  select("tmp.*")

################################ Processing will be here ############################

# Write Streaming Data to mongoDB atlas
query = df.writeStream.format("mongodb") \
  .option("spark.mongodb.write.connection.uri", connection_string) \
  .option("database", "healthcare") \
  .option("collection", "streaming") \
  .option("checkpointLocation", "/tmp/checkpoint/old") \
  .outputMode("append") \
  .start()

query.awaitTermination()
