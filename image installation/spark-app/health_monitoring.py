from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import random

# connection_string = "mongodb+srv://yahya:wuOCBUNsQ856HP3Z@cluster0.7wbr9.mongodb.net/healthcare.test?retryWrites=true&w=majority"
connection_string = "mongodb+srv://assem:1231234@cluster0.1kkof.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
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

# Schema
string_schema = "id string, date string, hour string, temperature string, age string, bmi string, bpm string, calories string, distance string, gender string, steps string"
# schema = StructType([
#   StructField("id", StringType(), False),
#   StructField("date", TimestampType(), False),
#   StructField("hour", DoubleType(), False),
#   StructField("temperature", DoubleType(), False),
#   StructField("age", StringType(), True),
#   StructField("bmi", StringType(), True),
#   StructField("bpm", DoubleType(), True),
#   StructField("calories", DoubleType(), True),
#   StructField("distance", DoubleType(), True),
#   StructField("gender", StringType(), True),
#   StructField("steps", DoubleType(), True),
# ])

# Get Data from kafka topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9093") \
  .option("subscribe", "mindf") \
  .load()

################################ Processing will be here ###############################

# convert from json format to string
df = df. \
  selectExpr("CAST(value AS STRING)"). \
  select(from_json("value", string_schema).alias("tmp")). \
  select("tmp.*")

# cast all columns from string to specified types
df_casted = df.withColumn("date", col("date").cast(DateType())) \
              .withColumn("hour", col("hour").cast(DoubleType())) \
              .withColumn("temperature", col("temperature").cast(DoubleType())) \
              .withColumn("bpm", col("bpm").cast(DoubleType())) \
              .withColumn("calories", col("calories").cast(DoubleType())) \
              .withColumn("distance", col("distance").cast(DoubleType())) \
              .withColumn("steps", col("steps").cast(IntegerType()))

# Drop rows with more than 6 nulls
total_columns = len(df_casted.columns)
df_dropped = df_casted.dropna(thresh=(total_columns-6))


############################### handling outliers ###############################

# Function to replace outliers
def replace_outliers(df):
    for name in ["calories", "distance", "steps"]:
        percentile25 = df.approxQuantile(name, [0.25], 0.01)[0]
        percentile75 = df.approxQuantile(name, [0.75], 0.01)[0]
        iqr = percentile75 - percentile25
        upper_limit = percentile75 + 3 * iqr
        lower_limit = percentile25 - 3 * iqr
        
        df = df.withColumn(name, F.when(col(name) > upper_limit, upper_limit)
                                  .when(col(name) < lower_limit, lower_limit)
                                  .otherwise(col(name)))
    
    bpm_mean = df.agg(mean("bpm")).first()[0]
    bpm_stddev = df.agg(stddev("bpm")).first()[0]
    upper_limit_bpm = bpm_mean + 3 * bpm_stddev
    lower_limit_bpm = bpm_mean - 3 * bpm_stddev
    
    df = df.withColumn("bpm", F.when(col("bpm") > upper_limit_bpm, upper_limit_bpm)
                          .when(col("bpm") < lower_limit_bpm, lower_limit_bpm)
                          .otherwise(col("bpm")))
    
    return df

# Step 6: Fill nulls with specific logic (alternative approaches)
# Custom logic: Fill nulls in hour, temperature, bpm, calories, distance, and steps


# Function to fill nulls in 'bmi' column with random values from the 'index' column
def fill_nulls_with_random(df_batch):
    
    index_list = df.select("bmi").distinct().rdd.flatMap(lambda x: x).collect()

    random_value = random.choice(index_list)  # Choose a random value from 'index' list
    df_filled = df_batch.withColumn(
        "bmi", when(col("bmi").isNull(), random_value).otherwise(col("bmi"))
    )
    return df_filled

# Function to calculate the mode for categorical columns
def get_mode(df, column):
    mode_df = df.groupBy(column).count().orderBy(F.desc("count")).limit(1)
    return mode_df.collect()[0][0] if mode_df.count() > 0 else None

# Function to process each micro-batch and fill nulls
def process_batch(df_batch, epoch_id):
    # Compute mean for numerical columns
    bpm_mean = df_batch.select(avg("bpm")).first()[0] or 0
    calories_mean = df_batch.select(avg("calories")).first()[0] or 0
    distance_mean = df_batch.select(avg("distance")).first()[0] or 0
    steps_mean = df_batch.select(avg("steps")).first()[0] or 0

    # Compute mode for categorical columns
    gender_mode = get_mode(df_batch, "gender")
    age_mode = get_mode(df_batch, "age")

    # Fill null values using the computed means and modes
    df_filled = df_batch.fillna({
      'bpm': bpm_mean,
      'calories': calories_mean,
      'distance': distance_mean,
      'steps': steps_mean,
      'gender': gender_mode,
      'age': age_mode
    })

    return df_filled

df_dropped = df_dropped.dropna(subset=["id", "date", "hour"])

# Directly concatenate 'date' and the integer part of 'hour' (without creating intermediate columns)
df_dropped = df_dropped.withColumn(
    "time",
    to_timestamp(
        concat(
            col("date").cast("string"),             # Cast date to string
            lit(" "),                               # Add a space
            lpad(floor(col("hour")).cast("string"), 2, "0")  # Extract integer part of hour, pad to 2 digits
        ), 
        "yyyy-MM-dd HH"  # Specify the timestamp format
    )
)

################################# Write Streaming Data to mongoDB atlas ########################################
query = df_dropped.writeStream \
  .foreachBatch(lambda batch_df, _: replace_outliers(batch_df))\
  .foreachBatch(lambda batch_df, _: fill_nulls_with_random(batch_df)) \
  .foreachBatch(process_batch) \
  .format("mongodb") \
  .option("spark.mongodb.write.connection.uri", connection_string) \
  .option("database", "healthcare") \
  .option("collection", "test") \
  .option("checkpointLocation", "/tmp/checkpoint/test") \
  .outputMode("append") \
  .start()
query.awaitTermination()

################################### write to the console ####################################################
# query = df_dropped \
# .writeStream \
# .foreachBatch(lambda batch_df, _: replace_outliers(batch_df)) \
# .foreachBatch(lambda batch_df, _: fill_nulls_with_random(batch_df)) \
# .foreachBatch(process_batch) \
# .format("console").outputMode("append").start()

