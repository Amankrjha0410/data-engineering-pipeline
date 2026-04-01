# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, IntegerType, StringType

# Create Spark Session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define Schema
schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("amount", IntegerType()) \
    .add("event_time", StringType())

# Read Stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value to String
json_df = kafka_df.selectExpr("CAST(value as STRING)")

# PARSE JSON DATA
parsed_df = json_df.select(from_json(col("value").schema).alias("data")).select("data.*")

# DATA CLEANSING & TRANSFORMATIONS
clean_df = parsed_df \
    .filter(col("amount")>0) \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .withColumn("processing_time", current_timestamp())

# WRITE OUTPUTS TO CONSOLE
query = clean_df.writeStream \
    .format("cosole") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

# KEEP STREAM RUNNING
query.awaitTermination()