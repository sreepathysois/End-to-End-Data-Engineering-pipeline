from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Kafka topic and MinIO bucket
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "weather"
raw_bucket = "weather-raw"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WeatherKafkaToCSV") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Convert value from bytes to string
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Optional: define schema for weather JSON
weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("weather", StringType()),  # For simplicity, store as JSON string
    StructField("base", StringType()),
    StructField("main", StringType()),
    StructField("wind", StringType()),
    StructField("clouds", StringType()),
    StructField("dt", LongType()),
    StructField("sys", StringType()),
    StructField("timezone", LongType()),
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("_ingest_ts", StringType()),
    StructField("_city", StringType())
])

# Optional: parse JSON if needed
# df = df.withColumn("json_data", from_json(col("json_str"), weather_schema))

# Write to MinIO CSV
query = df.writeStream \
    .format("csv") \
    .option("path", f"s3a://{raw_bucket}/") \
    .option("checkpointLocation", "/tmp/checkpoint_weather") \
    .trigger(processingTime="5 minutes") \
    .start()

query.awaitTermination()

