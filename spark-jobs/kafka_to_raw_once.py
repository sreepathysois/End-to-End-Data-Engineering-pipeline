from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "spark-raw-weather-data")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minio")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minio123")


def get_spark():
    return SparkSession.builder.appName("kafka_to_raw_once") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()


# Define schema
weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", DoubleType())
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType())
    ])),
    StructField("clouds", StructType([
        StructField("all", DoubleType())
    ])),
    StructField("dt", IntegerType()),
    StructField("name", StringType()),
    StructField("_ingest_ts", StringType())
])


def main():
    spark = get_spark()

    raw = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    df = raw.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(from_json(col("json_str"), weather_schema).alias("data")) \
        .select(
            col("data.coord.lon").alias("lon"),
            col("data.coord.lat").alias("lat"),
            col("data.main.temp").alias("temp"),
            col("data.main.humidity").alias("humidity"),
            col("data.wind.speed").alias("wind_speed"),
            col("data.clouds.all").alias("clouds_all"),
            col("data.dt").alias("dt_epoch"),
            col("data.name").alias("city"),
            col("data._ingest_ts").alias("_ingest_ts")
        ) \
        .withColumn("ingest_ts", expr("coalesce(to_timestamp(_ingest_ts), to_timestamp(from_unixtime(dt_epoch)))")) \
        .drop("_ingest_ts")

    df.write.mode("append").parquet(f"s3a://{RAW_BUCKET}/")

    spark.stop()


if __name__ == "__main__":
    main()

