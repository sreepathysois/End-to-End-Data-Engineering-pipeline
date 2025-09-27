# /opt/airflow/spark-jobs/weather_stream_agg_5min.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_timestamp, col, window, avg, when, from_json
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import os

# -----------------------
# Environment variables
# -----------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "spark-raw-weather-data")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "spark-weather-silver-data")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minio")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minio123")

POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/airflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASS = os.environ.get("POSTGRES_PASS", "airflow")
POSTGRES_DRIVER = "org.postgresql.Driver"
POSTGRES_TABLE = "weather_rt"

# -----------------------
# Spark session builder
# -----------------------
def get_spark():
    return SparkSession.builder.appName("weather_stream_agg_5min") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

# -----------------------
# Main processing
# -----------------------
def main():
    spark = get_spark()

    # Read Kafka data (micro-batch style)
    raw = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    if raw.rdd.isEmpty():
        print("No data in Kafka topic, exiting job.")
        spark.stop()
        return

    # Cast value as string
    json_df = raw.selectExpr("CAST(value AS STRING) AS json_str")

    # Optional debug
    # json_df.show(5, truncate=False)

    # Define schema
    schema = StructType([
        StructField("main", StructType([
            StructField("temp", DoubleType()),
            StructField("humidity", DoubleType())
        ])),
        StructField("dt", IntegerType()),
        StructField("name", StringType()),
        StructField("_ingest_ts", StringType())
    ])

    # Parse JSON safely
    parsed = json_df.select(from_json(col("json_str"), schema).alias("d"))

    # Only select rows where parsing succeeded
    parsed = parsed.filter(col("d").isNotNull())

    parsed = parsed.select(
        col("d.name").alias("city"),
        col("d.main.temp").alias("temp"),
        col("d.main.humidity").alias("humidity"),
        col("d.dt").alias("dt_epoch"),
        col("d._ingest_ts").alias("_ingest_ts")
    )

    # Compute event timestamp with fallback
    parsed = parsed.withColumn(
        "event_ts",
        when(col("_ingest_ts").isNotNull(), to_timestamp(col("_ingest_ts")))
        .otherwise(to_timestamp(from_unixtime(col("dt_epoch"))))
    ).drop("_ingest_ts")

    parsed = parsed.filter(col("event_ts").isNotNull())

    # 5-minute tumbling window aggregation by city
    agg = parsed.groupBy(window(col("event_ts"), "5 minutes").alias("w"), col("city")) \
        .agg(
            avg(col("temp")).alias("avg_temp"),
            avg(col("humidity")).alias("avg_humidity")
        ) \
        .select(
            col("city"),
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("avg_temp"),
            col("avg_humidity")
        )

    # Write to MinIO (silver)
    silver_out = f"s3a://{SILVER_BUCKET}/5min/"
    agg.write.mode("overwrite").parquet(silver_out)

    # Write to Postgres using JDBC
    agg_to_pg = agg.withColumn("window_start_ts", col("window_start")) \
                   .withColumn("window_end_ts", col("window_end"))

    (agg_to_pg.write
        .mode("append")
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", POSTGRES_DRIVER)
        .save()
    )

    spark.stop()

# -----------------------
# Entry point
# -----------------------
if __name__ == "__main__":
    main()

