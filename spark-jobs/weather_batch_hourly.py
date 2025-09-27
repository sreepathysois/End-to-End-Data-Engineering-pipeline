# /opt/airflow/spark-jobs/weather_batch_hourly.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime, window, avg, current_timestamp
import os
from pyspark.sql.types import IntegerType

RAW_BUCKET = os.environ.get("RAW_BUCKET", "spark-raw-weather-data")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "spark-weather-silver-data")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minio")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minio123")

POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/airflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASS = os.environ.get("POSTGRES_PASS", "airflow")
POSTGRES_DRIVER = "org.postgresql.Driver"
POSTGRES_TABLE = "weather_bt"

def get_spark():
    return SparkSession.builder.appName("weather_batch_hourly") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def main():
    spark = get_spark()
    df = spark.read.parquet(f"s3a://{RAW_BUCKET}/")

    # assume df has dt_epoch or ingest_ts; try both
    if "dt_epoch" in df.columns:
        df = df.withColumn("event_ts", to_timestamp(from_unixtime(col("dt_epoch").cast(IntegerType()))))
    elif "ingest_ts" in df.columns:
        df = df.withColumn("event_ts", to_timestamp(col("ingest_ts")))
    else:
        df = df.withColumn("event_ts", current_timestamp())

    # filter last hour (based on event_ts)
    from pyspark.sql.functions import current_timestamp, expr
    df_last_hour = df.filter(col("event_ts") >= expr("current_timestamp() - interval 1 hours"))

    hourly = df_last_hour.groupBy(window(col("event_ts"), "1 hour").alias("w"), col("city")) \
        .agg(avg(col("temp")).alias("avg_temp"), avg(col("humidity")).alias("avg_humidity")) \
        .select(col("city"), col("w.start").alias("window_start"), col("w.end").alias("window_end"), "avg_temp", "avg_humidity")

    out_path = f"s3a://{SILVER_BUCKET}/hourly/"
    hourly.write.mode("overwrite").parquet(out_path)

    # to Postgres
    hourly_to_pg = hourly.withColumn("window_start_ts", col("window_start")).withColumn("window_end_ts", col("window_end"))
    (hourly_to_pg.write
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

if __name__ == "__main__":
    main()

