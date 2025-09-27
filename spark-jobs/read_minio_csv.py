from pyspark.sql import SparkSession

# MinIO (S3)
minio_endpoint = "http://minio:9000"
minio_access_key = "minio"
minio_secret_key = "minio123"
bucket_path = "raw-data/sample_data.csv"

def get_spark():
    spark = (
        SparkSession.builder.appName("Read_MinIO_CSV")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # Overriding timeouts to avoid 60s issue
        .config("spark.network.timeout", "60")
        .config("spark.executor.heartbeatInterval", "60")
        .config("spark.sql.broadcastTimeout", "60")
        .config("fs.s3a.connection.timeout", "60")
        .config("fs.s3a.connection.establish.timeout", "60")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "15000")
        .getOrCreate()
    )
    return spark

if __name__ == "__main__":
    spark = get_spark()
    df = spark.read.option("header", "true").csv(f"s3a://{bucket_path}")
    df.show()
    spark.stop()

