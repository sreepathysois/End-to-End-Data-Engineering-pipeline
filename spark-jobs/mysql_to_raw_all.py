from pyspark.sql import SparkSession

mysql_host = "172.18.181.60"
mysql_user = "cdc"
mysql_password = "msis@123"
mysql_db = "retaildb"

minio_endpoint = "http://minio:9000"
minio_access_key = "minio"
minio_secret_key = "minio123"
raw_bucket = "spark-raw-retail-data"

tables = ["categories", "customers", "departments", "order_items", "orders", "products"]

def get_spark():
    return (
        SparkSession.builder.appName("MySQL_to_Raw_Minio")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

def ingest_table(spark, table):
    jdbc_url = f"jdbc:mysql://{mysql_host}:3306/{mysql_db}"
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", mysql_user)
        .option("password", mysql_password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )
    output_path = f"s3a://{raw_bucket}/{table}/"
    #df.write.mode("overwrite").parquet(output_path)
    #df.write.mode("overwrite").csv(output_path).option("header", True)
    df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

if __name__ == "__main__":
    spark = get_spark()
    for table in tables:
        ingest_table(spark, table)
    spark.stop()

