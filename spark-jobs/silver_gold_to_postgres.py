from pyspark.sql import SparkSession

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder.appName("SilverGold_to_Postgres")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# -----------------------------
# Paths
# -----------------------------
silver_bucket = "s3a://spark-silver-retail-data"
gold_bucket = "s3a://spark-gold-retail-data"

# -----------------------------
# PostgreSQL Config
# -----------------------------
postgres_url = "jdbc:postgresql://postgres:5432/airflow"
postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# -----------------------------
# Load Silver Tables
# -----------------------------
silver_tables = ["customer_dim", "department_dim", "category_dim", "product_dim", "order_fact"]
for table in silver_tables:
    df = spark.read.option("header", True).csv(f"{silver_bucket}/{table}/")
    df.write.jdbc(url=postgres_url, table=table, mode="overwrite", properties=postgres_properties)
    print(f"Silver table {table} written to PostgreSQL")

# -----------------------------
# Load Gold Tables
# -----------------------------
gold_tables = ["sales_by_customer", "sales_by_product", "sales_by_category", "sales_by_department", "orders_time_series"]
for table in gold_tables:
    df = spark.read.parquet(f"{gold_bucket}/{table}/")
    df.write.jdbc(url=postgres_url, table=table, mode="overwrite", properties=postgres_properties)
    print(f"Gold table {table} written to PostgreSQL")

spark.stop()

