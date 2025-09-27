from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col, sum as _sum, countDistinct

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder.appName("Silver_to_Gold")
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
# Load Silver Tables
# -----------------------------
customers = spark.read.option("header", True).csv(f"{silver_bucket}/customer_dim/")
departments = spark.read.option("header", True).csv(f"{silver_bucket}/department_dim/")
categories = spark.read.option("header", True).csv(f"{silver_bucket}/category_dim/")
products = spark.read.option("header", True).csv(f"{silver_bucket}/product_dim/")
order_fact = spark.read.option("header", True).csv(f"{silver_bucket}/order_fact/")

# -----------------------------
# Convert order_date from ISO 8601 to date
# -----------------------------
order_fact = order_fact.withColumn(
    "order_date",
    to_date(to_timestamp(col("order_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
)

# -----------------------------
# Gold Table: Sales by Customer
# -----------------------------
sales_by_customer = (
    order_fact.groupBy("customer_id", "customer_fname", "customer_lname")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("order_item_subtotal").alias("total_revenue")
    )
)

# -----------------------------
# Gold Table: Sales by Product
# -----------------------------
sales_by_product = (
    order_fact.groupBy("product_id", "product_name")
    .agg(
        _sum("order_item_quantity").alias("total_quantity"),
        _sum("order_item_subtotal").alias("total_revenue")
    )
)

# -----------------------------
# Gold Table: Sales by Category
# -----------------------------
sales_by_category = (
    order_fact.groupBy("category_id", "category_name")
    .agg(
        _sum("order_item_quantity").alias("total_quantity"),
        _sum("order_item_subtotal").alias("total_revenue")
    )
)

# -----------------------------
# Gold Table: Sales by Department
# -----------------------------
sales_by_department = (
    order_fact.groupBy("department_id", "department_name")
    .agg(
        _sum("order_item_quantity").alias("total_quantity"),
        _sum("order_item_subtotal").alias("total_revenue")
    )
)

# -----------------------------
# Gold Table: Orders Time Series
# -----------------------------
orders_time_series = (
    order_fact.groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("order_item_subtotal").alias("total_revenue")
    )
)

# -----------------------------
# Write Gold Tables to MinIO
# -----------------------------
gold_tables = {
    "sales_by_customer": sales_by_customer,
    "sales_by_product": sales_by_product,
    "sales_by_category": sales_by_category,
    "sales_by_department": sales_by_department,
    "orders_time_series": orders_time_series
}

for table_name, df in gold_tables.items():
    output_path = f"{gold_bucket}/{table_name}/"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Gold table {table_name} written to {output_path}")

spark.stop()

