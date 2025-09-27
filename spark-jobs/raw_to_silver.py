from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, DateType

# MinIO config
minio_endpoint = "http://minio:9000"
minio_access_key = "minio"
minio_secret_key = "minio123"
raw_bucket = "spark-raw-retail-data"
silver_bucket = "spark-silver-retail-data"

def get_spark():
    spark = (
        SparkSession.builder.appName("Raw_to_Silver")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark

def read_csv(spark, table):
    path = f"s3a://{raw_bucket}/{table}/*.csv"
    df = spark.read.option("header", True).csv(path)
    return df

if __name__ == "__main__":
    spark = get_spark()

    # ---------------- Read raw CSVs ----------------
    customers = read_csv(spark, "customers")
    departments = read_csv(spark, "departments")
    categories = read_csv(spark, "categories")
    products = read_csv(spark, "products")
    orders = read_csv(spark, "orders")
    order_items = read_csv(spark, "order_items")

    # ---------------- Dimension Tables ----------------
    customer_dim = customers.select(
        "customer_id", "customer_fname", "customer_lname", "customer_email",
        "customer_street", "customer_city", "customer_state", "customer_zipcode"
    )
    department_dim = departments
    category_dim = categories.join(departments, 
                                   categories["category_department_id"] == departments["department_id"], 
                                   "left") \
                             .select("category_id", "category_name", "department_id", "department_name")
    product_dim = products.join(categories, 
                                products["product_category_id"] == categories["category_id"], 
                                "left") \
                          .select("product_id", "product_name", "category_id", "category_name")

    # ---------------- Fact Table ----------------
    order_fact = order_items.join(orders, order_items["order_item_order_id"] == orders["order_id"], "inner") \
                            .join(customers, orders["order_customer_id"] == customers["customer_id"], "inner") \
                            .join(products, order_items["order_item_product_id"] == products["product_id"], "left") \
                            .join(categories, products["product_category_id"] == categories["category_id"], "left") \
                            .join(departments, categories["category_department_id"] == departments["department_id"], "left") \
                            .select(
                                orders["order_id"], orders["order_date"],
                                customers["customer_id"], customers["customer_fname"], customers["customer_lname"],
                                products["product_id"], products["product_name"],
                                categories["category_id"], categories["category_name"],
                                departments["department_id"], departments["department_name"],
                                order_items["order_item_quantity"], order_items["order_item_subtotal"]
                            )

    # ---------------- Write to Silver ----------------
    silver_tables = {
        "customer_dim": customer_dim,
        "department_dim": department_dim,
        "category_dim": category_dim,
        "product_dim": product_dim,
        "order_fact": order_fact
    }

    for table_name, df in silver_tables.items():
        output_path = f"s3a://{silver_bucket}/{table_name}/"
        df.write.mode("overwrite").option("header", True).csv(output_path)
        print(f"Written {table_name} to {output_path}")

    spark.stop()

