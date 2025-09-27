from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import io

# MinIO client setup
minio_client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

RAW_BUCKET = "raw-retail-data"
SILVER_BUCKET = "silver-retail-data"

def read_raw_csv(table_name):
    data = minio_client.get_object(RAW_BUCKET, f"{table_name}.csv")
    df = pd.read_csv(data)
    return df

def transform_retail_data(**context):
    # Read raw CSVs
    customers = read_raw_csv("customers")
    departments = read_raw_csv("departments")
    categories = read_raw_csv("categories")
    products = read_raw_csv("products")
    orders = read_raw_csv("orders")
    order_items = read_raw_csv("order_items")

    # Clean and standardize
    customers.fillna("", inplace=True)
    customers["customer_fname"] = customers["customer_fname"].str.strip()
    customers["customer_lname"] = customers["customer_lname"].str.strip()
    orders["order_date"] = pd.to_datetime(orders["order_date"])

    # Merge dimensions
    categories = categories.merge(departments, left_on="category_department_id", right_on="department_id", how="left")
    products = products.merge(categories, left_on="product_category_id", right_on="category_id", how="left")

    # Merge fact table
    order_fact = order_items.merge(orders, left_on="order_item_order_id", right_on="order_id", how="left")
    order_fact = order_fact.merge(customers, left_on="order_customer_id", right_on="customer_id", how="left")
    order_fact = order_fact.merge(products, left_on="order_item_product_id", right_on="product_id", how="left")

    # Save silver tables back to MinIO
    silver_tables = {
        "customer_dim": customers,
        "department_dim": departments,
        "category_dim": categories,
        "product_dim": products,
        "order_fact": order_fact
    }

    for table_name, df in silver_tables.items():
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            SILVER_BUCKET,
            f"{table_name}.csv",
            io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="application/csv"
        )
        print(f"Silver table {table_name} written to MinIO")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "silver_retail_etl",
    default_args=default_args,
    description="Transform raw retail CSVs into silver layer in MinIO",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "silver", "retail"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_retail_data",
        python_callable=transform_retail_data,
    )

