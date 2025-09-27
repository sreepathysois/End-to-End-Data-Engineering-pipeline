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

SILVER_BUCKET = "silver-retail-data"
GOLD_BUCKET = "gold-retail-data"

def read_silver_csv(table_name):
    """Read CSV from MinIO silver bucket"""
    data = minio_client.get_object(SILVER_BUCKET, f"{table_name}.csv")
    df = pd.read_csv(data)
    return df

def generate_gold_tables(**context):
    """Generate Gold layer tables from Silver layer"""
    # Load silver tables
    customers = read_silver_csv("customer_dim")
    departments = read_silver_csv("department_dim")
    categories = read_silver_csv("category_dim")
    products = read_silver_csv("product_dim")
    order_fact = read_silver_csv("order_fact")

    # Ensure order_date is datetime
    order_fact["order_date"] = pd.to_datetime(order_fact["order_date"], errors="coerce")

    # ----- Gold Table: Sales by Customer -----
    sales_by_customer = order_fact.groupby(
        ["customer_id", "customer_fname", "customer_lname"]
    ).agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc="nunique"),
        total_revenue=pd.NamedAgg(column="order_item_subtotal", aggfunc="sum")
    ).reset_index()

    # ----- Gold Table: Sales by Product -----
    sales_by_product = order_fact.groupby(
        ["product_id", "product_name"]
    ).agg(
        total_quantity=pd.NamedAgg(column="order_item_quantity", aggfunc="sum"),
        total_revenue=pd.NamedAgg(column="order_item_subtotal", aggfunc="sum")
    ).reset_index()

    # ----- Gold Table: Sales by Category -----
    sales_by_category = order_fact.groupby(
        ["category_id", "category_name"]
    ).agg(
        total_quantity=pd.NamedAgg(column="order_item_quantity", aggfunc="sum"),
        total_revenue=pd.NamedAgg(column="order_item_subtotal", aggfunc="sum")
    ).reset_index()

    # ----- Gold Table: Sales by Department -----
    sales_by_department = order_fact.groupby(
        ["department_id", "department_name"]
    ).agg(
        total_quantity=pd.NamedAgg(column="order_item_quantity", aggfunc="sum"),
        total_revenue=pd.NamedAgg(column="order_item_subtotal", aggfunc="sum")
    ).reset_index()

    # ----- Gold Table: Orders Time Series -----
    orders_time_series = order_fact.groupby(
        order_fact["order_date"].dt.date
    ).agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc="nunique"),
        total_revenue=pd.NamedAgg(column="order_item_subtotal", aggfunc="sum")
    ).reset_index().rename(columns={"order_date": "date"})

    # Save Gold Tables to MinIO
    gold_tables = {
        "sales_by_customer": sales_by_customer,
        "sales_by_product": sales_by_product,
        "sales_by_category": sales_by_category,
        "sales_by_department": sales_by_department,
        "orders_time_series": orders_time_series
    }

    for table_name, df in gold_tables.items():
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            GOLD_BUCKET,
            f"{table_name}.csv",
            io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="application/csv"
        )
        print(f"Gold table {table_name} written to MinIO")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    "gold_retail_etl",
    default_args=default_args,
    description="Transform silver retail tables into gold layer metrics",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "gold", "retail"],
) as dag:

    gold_task = PythonOperator(
        task_id="generate_gold_tables",
        python_callable=generate_gold_tables,
    )

