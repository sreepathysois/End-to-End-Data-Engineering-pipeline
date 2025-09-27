from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import psycopg2
import io

# MinIO client
minio_client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

SILVER_BUCKET = "silver-retail-data"
GOLD_BUCKET = "gold-retail-data"

POSTGRES_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432
}

silver_tables = ["category_dim", "customer_dim", "product_dim", "order_fact"]
gold_tables = ["sales_by_customer", "sales_by_product", "sales_by_department"]

def load_table_to_postgres(bucket_name, table_name, **context):
    # Download CSV from MinIO
    data = minio_client.get_object(bucket_name, f"{table_name}.csv")
    df = pd.read_csv(data)
    
    # Connect to Postgres
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    # Create table dynamically (simple version: overwrite)
    cols = ", ".join([f"{col} TEXT" for col in df.columns])
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(f"CREATE TABLE {table_name} ({cols});")

    # Insert data
    for _, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(df.columns))
        cur.execute(
            f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})",
            tuple(row)
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {table_name} into Postgres")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_postgres",
    default_args=default_args,
    description="Load Silver and Gold data from MinIO to Postgres",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "MinIO", "Postgres"],
) as dag:

    # Silver tables tasks
    silver_tasks = []
    for table in silver_tables:
        task = PythonOperator(
            task_id=f"load_silver_{table}",
            python_callable=load_table_to_postgres,
            op_kwargs={"bucket_name": SILVER_BUCKET, "table_name": table},
        )
        silver_tasks.append(task)

    # Gold tables tasks
    gold_tasks = []
    for table in gold_tables:
        task = PythonOperator(
            task_id=f"load_gold_{table}",
            python_callable=load_table_to_postgres,
            op_kwargs={"bucket_name": GOLD_BUCKET, "table_name": table},
        )
        gold_tasks.append(task)

    # Set dependencies: silver first, then gold
    for silver_task in silver_tasks:
        for gold_task in gold_tasks:
            silver_task >> gold_task

