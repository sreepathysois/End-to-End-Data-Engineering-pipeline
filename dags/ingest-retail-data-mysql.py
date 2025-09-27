from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import mysql.connector
import io

# MinIO client setup
minio_client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

BUCKET = "raw-retail-data"
TABLES = ["categories", "customers", "departments", "order_items", "orders", "products"]

# MySQL connection info
MYSQL_CONFIG = {
    "host": "172.18.181.60",  # use host.docker.internal if MySQL runs on localhost outside docker
    "user": "cdc",
    "password": "msis@123",
    "database": "retaildb"
}

def ingest_table_to_minio(table_name, **context):
    # Create bucket if it doesn't exist
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
    
    # Connect to MySQL
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    # Convert to CSV and upload to MinIO
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    minio_client.put_object(
        BUCKET,
        f"{table_name}.csv",
        io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="application/csv"
    )
    print(f"Uploaded {table_name}.csv to MinIO")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "mysql_to_minio_ingestion",
    default_args=default_args,
    description="Ingest all MySQL tables into MinIO raw zone",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "minio", "mysql"],
) as dag:

    for table in TABLES:
        PythonOperator(
            task_id=f"ingest_{table}",
            python_callable=ingest_table_to_minio,
            op_kwargs={"table_name": table}
        )

