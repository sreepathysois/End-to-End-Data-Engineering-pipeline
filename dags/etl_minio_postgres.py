from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import psycopg2
import io
import os

# MinIO client setup
minio_client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

BUCKET = "raw-data"
LOCAL_FILE = "/opt/airflow/sample_data/sample_data.csv"  # Path inside container

def ingest_to_minio(**context):
    if not os.path.exists(LOCAL_FILE):
        raise FileNotFoundError(f"File not found: {LOCAL_FILE}")

    file_size = os.path.getsize(LOCAL_FILE)
    with open(LOCAL_FILE, "rb") as f:
        minio_client.put_object(
            BUCKET,
            "sample_data.csv",
            f,
            length=file_size,
            content_type="application/csv"
        )
    print("Uploaded local sample_data.csv to MinIO")

def transform_and_load(**context):
    # Download from MinIO
    data = minio_client.get_object(BUCKET, "sample_data.csv")
    df = pd.read_csv(data)

    # Simple transformation
    df["amount"] = df["amount"] * 1.1

    # Load into Postgres
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS transactions;")
    cur.execute("CREATE TABLE transactions (id INT, name VARCHAR, amount FLOAT);")

    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO transactions (id, name, amount) VALUES (%s, %s, %s)",
            (row["id"], row["name"], row["amount"])
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Data loaded into Postgres")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_minio_postgres",
    default_args=default_args,
    description="Ingest data to MinIO and load to Postgres",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "minio", "postgres"],
) as dag:

    task_ingest = PythonOperator(
        task_id="ingest_to_minio",
        python_callable=ingest_to_minio,
    )

    task_transform_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    task_ingest >> task_transform_load

