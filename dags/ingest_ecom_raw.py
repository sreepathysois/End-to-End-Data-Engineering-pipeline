from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE = False
RAW_BUCKET = "ecom-raw"

LOCAL_RAW_PATH = "/opt/airflow/sample_data/ecommerce_products_raw.csv"

def upload_raw_to_minio(**context):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    # create bucket if not exists
    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)

    execution_date = context["ds_nodash"]
    object_name = f"raw/ecommerce_products_raw_{execution_date}.csv"

    client.fput_object(
        RAW_BUCKET,
        object_name,
        LOCAL_RAW_PATH,
    )

    print(f"Uploaded {LOCAL_RAW_PATH} to minio://{RAW_BUCKET}/{object_name}")

with DAG(
    dag_id="raw_to_minio_raw_bucket",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "raw", "minio"],
) as dag:

    upload_raw = PythonOperator(
        task_id="upload_raw_csv_to_minio",
        python_callable=upload_raw_to_minio,
        provide_context=True,
    )

    upload_raw

