from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import io
import pandas as pd

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE = False

RAW_BUCKET = "ecom-raw"
SILVER_BUCKET = "ecom-silver"

def _get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

def process_raw_to_silver(**context):
    client = _get_minio_client()

    if not client.bucket_exists(SILVER_BUCKET):
        client.make_bucket(SILVER_BUCKET)

    execution_date = context["ds_nodash"]
    raw_object = f"raw/ecommerce_products_raw_{execution_date}.csv"

    response = client.get_object(RAW_BUCKET, raw_object)
    raw_bytes = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(io.BytesIO(raw_bytes))

    # Drop unwanted raw-only fields
    unwanted_cols = ["tmp_notes", "internal_flag", "extra_col1", "extra_col2"]
    existing_unwanted = [c for c in unwanted_cols if c in df.columns]
    df = df.drop(columns=existing_unwanted)

    # Basic cleaning
    # Strip spaces
    df.columns = [c.strip() for c in df.columns]

    # Convert price to numeric and fill missing with median
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    median_price = df["price"].median()
    df["price"] = df["price"].fillna(median_price)

    # stock_qty
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)

    # rating
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["rating"] = df["rating"].fillna(df["rating"].mean())

    # num_reviews
    if "num_reviews" in df.columns:
        df["num_reviews"] = pd.to_numeric(df["num_reviews"], errors="coerce").fillna(0).astype(int)

    # category & subcategory
    for col in ["category", "subcategory", "brand", "color", "size", "material", "country_of_origin"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").replace("", "Unknown")

    # is_active – keep only active products
    if "is_active" in df.columns:
        df["is_active"] = df["is_active"].astype(str).str.lower().isin(["true", "1", "yes"])
        df = df[df["is_active"]]

    # ensure product_id is int
    if "product_id" in df.columns:
        df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")

    # Write back to CSV in memory
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    data = buf.getvalue().encode("utf-8")

    silver_object = f"silver/ecommerce_products_silver_{execution_date}.csv"
    client.put_object(
        SILVER_BUCKET,
        silver_object,
        io.BytesIO(data),
        length=len(data),
        content_type="text/csv",
    )

    print(f"Silver data written to minio://{SILVER_BUCKET}/{silver_object}")

with DAG(
    dag_id="raw_to_silver_minio",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "silver", "minio"],
) as dag:

    raw_to_silver = PythonOperator(
        task_id="process_raw_to_silver",
        python_callable=process_raw_to_silver,
        provide_context=True,
    )

    raw_to_silver

