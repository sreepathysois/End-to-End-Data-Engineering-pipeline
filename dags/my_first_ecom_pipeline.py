from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import io
import pandas as pd
import psycopg2

# ----------------- COMMON CONFIG -----------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE = False

RAW_BUCKET = "ecom-raw"
SILVER_BUCKET = "ecom-silver"
GOLD_BUCKET = "ecom-gold"

LOCAL_RAW_PATH = "/opt/airflow/sample_data/ecommerce_products_raw.csv"

POSTGRES_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

GOLD_TABLE = "ecom_product_gold"


def _get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

# ----------------- TASK 1: RAW → MINIO (RAW BUCKET) -----------------
def upload_raw_to_minio(**context):
    client = _get_minio_client()

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


# ----------------- TASK 2: RAW → SILVER -----------------
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
    df.columns = [c.strip() for c in df.columns]

    # price
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    median_price = df["price"].median()
    df["price"] = df["price"].fillna(median_price)

    # stock_qty
    df["stock_qty"] = (
        pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)
    )

    # rating
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["rating"] = df["rating"].fillna(df["rating"].mean())

    # num_reviews
    if "num_reviews" in df.columns:
        df["num_reviews"] = (
            pd.to_numeric(df["num_reviews"], errors="coerce").fillna(0).astype(int)
        )

    # category & subcategory & other text dims
    for col in [
        "category",
        "subcategory",
        "brand",
        "color",
        "size",
        "material",
        "country_of_origin",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").replace("", "Unknown")

    # is_active – keep only active products
    if "is_active" in df.columns:
        df["is_active"] = (
            df["is_active"].astype(str).str.lower().isin(["true", "1", "yes"])
        )
        df = df[df["is_active"]]

    # ensure product_id is int-like
    if "product_id" in df.columns:
        df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype(
            "Int64"
        )

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


# ----------------- TASK 3: SILVER → GOLD + POSTGRES -----------------
def silver_to_gold(**context):
    execution_date = context["ds_nodash"]
    client = _get_minio_client()

    silver_object = f"silver/ecommerce_products_silver_{execution_date}.csv"

    response = client.get_object(SILVER_BUCKET, silver_object)
    silver_bytes = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(io.BytesIO(silver_bytes))

    # Ensure numeric
    if "discount_pct" in df.columns:
        df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(
            0
        )
    else:
        df["discount_pct"] = 0

    df["price"] = pd.to_numeric(df.get("price", 0), errors="coerce").fillna(0)

    # effective_price = price * (1 - discount_pct/100)
    df["effective_price"] = df["price"] * (1 - df["discount_pct"] / 100)

    # Keep only useful columns for gold
    keep_cols = [
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "price",
        "discount_pct",
        "effective_price",
        "stock_qty",
        "rating",
        "num_reviews",
        "country_of_origin",
        "seller_id",
        "created_at",
        "updated_at",
        "is_active",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]

    # Save gold to MinIO
    if not client.bucket_exists(GOLD_BUCKET):
        client.make_bucket(GOLD_BUCKET)

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    data = buf.getvalue().encode("utf-8")

    gold_object = f"gold/ecommerce_products_gold_{execution_date}.csv"
    client.put_object(
        GOLD_BUCKET,
        gold_object,
        io.BytesIO(data),
        length=len(data),
        content_type="text/csv",
    )

    print(f"Gold data written to minio://{GOLD_BUCKET}/{gold_object}")

    # Load gold into Postgres
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cols_def = ", ".join([f"{col} TEXT" for col in df.columns])
    cur.execute(f"DROP TABLE IF EXISTS {GOLD_TABLE};")
    cur.execute(f"CREATE TABLE {GOLD_TABLE} ({cols_def});")

    col_names = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {GOLD_TABLE} ({col_names}) VALUES ({placeholders});"

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else str(v) for v in row.tolist()]
        cur.execute(insert_sql, values)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(df)} rows into Postgres table {GOLD_TABLE}")


# ----------------- DAG DEFINITION -----------------
with DAG(
    dag_id="myfirst_pipeline_ecom",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "raw", "silver", "gold", "minio", "postgres"],
) as dag:

    t_upload_raw = PythonOperator(
        task_id="upload_raw_csv_to_minio",
        python_callable=upload_raw_to_minio,
        provide_context=True,
    )

    t_raw_to_silver = PythonOperator(
        task_id="process_raw_to_silver",
        python_callable=process_raw_to_silver,
        provide_context=True,
    )

    t_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
        provide_context=True,
    )

    # Orchestration: RAW → SILVER → GOLD
    t_upload_raw >> t_raw_to_silver >> t_silver_to_gold

