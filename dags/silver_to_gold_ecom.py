from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import io
import pandas as pd
import psycopg2

# ---------- MinIO CONFIG (align with your style) ----------
minio_client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

SILVER_BUCKET = "ecom-silver"   # change if needed
GOLD_BUCKET = "ecom-gold"       # change if needed

# ---------- POSTGRES CONFIG (same pattern as your code) ----------
POSTGRES_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432
}

# Name of gold table in Postgres
GOLD_TABLE = "ecom_product_gold"   # change if you want (e.g. 'sales_by_product')


def silver_to_gold(**context):
    execution_date = context["ds_nodash"]

    # ------------ 1) READ SILVER DATA FROM MINIO ------------
    silver_object = f"silver/ecommerce_products_silver_{execution_date}.csv"

    response = minio_client.get_object(SILVER_BUCKET, silver_object)
    silver_bytes = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(io.BytesIO(silver_bytes))

    # ------------ 2) CREATE GOLD FEATURES ------------
    # Ensure numeric
    if "discount_pct" in df.columns:
        df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)
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

    # ------------ 3) WRITE GOLD CSV BACK TO MINIO ------------
    if not minio_client.bucket_exists(GOLD_BUCKET):
        minio_client.make_bucket(GOLD_BUCKET)

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    data = buf.getvalue().encode("utf-8")

    gold_object = f"gold/ecommerce_products_gold_{execution_date}.csv"
    minio_client.put_object(
        GOLD_BUCKET,
        gold_object,
        io.BytesIO(data),
        length=len(data),
        content_type="text/csv",
    )

    print(f"Gold data written to minio://{GOLD_BUCKET}/{gold_object}")

    # ------------ 4) LOAD GOLD DATA INTO POSTGRES (your style) ------------
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    # Drop and recreate table with TEXT columns (simple & generic)
    cols_def = ", ".join([f"{col} TEXT" for col in df.columns])
    cur.execute(f"DROP TABLE IF EXISTS {GOLD_TABLE};")
    cur.execute(f"CREATE TABLE {GOLD_TABLE} ({cols_def});")

    # Insert row by row
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


# ------------ 5) DAG DEFINITION ------------
with DAG(
    dag_id="silver_to_gold_minio_postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "gold", "minio", "postgres"],
) as dag:

    t_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
        provide_context=True,
    )

    t_silver_to_gold

