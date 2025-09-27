from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    "retail_master_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "master"],
) as dag:

    # ---- Raw ingestion ----
    ingest_mysql_to_raw = SparkSubmitOperator(
        task_id="ingest_mysql_to_raw",
        application="/opt/airflow/spark-jobs/mysql_to_raw_all.py",
        conn_id="spark_default",
        jars="/opt/jars/mysql-connector-java-8.0.29.jar,"
             "/opt/jars/hadoop-aws-3.4.1.jar,"
             "/opt/jars/bundle-2.24.6.jar",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    # ---- Raw → Silver ----
    raw_to_silver = SparkSubmitOperator(
        task_id="raw_to_silver",
        application="/opt/airflow/spark-jobs/raw_to_silver.py",
        conn_id="spark_default",
        jars="/opt/jars/hadoop-aws-3.4.1.jar,"
             "/opt/jars/bundle-2.24.6.jar",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    # ---- Silver → Gold ----
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/spark-jobs/silver_to_gold.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar,"
             "/opt/jars/hadoop-aws-3.4.1.jar,"
             "/opt/jars/bundle-2.24.6.jar",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    # ---- Gold → Postgres ----
    gold_to_postgres = SparkSubmitOperator(
        task_id="gold_to_postgres",
        application="/opt/airflow/spark-jobs/silver_gold_to_postgres.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar,"
             "/opt/jars/hadoop-aws-3.4.1.jar,"
             "/opt/jars/bundle-2.24.6.jar",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    # ---- Dependencies ----
    ingest_mysql_to_raw >> raw_to_silver >> silver_to_gold >> gold_to_postgres

