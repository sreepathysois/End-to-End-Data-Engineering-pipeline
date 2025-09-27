from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="weather_master_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["weather", "streaming", "spark"],
) as dag:

    ingest_kafka = SparkSubmitOperator(
        task_id="weather_kafka_to_raw",
        application="/opt/airflow/spark-jobs/weather_kafka_to_raw.py",
        conn_id="spark_default",
        jars="/opt/jars/hadoop-aws-3.4.1.jar,/opt/jars/bundle-2.24.6.jar",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    weather_raw_to_silver = SparkSubmitOperator(
        task_id="weather_raw_to_silver",
        application="/opt/airflow/spark-jobs/weather_raw_to_silver.py",
        conn_id="spark_default",
        jars="/opt/jars/hadoop-aws-3.4.1.jar,/opt/jars/bundle-2.24.6.jar",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    weather_silver_to_gold = SparkSubmitOperator(
        task_id="weather_silver_to_gold",
        application="/opt/airflow/spark-jobs/weather_silver_to_gold.py",
        conn_id="spark_default",
        jars="/opt/jars/hadoop-aws-3.4.1.jar,/opt/jars/bundle-2.24.6.jar,/opt/jars/postgresql-42.6.0.jar",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    weather_load_postgres = SparkSubmitOperator(
        task_id="weather_silver_gold_to_postgres",
        application="/opt/airflow/spark-jobs/weather_silver_gold_to_postgres.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar,/opt/jars/hadoop-aws-3.4.1.jar,/opt/jars/bundle-2.24.6.jar",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
    )

    ingest_kafka >> weather_raw_to_silver >> weather_silver_to_gold >> weather_load_postgres

