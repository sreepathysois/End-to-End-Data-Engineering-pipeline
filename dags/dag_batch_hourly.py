# dags/dag_batch_hourly.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    "batch_hourly_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    #schedule_interval="0 * * * *",  # every hour
    schedule_interval="*/10 * * * *",  # every hour
    catchup=False,
    max_active_runs=1,
) as dag:

    spark_hourly = SparkSubmitOperator(
        task_id="weather_hourly_batch",
        application="/opt/airflow/spark-jobs/weather_batch_hourly.py",
        conn_id="spark_default",
        jars="/opt/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,"
         "/opt/jars/bundle-sdk-2.24.6.jar,"
         "/opt/jars/kafka-clients-3.6.0.jar,"
         "/opt/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar,"
         "/opt/jars/commons-pool2-2.12.0.jar,"
         "/opt/jars/hadoop-aws-3.4.1.jar,"
         "/opt/jars/postgresql-42.6.0.jar",
        executor_memory="1g",
        total_executor_cores=1,
        name="weather_hourly_batch_submit",
        verbose=True,
    )

