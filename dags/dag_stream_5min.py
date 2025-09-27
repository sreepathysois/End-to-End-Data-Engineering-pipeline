# dags/dag_stream_5min.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    "stream_5min_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    spark_agg = SparkSubmitOperator(
        task_id="weather_5min_agg",
        application="/opt/airflow/spark-jobs/weather_stream_agg_5min.py",
        conn_id="spark_default",
        jars="/opt/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,"
         "/opt/jars/bundle-sdk-2.24.6.jar,"
         "/opt/jars/kafka-clients-3.6.0.jar,"
         "/opt/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar,"
         "/opt/jars/commons-pool2-2.12.0.jar,"
         "/opt/jars/hadoop-aws-3.4.1.jar,"
         "/opt/jars/postgresql-42.6.0.jar",
        #packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
        name="weather_5min_agg_submit",
        verbose=True,
    )

