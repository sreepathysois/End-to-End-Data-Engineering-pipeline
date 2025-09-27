from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    "ingest_mysql_to_raw",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "raw"],
) as dag:

    ingest_mysql = SparkSubmitOperator(
        task_id="ingest_mysql_raw",
        application="/opt/airflow/spark-jobs/mysql_to_raw_all.py",
        conn_id="spark_default",
        jars="/opt/jars/mysql-connector-java-8.0.29.jar,"
             "/opt/jars/hadoop-aws-3.4.1.jar,"
             "/opt/jars/bundle-2.24.6.jar",
        conf={"spark.master": "spark://spark-master:7077"},
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_raw_to_silver",
        trigger_dag_id="process_raw_to_silver",  # downstream DAG
        wait_for_completion=False,
    )

    ingest_mysql >> trigger_next

