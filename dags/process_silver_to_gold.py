from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    "process_silver_to_gold",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "gold"],
) as dag:

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

    trigger_postgres = TriggerDagRunOperator(
        task_id="trigger_silver_gold_to_postgres",
        trigger_dag_id="silver_gold_to_postgres",  # must match target DAG id
        reset_dag_run=True,
        wait_for_completion=False,
    )

    silver_to_gold >> trigger_postgres

