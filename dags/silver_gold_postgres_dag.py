from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_gold_to_postgres",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "silver_gold", "postgres"],
) as dag:

    spark_postgres = SparkSubmitOperator(
        task_id="write_silver_gold_to_postgres",
        application="/opt/airflow/spark-jobs/silver_gold_to_postgres.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar,"
             "/opt/jars/hadoop-aws-3.4.1.jar,"
             "/opt/jars/bundle-2.24.6.jar",
        deploy_mode="client",
        executor_memory="1g",
        total_executor_cores=1,
        name="silver_gold_postgres",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.extraJavaOptions": "-Duser.home=/home/airflow",
        },
    )

    spark_postgres

