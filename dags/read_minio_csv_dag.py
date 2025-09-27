from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="read_minio_csv",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["minio", "spark", "test"],
) as dag:

    read_csv = SparkSubmitOperator(
        task_id='read_csv_from_minio',
        application='/opt/airflow/spark-jobs/read_minio_csv.py',
        conn_id='spark_default',
        jars="/opt/jars/hadoop-aws-3.4.1.jar,/opt/jars/bundle-2.24.6.jar",
        deploy_mode='client',
        executor_memory='1g',
        total_executor_cores=1,
        name='minio-spark-read',
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.extraJavaOptions": "-Duser.home=/home/airflow"
        }
    )

