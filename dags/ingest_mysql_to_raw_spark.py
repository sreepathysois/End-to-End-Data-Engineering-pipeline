from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="ingest_mysql_to_raw_spark",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "raw", "spark", "test"],
) as dag:

    ingest_mysql = SparkSubmitOperator(
        task_id='ingest_mysql_raw_spark',
        application='/opt/airflow/spark-jobs/mysql_to_raw_all_backup.py',
        conn_id='spark_default',  # Airflow Spark connection
        jars="/opt/jars/mysql-connector-java-8.0.29.jar,"
         "/opt/jars/hadoop-aws-3.4.1.jar,"
         "/opt/jars/bundle-2.24.6.jar",
        deploy_mode='client',
        executor_memory='1g',
        total_executor_cores=1,
        name='arrow-spark',
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.extraJavaOptions": "-Duser.home=/home/airflow",
            "spark.network.timeout": "60",
            "spark.executor.heartbeatInterval": "60",
            "spark.sql.broadcastTimeout": "60"
             
        }
       
    )

