# dags/dag_kafka_to_raw.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    "kafka_to_raw_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="kafka_to_raw_once",
        application="/opt/airflow/spark-jobs/kafka_to_raw_once.py",
        conn_id="spark_default",
        jars="/opt/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,"
         "/opt/jars/bundle-sdk-2.24.6.jar,"
         "/opt/jars/kafka-clients-3.6.0.jar,"
         "/opt/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar,"
         "/opt/jars/commons-pool2-2.12.0.jar,"
         "/opt/jars/hadoop-aws-3.4.1.jar,"
         "/opt/jars/postgresql-42.6.0.jar",

        #packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.kafka:kafka-clients:2.8.1",
        #packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.kafka:kafka-clients:3.5.1",

        # jars or packages for kafka/s3a if needed (must be available)
        #packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
        name="kafka_to_raw_submit",
        verbose=True,
    )

    # optional: trigger 5min aggregation DAG immediately after raw ingest finishes
    trigger_5min = TriggerDagRunOperator(
        task_id="trigger_stream_5min",
        trigger_dag_id="stream_5min_dag",
        wait_for_completion=False,
    )

    spark_job >> trigger_5min

