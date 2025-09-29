#  Data Engineering Project – Batch & Streaming Pipelines

This project demonstrates end-to-end Data Engineering pipelines using both Batch and Streaming approaches. It covers:

* Batch processing with RetailDB (using Pandas and Apache Spark)

* Real-time streaming processing of Weather data (using Kafka, Spark Structured Streaming, MinIO, and Postgres)

## Tech Stack 
| Component           | Purpose                                                                 |
| ------------------- | ----------------------------------------------------------------------- |
| **Apache Airflow**  | Orchestrates batch & streaming pipelines (DAG scheduling, dependencies) |
| **Apache Spark**    | Distributed data processing for batch and streaming pipelines           |
| **Pandas**          | Lightweight batch ETL on RetailDB for prototyping                       |
| **Kafka**           | Ingests weather data as a real-time event stream                        |
| **MinIO (S3a)**     | Acts as a Data Lake for raw and processed data (Bronze → Silver)        |
| **Postgres**        | Stores batch and real-time views (for analytics & dashboards)           |
| **RetailDB**        | Benchmark dataset for batch pipelines (orders, customers, products)     |
| **OpenWeather API** | Source for real-time weather data                                       |
| **Docker Compose**  | Containerized setup for Spark, Kafka, MinIO, Airflow, Postgres          |
| **Metabase**        | Business Intelligence Tool                                              |
|**Jupyter Notebook**  | Environment for EDA and Machine learning workloads                      |

## Stack Set-up installation

<pre>git clone https://github.com/sreepathysois/End-to-End-Data-Engineering-pipeline.git
cd End-to-End-Data-Engineering-pipeline
sudo docker-compose up --build -d </pre>

This setup all services: 

# Services Setup

| Service        | URL / Port              | Notes                           |
|----------------|-------------------------|---------------------------------|
| **Spark Master** | http://localhost:8085   | Accessible Spark Web UI          |
| **Spark Workers** | —                     | Connect to Spark Master          |
| **Kafka + Zookeeper** | —                 | Core messaging services           |
| **Kafka UI**   | http://localhost:8083   | Web UI for Kafka (if enabled)    |
| **MinIO**      | http://localhost:9090   | S3-compatible storage            |
| **Airflow**    | http://localhost:8082   | Workflow orchestration           |
| **Postgres**   | `localhost:5432`        | Default DB port                  |
| **Metabase**   | http://localhost:3000   | Data Visualization Tool( BI)     |
| **Jupyter Notebook**| http://localhost:8888| Environment for EDA            |  


## Post installation steps   

#### Verify Containers

Check that all containers are running (Airflow webserver, scheduler, workers, Spark master, Spark workers, Kafka, Zookeeper, MinIO, Postgres):

<pre>docker ps </pre>  

|CONTAINER ID | IMAGE | COMMAND | CREATED | STATUS | PORTS | NAMES|
|-------------|-------|---------|---------|--------|--------|-----|
|4dfe3f8cf88c | provectuslabs/kafka-ui:latest | "/bin/sh -c 'java --…" | 2 days ago | Up 2 days | 0.0.0.0:8083->8080/tcp, [::]:8083->8080/tcp | kafka-ui
2d3e57dfd1e8 | bitnami/spark:latest | "/opt/bitnami/script…" | 2 days ago | Up 2 days |  | spark-worker
970277d94f45 | data-pipeline_airflow-worker | "/usr/bin/dumb-init …" | 2 days ago | Up 2 days | 8080/tcp | data-pipeline_airflow-worker_1
928d364a5a74 | data-pipeline_airflow-webserver | "/usr/bin/dumb-init …" | 2 days ago | Up 2 days | 0.0.0.0:8082->8080/tcp, [::]:8082->8080/tcp | data-pipeline_airflow-webserver_1
4805822c5e07 | data-pipeline_airflow-scheduler | "/usr/bin/dumb-init …" | 2 days ago | Up 2 days | 8080/tcp | data-pipeline_airflow-scheduler_1
fd968390225e | metabase/metabase:latest | "/app/run_metabase.sh" | 2 days ago | Up 2 days | 0.0.0.0:3000->3000/tcp, :::3000->3000/tcp | metabase
7f078e29c933 | bitnami/kafka:3.6.1 | "/opt/bitnami/script…" | 2 days ago | Up 2 days | 0.0.0.0:9092->9092/tcp, :::9092->9092/tcp | kafka
711469ce8b84 | redis:latest | "docker-entrypoint.s…" | 2 days ago | Up 2 days | 0.0.0.0:6379->6379/tcp, :::6379->6379/tcp | data-pipeline_redis_1
b7bdcb119a78 | jupyter/base-notebook:latest | "tini -g -- start-no…" | 2 days ago | Up 2 days (healthy) | 0.0.0.0:8888->8888/tcp, :::8888->8888/tcp | jupyter
1e5128e93893 | bitnami/zookeeper:3.8.4 | "/opt/bitnami/script…" | 2 days ago | Up 2 days | 2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp, :::2181->2181/tcp, 8080/tcp | zookeeper
4194a1866b54 | minio/minio | "/usr/bin/docker-ent…" | 2 days ago | Up 2 days | 0.0.0.0:9000-9001->9000-9001/tcp, :::9000-9001->9000-9001/tcp | data-pipeline_minio_1
cad8003bf693 | postgres:14 | "docker-entrypoint.s…" | 2 days ago | Up 2 days | 0.0.0.0:5432->5432/tcp, :::5432->5432/tcp | data-pipeline_postgres_1
001a81c28501 | bitnami/spark:latest | "/opt/bitnami/script…" | 2 days ago | Up 2 days | 0.0.0.0:7077->7077/tcp, :::7077->7077/tcp, 0.0.0.0:8085->8080/tcp, [::]:8085->8080/tcp, 0.0.0.0:8084->8081/tcp, [::]:8084->8081/tcp | spark-master


#### Install PySpark 4.0.0 in Airflow Worker

Make the Airflow worker compatible with Spark 4.0.0:

<pre>docker exec -it airflow-worker-container bash
pip install pyspark==4.0.0  </pre>

#### Download Required JARs

Download the following JARs and keep them in a local folder named jars/:

* spark-sql-kafka-0-10_2.13-4.0.0.jar

* spark-token-provider-kafka-0-10_2.13-4.0.0.jar

* kafka-clients-3.6.0.jar

* bundle-sdk-2.24.6.jar

* commons-pool2-2.12.0.jar

* hadoop-aws-3.4.1.jar

* postgresql-42.6.0.jar

Your folder structure should look like:

<pre>project-root/
│── dags/
│── spark-jobs/
│── jars/
    ├── spark-sql-kafka-0-10_2.13-4.0.0.jar
    ├── spark-token-provider-kafka-0-10_2.13-4.0.0.jar
    ├── kafka-clients-3.6.0.jar
    ├── bundle-sdk-2.24.6.jar
    ├── commons-pool2-2.12.0.jar
    ├── hadoop-aws-3.4.1.jar
    └── postgresql-42.6.0.jar</pre>



#### Copy JARs into Spark Master and Workers

Use docker cp to copy the JARs into /opt/bitnami/spark/jars/ inside the Spark containers:

##### Copy jars into Spark Master
<pre> for jar in jars/*.jar; do
  docker cp $jar spark-master:/opt/bitnami/spark/jars/
done </pre>

##### Copy jars into Spark Worker(s)
<pre>for jar in jars/*.jar; do
  docker cp $jar spark-worker-1:/opt/bitnami/spark/jars/
  docker cp $jar spark-worker-2:/opt/bitnami/spark/jars/
done </pre> 


## Data preparation 

### Batch data engineering data preparation: 

To prepare the Retail Database (RetailDB) for batch data engineering, follow these steps:  

#### Create MySQL Server on Remote Machine

Install MySQL server on your remote machine (Linux example): 

<pre>sudo apt update
sudo apt install mysql-server -y
sudo systemctl enable mysql
sudo systemctl start mysql </pre>

#### Login to MySQL

Connect to the MySQL server: 

<pre>mysql -u root -p </pre>

#### Create Database

Inside the MySQL shell:

<pre>CREATE DATABASE retaildb;</pre>

#### Create User with Privileges

Create a new user cdc with password msis@123 and grant full access to the retaildb database:

<pre>CREATE USER 'cdc'@'%' IDENTIFIED BY 'msis@123';
GRANT ALL PRIVILEGES ON retaildb.* TO 'cdc'@'%';
FLUSH PRIVILEGES;</pre>

#### Import RetailDB Schema and Data

Exit MySQL and load the provided retaildb.sql file into the database:

<pre>mysql -u cdc -p retaildb < /path/to/retaildb.sql </pre>

#### Verify Data

Check if tables are loaded correctly:

<pre>USE retaildb;
SHOW TABLES;
SELECT COUNT(*) FROM orders;</pre>  


✅ Now the RetailDB is ready for use in batch data engineering pipelines.


## Project Folders objective and Overview  

<pre>.
├── dags/                     # Airflow DAGs (workflow definitions)
├── ivy2/                     # Local Ivy cache for Spark/Scala dependencies
├── jars/                     # Extra JARs required for Spark jobs (Kafka, JDBC, AWS, etc.)
├── logs/                     # Airflow logs (scheduler, workers, webserver)
├── metabase-data/            # Metabase metadata storage
│   └── metabase.db           # SQLite database used by Metabase
├── minio-data/               # MinIO object storage data
│   └── raw-data/sample_data.csv  # Sample dataset stored in MinIO
├── notebooks/                # Jupyter/Databricks notebooks for experimentation
├── plugins/                  # Custom Airflow plugins/operators/hooks
├── spark-jars/               # Spark runtime JARs
├── spark-jobs/               # PySpark job scripts (batch & streaming pipelines)
├── Dockerfile.airflow        # Custom Dockerfile for Airflow with extra dependencies
├── docker-compose.yml        # Compose file to spin up all services
└── README.md                 # Project documentation
</pre>

### Objectives and Overview of Key Folders


| Folder/File          | Purpose                                                    |
| -------------------- | ---------------------------------------------------------- |
| `dags/`              | Airflow DAGs to orchestrate Spark jobs (batch & streaming) |
| `ivy2/`              | Local Ivy cache for Spark/Scala dependencies               |
| `jars/`              | External JARs: Kafka, JDBC drivers, Hadoop-AWS, etc.       |
| `logs/`              | Airflow logs for scheduler, workers, and webserver         |
| `metabase-data/`     | Metabase metadata and dashboards                           |
| `minio-data/`        | Persistent storage for MinIO (raw datasets)                |
| `notebooks/`         | Jupyter/Databricks notebooks for exploration               |
| `plugins/`           | Custom Airflow plugins/operators/hooks                     |
| `spark-jars/`        | Spark runtime JARs                                         |
| `spark-jobs/`        | PySpark jobs (Kafka ingestion, batch/stream processing)    |
| `Dockerfile.airflow` | Custom Airflow image with Spark & dependencies             |
| `docker-compose.yml` | Orchestrates the complete environment                      |
| `README.md`          | Project documentation                                      |


#### System Architecture  

<img width="1400" height="823" alt="image" src="https://github.com/user-attachments/assets/099a7985-66a2-4a7d-8e5a-eb30a7906f0c" />

 
## Batch Data Engineering (RetailDB) 

### Using Pandas 

#### Scenario 1: Ingest sample data from local files system into MinIo storage and PostgresSQL

**Running ETL DAG: MinIO → Postgres**

We will create a new Airflow DAG (etl_minio_postgres.py) inside the dags/ folder to perform an ETL pipeline:

- Extract: Load data from MinIO (raw-data bucket, CSV file)

- Transform: Parse and clean data with Spark

- Load: Insert processed data into Postgres

1️⃣ Create Sample Data

You can use either a small dataset in CSV format or generate it inside your DAG.
Example dataset:


| id |   name  | amount|
|-----|---------|------|
|  1|  Alice  |   100 |
|  2|  Bob    |   200 |
|  3| Charlie |   300 |


Save this as sample_data.csv and upload it into the MinIO bucket (raw-data).

2️⃣ MinIO Setup

Access MinIO Console: [http://localhost:9000] (http://localhost:9000)

Credentials:

**Username: minio**

**Password: minio123**

Create a new bucket named: raw-data

Upload sample_data.csv into raw-data/

3️⃣ Create Airflow DAG

Inside the dags/ folder, create a file named etl_minio_postgres.py.
This DAG should:

Read the CSV from MinIO

Transform using PySpark (or Pandas if lightweight)

Write results into Postgres

Example skeleton:

<pre>from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="etl_minio_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # run on demand
    catchup=False,
    default_args=default_args,
) as dag:

    etl_task = SparkSubmitOperator(
        task_id="minio_to_postgres",
        application="/opt/airflow/spark-jobs/etl_minio_postgres.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        executor_memory="1g",
        total_executor_cores=1,
        name="etl_minio_postgres",
        verbose=True,
    )</pre>

4️⃣ Run the DAG

Access Airflow Webserver: [http://localhost:8082] (http://localhost:8082)

Login with:

**Username: admin**

**Password: admin**

Enable and trigger the DAG: etl_minio_postgres

Monitor the task logs to verify data is loaded into Postgres

✅ At the end, your ETL pipeline will move data from MinIO → into Postgres with Airflow + Spark.
  

##### Validate DAG run succesfully 

* Check minio bucker raw-data for data ingestion.
* Also check Postgresql for table  transactions using psql

