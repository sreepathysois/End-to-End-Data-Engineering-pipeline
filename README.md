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
|**Jupyter Notebook   | Environment for EDA and Machine learning workloads                      |

## Stack Set-up installation

* git clone https://github.com/sreepathysois/End-to-End-Data-Engineering-pipeline.git
* cd End-to-End-Data-Engineering-pipeline
* sudo docker-compose up --build -d

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

1. Verify Containers

Check that all containers are running (Airflow webserver, scheduler, workers, Spark master, Spark workers, Kafka, Zookeeper, MinIO, Postgres):

* docker ps  

2. Install PySpark 4.0.0 in Airflow Worker

Make the Airflow worker compatible with Spark 4.0.0:

* docker exec -it <airflow-worker-container> bash
* pip install pyspark==4.0.0  

3. Download Required JARs

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



4. Copy JARs into Spark Master and Workers

Use docker cp to copy the JARs into /opt/bitnami/spark/jars/ inside the Spark containers:

### Copy jars into Spark Master
<pre> for jar in jars/*.jar; do
  docker cp $jar spark-master:/opt/bitnami/spark/jars/
done </pre>

### Copy jars into Spark Worker(s)
<pre>for jar in jars/*.jar; do
  docker cp $jar spark-worker-1:/opt/bitnami/spark/jars/
  docker cp $jar spark-worker-2:/opt/bitnami/spark/jars/
done </pre> 


## Data preparation 

### Batch data engineering data preparation: 

To prepare the Retail Database (RetailDB) for batch data engineering, follow these steps:  

#### Create MySQL Server on Remote Machine

Install MySQL server on your remote machine (Linux example): 

* sudo apt update
* sudo apt install mysql-server -y
* sudo systemctl enable mysql
* sudo systemctl start mysql

#### Login to MySQL

Connect to the MySQL server: 

* mysql -u root -p

#### Create Database

Inside the MySQL shell:

* CREATE DATABASE retaildb;

#### Create User with Privileges

Create a new user cdc with password msis@123 and grant full access to the retaildb database:

* CREATE USER 'cdc'@'%' IDENTIFIED BY 'msis@123';
* GRANT ALL PRIVILEGES ON retaildb.* TO 'cdc'@'%';
* FLUSH PRIVILEGES;

#### Import RetailDB Schema and Data

Exit MySQL and load the provided retaildb.sql file into the database:

* mysql -u cdc -p retaildb < /path/to/retaildb.sql

#### Verify Data

Check if tables are loaded correctly:

* USE retaildb;
* SHOW TABLES;
* SELECT COUNT(*) FROM orders;


✅ Now the RetailDB is ready for use in batch data engineering pipelines.

## Batch Data Engineering (RetailDB) 

### Using Pandas

  



