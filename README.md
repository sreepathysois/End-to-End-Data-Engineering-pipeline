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

