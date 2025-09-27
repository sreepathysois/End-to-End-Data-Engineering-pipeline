from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json, requests, os

# Put your API key in Airflow Variables or env
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_KEY", "c919564eebb80d99021f84b2bac1559f")  # set in docker-compose env for airflow containers or use Airflow Variables
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "weather"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def fetch_and_produce(**context):
    # Example: OpenWeatherMap (replace with your API of choice)
    # pick a city or list of cities
    cities = ["London", "New Delhi", "New York"]
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100
    )

    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            payload = resp.json()
            # add ingest metadata
            payload["_ingest_ts"] = datetime.utcnow().isoformat()
            payload["_city"] = city
            producer.send(TOPIC, payload)
        else:
            # send an error record (optional)
            producer.send(TOPIC, {"_error": True, "status_code": resp.status_code, "city": city})
    producer.flush()
    producer.close()

with DAG(
    dag_id="producer_weather_kafka",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),  # polling frequency
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stream","kafka","producer"]
) as dag:
    produce = PythonOperator(
        task_id="fetch_and_produce",
        python_callable=fetch_and_produce,
    )

