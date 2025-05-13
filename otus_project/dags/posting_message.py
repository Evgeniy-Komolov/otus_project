from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import string
import json
from kafka import KafkaProducer
from datetime import datetime, timedelta


# Функция для генерации случайного адреса
def generate_address():
    streets = ["Ленина", "Пушкина", "Гагарина", "Советская", "Первомайская"]
    return f"ул. {random.choice(streets)}, д. {random.randint(1, 100)}"


# Функция для генерации случайного статуса
def generate_status():
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    return random.choice(statuses)


# Функция для генерации случайных данных
def generate_message():
    posting_id = random.randint(10000, 99999)
    days = random.randint(1, 30)
    order_time = (datetime.now() - timedelta(days=days)).isoformat()
    address = generate_address()
    status = generate_status()

    return {
        "posting_id": posting_id,
        "order_time": order_time,
        "adress": address,
        "status": status
    }


# Функция для отправки в Kafka
def send_to_kafka(topic='posting_data', bootstrap_servers='kafka:9092'):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    message = generate_message()
    print(f"Отправляем сообщение в Kafka: {message}")
    producer.send(topic, value=message)
    producer.flush()


# Определяем DAG
with DAG(
    dag_id='send_random_kafka_messages',
    schedule_interval='*/1 * * * *',  # каждые 1 минут
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example', 'kafka', 'otus_project']
) as dag:

    send_task = PythonOperator(
        task_id='send_kafka_message',
        python_callable=send_to_kafka,
        op_kwargs={
            'topic': 'posting_data',
            'bootstrap_servers': 'kafka:9092'
        }
    )

    send_task