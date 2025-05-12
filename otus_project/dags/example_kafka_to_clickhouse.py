from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import clickhouse_connect
from kafka import KafkaConsumer

def read_kafka_and_write_to_clickhouse():
    consumer = KafkaConsumer(
        'topic-otus',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        group_id='airflow-clickhouse-consumer',
        enable_auto_commit=False,
        value_deserializer=lambda m: m.decode('utf-8')
    )

    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password='qwert1234'
    )

    messages = []
    for message in consumer:
        try:
            msg = eval(message.value)  # только для теста, лучше использовать json.loads()
            messages.append((msg['id'], msg['name']))
            if len(messages) >= 100:
                break
        except Exception as e:
            print("Ошибка парсинга:", e)
            continue

    if messages:
        client.insert('otus.kafka_messages', messages, column_names=['id', 'name'])
        print(f"Записано {len(messages)} записей")

with DAG(
    dag_id='kafka_to_clickhouse_dag',
    schedule='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    run_task = PythonOperator(
        task_id='read_kafka_write_clickhouse',
        python_callable=read_kafka_and_write_to_clickhouse
    )