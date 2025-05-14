import subprocess
import re

from flask import Flask, Response
from prometheus_client import Gauge, CollectorRegistry, generate_latest, start_http_server

# -----------------------------
# Метрики Prometheus
# -----------------------------
registry = CollectorRegistry()

# Лаг по конкретным топикам и партициям
kafka_partition_lag = Gauge(
    'kafka_topic_partition_lag',
    'Current lag for a Kafka topic partition',
    ['topic', 'partition'],
    registry=registry
)

# Общий лаг по группе
kafka_total_lag = Gauge(
    'kafka_consumer_group_total_lag',
    'Total lag across all topics and partitions in consumer group',
    registry=registry
)

# Количество активных партиций
kafka_active_partitions = Gauge(
    'kafka_consumer_group_active_partitions',
    'Number of active partitions assigned to consumers in the group',
    registry=registry
)


def get_kafka_lag(consumer_group, bootstrap_server=None, zookeeper=None):
    """
    Выполняет команду kafka-consumer-groups.sh и собирает информацию о лаге.
    Можно указать либо bootstrap_server, либо zookeeper
    """

    if bootstrap_server:
        cmd = [
            "kafka-consumer-groups.sh",
            "--bootstrap-server", bootstrap_server,
            "--describe",
            "--group", consumer_group
        ]
    elif zookeeper:
        cmd = [
            "kafka-consumer-groups.sh",
            "--zookeeper", zookeeper,
            "--describe",
            "--group", consumer_group
        ]
    else:
        raise ValueError("You must provide either bootstrap_server or zookeeper")

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        output = result.stdout
        return parse_kafka_output(output)
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to fetch Kafka lag info: {e.stderr}")
        return []


def parse_kafka_output(output):
    """
    Парсит вывод kafka-consumer-groups.sh
    """
    lines = output.strip().split('\n')
    headers = lines[0].split()
    data = []

    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split()
        row = dict(zip(headers, parts))
        data.append(row)

    return data


def collect_and_export(consumer_group, bootstrap_server=None, zookeeper=None):
    """
    Собирает данные и обновляет метрики Prometheus
    """
    lag_data = get_kafka_lag(consumer_group, bootstrap_server, zookeeper)

    total_lag = 0
    active_partitions = 0

    # Очищаем старые метрики
    kafka_partition_lag._metrics.clear()

    for entry in lag_data:
        if 'LAG' in entry and 'TOPIC' in entry and 'PARTITION' in entry:
            lag = int(entry['LAG'])
            topic = entry['TOPIC']
            partition = entry['PARTITION']

            kafka_partition_lag.labels(topic=topic, partition=partition).set(lag)
            total_lag += lag
            active_partitions += 1

    kafka_total_lag.set(total_lag)
    kafka_active_partitions.set(active_partitions)

    print(f"📊 Total Lag: {total_lag}, Active Partitions: {active_partitions}")


# -----------------------------
# HTTP сервер для /metrics
# -----------------------------
app = Flask(__name__)


@app.route('/metrics')
def metrics_route():
    try:
        collect_and_export(
            consumer_group='your_consumer_group',
            bootstrap_server='localhost:9092'
        )
        return Response(generate_latest(registry), content_type='text/plain; version=0.0.4')
    except Exception as e:
        return Response(f"Internal Server Error: {str(e)}", status=500)


@app.route('/ping')
def ping():
    return "OK", 200


# -----------------------------
# Запуск приложения
# -----------------------------
if __name__ == '__main__':
    # Для тестирования запустим Prometheus HTTP сервер на порту 9364
    start_http_server(9364, registry=registry)

    # Или можно запускать как Flask-сервер (для дебага)
    app.run(host='0.0.0.0', port=9363)