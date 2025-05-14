
import clickhouse_connect
from flask import Flask, Response
from prometheus_client import Gauge, CollectorRegistry, generate_latest
import subprocess
import re

import os

# -----------------------------
# Регистр метрик
# -----------------------------
registry = CollectorRegistry()

# Базовые метрики
ch_active_queries = Gauge('clickhouse_active_queries', 'Number of active queries', registry=registry)
ch_uptime_seconds = Gauge('clickhouse_uptime_seconds', 'Server uptime in seconds', registry=registry)
ch_select_query_count = Gauge('clickhouse_select_queries_total', 'Total number of SELECT queries executed', registry=registry)
ch_memory_usage_bytes = Gauge('clickhouse_memory_usage_bytes', 'Memory usage in bytes', registry=registry)

# Новые метрики
ch_query_latency_seconds = Gauge('clickhouse_query_latency_seconds', 'Average query latency in seconds', registry=registry)
ch_error_count_total = Gauge('clickhouse_error_count_total', 'Total number of errors occurred', registry=registry)
ch_data_size_bytes = Gauge('clickhouse_data_size_bytes', 'Total size of all tables in bytes', registry=registry)

ch_disk_space_bytes_total = Gauge('clickhouse_disk_space_bytes_total', 'Total disk space available', registry=registry)
ch_disk_space_bytes_used = Gauge('clickhouse_disk_space_bytes_used', 'Disk space used', registry=registry)

test_metric = Gauge('test_metric', 'This is a test metric', registry=registry)
# Kafka метрики
kafka_consumers_active = Gauge('kafka_consumers_active', 'Number of active Kafka consumers', registry=registry)
kafka_consumers_assigned_partitions = Gauge('kafka_consumers_assigned_partitions', 'Total number of assigned Kafka partitions', registry=registry)
kafka_consumer_lag = Gauge('kafka_consumer_lag', 'Current lag in Kafka consumer', registry=registry)
kafka_messages_read_total = Gauge('kafka_messages_read_total', 'Total messages read from Kafka', registry=registry)
kafka_messages_failed_total = Gauge('kafka_messages_failed_total', 'Total failed Kafka message reads', registry=registry)
kafka_rows_inserted_total = Gauge('kafka_rows_inserted_total', 'Total rows inserted from Kafka', registry=registry)

registry = CollectorRegistry()

kafka_consumer_lag_total = Gauge(
    'kafka_consumer_group_total_lag',
    'Total lag across all topics and partitions in consumer group',
    ['group'],
    registry=registry
)

# -----------------------------
# Подключение к ClickHouse
# -----------------------------
def connect_ch():
    """Подключение к ClickHouse с использованием переменных окружения"""
    try:
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username="default",
            password="qwert1234",
            database='default'
        )
        print("✅ Connected to ClickHouse using monitoring user")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        return None


# -----------------------------
# Обновление метрик
# -----------------------------
def safe_get_result(result):
    """Безопасно извлекает первое значение из результата запроса"""
    if result.result_set and len(result.result_set) > 0:
        row = result.result_set[0]
        if len(row) > 0 and row[0] is not None:
            return float(row[0])
    return 0.0  # или float('nan'), зависит от ваших целей


def update_metrics():
    """Обновляем значения метрик из ClickHouse"""
    try:
        client = connect_ch()
        if not client:
            return

        # Активные запросы
        result = client.query("SELECT value FROM system.metrics WHERE metric = 'Query'")
        ch_active_queries.set(safe_get_result(result))

        # Время работы сервера
        result = client.query("SELECT uptime()")
        ch_uptime_seconds.set(safe_get_result(result))

        # Количество SELECT-запросов
        result = client.query("SELECT value FROM system.events WHERE event = 'SelectQuery'")
        ch_select_query_count.set(safe_get_result(result))

        # Использование памяти
        result = client.query("""SELECT value
FROM system.metrics
WHERE metric = 'MemoryTracking';""")
        ch_memory_usage_bytes.set(safe_get_result(result))

        # Латентность запросов
        result = client.query("SELECT value FROM system.events WHERE event = 'QueryTimeMicroseconds'")
        latency = safe_get_result(result)
        ch_query_latency_seconds.set(latency / 1_000_000 if latency > 0 else 0.0)

        # Ошибки
        result = client.query("SELECT value FROM system.events WHERE event = 'FailedQuery'")
        ch_error_count_total.set(safe_get_result(result))

        # Размер данных
        result = client.query("SELECT sum(data_compressed_bytes) FROM system.columns")
        ch_data_size_bytes.set(safe_get_result(result))

        # Дисковое пространство
        result = client.query("SELECT sum(total_space), sum(total_space - free_space) FROM system.disks")
        if result.result_set and len(result.result_set[0]) >= 2:
            total, used = result.result_set[0]
            ch_disk_space_bytes_total.set(float(total))
            ch_disk_space_bytes_used.set(float(used))
        else:
            ch_disk_space_bytes_total.set(0.0)
            ch_disk_space_bytes_used.set(0.0)

        # Kafka: количество активных потребителей
        result = client.query("SELECT count() FROM system.kafka_consumers WHERE table = 'posting_kafka'")
        kafka_consumers_active.set(safe_get_result(result))

        # Kafka: назначенные партиции
        result = client.query("SELECT sum(partitions) FROM (SELECT count() AS partitions FROM system.kafka_consumers WHERE table = 'posting_kafka')")
        kafka_consumers_assigned_partitions.set(safe_get_result(result))

        # Kafka: общий лаг
        result = client.query("SELECT sum(lag) FROM system.kafka_consumers WHERE table = 'posting_kafka'")
        kafka_consumer_lag.set(safe_get_result(result))

        # Kafka: прочитано сообщений
        result = client.query("SELECT value FROM system.events WHERE event = 'KafkaMessagesRead'")
        kafka_messages_read_total.set(safe_get_result(result))

        # Kafka: ошибки при чтении
        result = client.query("SELECT value FROM system.events WHERE event = 'KafkaMessagesFailed'")
        kafka_messages_failed_total.set(safe_get_result(result))

        # Kafka: строки вставлены
        result = client.query("SELECT value FROM system.events WHERE event = 'KafkaRowsRead'")
        kafka_rows_inserted_total.set(safe_get_result(result))

    except Exception as e:
        print(f"❌ Error fetching metrics: {e}")


def collect_kafka_lag(consumer_group, bootstrap_server='localhost:9092'):
    try:
        # Выполняем CLI-запрос
        cmd = [
            "kafka-consumer-groups.sh",
            "--bootstrap-server", bootstrap_server,
            "--describe",
            "--group", consumer_group
        ]

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        output = result.stdout

        total_lag = 0
        lines = output.strip().split('\n')

        for line in lines[1:]:  # Пропускаем заголовок
            if not line.strip():
                continue
            parts = re.split(r'\s+', line)
            if len(parts) >= 6 and parts[5].isdigit():
                lag = int(parts[5])
                total_lag += lag

        kafka_consumer_lag_total.labels(group=consumer_group).set(total_lag)
        print(f"📊 Collected Kafka lag for group '{consumer_group}': {total_lag}")

    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to fetch Kafka lag info: {e.stderr}")
# -----------------------------
# Flask сервер
# -----------------------------
app = Flask(__name__)

@app.route('/metrics')
def metrics():
    collect_kafka_lag(consumer_group='clickhouse_group', bootstrap_server='kafka:9092')
    return Response(generate_latest(registry), content_type='text/plain; version=0.0.4')

@app.route('/ping')
def ping():
    return "OK", 200

# -----------------------------
# Запуск
# -----------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9363)