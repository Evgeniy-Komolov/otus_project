#!/bin/sh

./bin/kafka-topics.sh --create --topic test --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:9092

while true; do
    LAG=$(./bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group clickhouse_group | awk '$6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}')

    echo "# HELP kafka_consumer_group_total_lag Total lag across all topics and partitions in consumer group" > /tmp/metrics
    echo "# TYPE kafka_consumer_group_total_lag gauge" >> /tmp/metrics
    echo "kafka_consumer_group_total_lag{group=\"clickhouse_group\"} $LAG" >> /tmp/metrics

    python3 -m http.server 9364 --directory /tmp
done