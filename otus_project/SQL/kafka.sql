DROP TABLE otus.kafka_test;

set stream_like_engine_allow_direct_select = 1;
SELECT * FROM otus.posting_kafka LIMIT 5;


CREATE MATERIALIZED VIEW otus.kafka_consumer TO otus.kafka_messages AS
SELECT * FROM otus.kafka_test;

SELECT * FROM otus.kafka_messages;

{"posting_id": 90101, "order_time": "2025-05-09T16:59:01.363601", "adress": "\u0443\u043b. \u041f\u0443\u0448\u043a\u0438\u043d\u0430, \u0434. 13", "status": "cancelled"}





DROP TABLE otus.posting_kafka;


set stream_like_engine_allow_direct_select = 1;
SELECT * FROM otus.posting_kafka;


CREATE TABLE otus.posting_kafka (
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'posting_data',
    kafka_group_name = 'otus-group',
    kafka_format = 'JSONAsString';


CREATE TABLE otus.posting_data (
    posting_id UInt32,
    order_time DateTime,
    adress String,
    status String,
    clickhoue_timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (posting_id, clickhoue_timestamp);

CREATE MATERIALIZED VIEW otus.posting_mv TO otus.posting_data AS
SELECT
    JSONExtractUInt(message, 'posting_id') AS posting_id,
    parseDateTimeBestEffort(JSONExtractString(message, 'order_time')) AS order_time,
    JSONExtractString(message, 'adress') AS adress,
    JSONExtractString(message, 'status') AS status,
    now() AS clickhoue_timestamp
FROM otus.posting_kafka;



DROP TABLE otus.posting_mv_stop;

CREATE MATERIALIZED VIEW otus.posting_mv_stop TO otus.posting_data_stop AS
SELECT
    JSONExtractUInt(message, 'posting_id') AS posting_id,
    parseDateTimeBestEffort(JSONExtractString(message, 'order_time')) AS order_time,
    JSONExtractString(message, 'adress') AS adress,
    JSONExtractString(message, 'status') AS status,
    now() AS clickhoue_timestamp
FROM otus.posting_kafka;


SELECT status FROM system. WHERE table = 'posting_mv';

SELECT * FROM otus.posting_data;


kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group clickhouse-consumer-groupkafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group clickhouse-consumer-group