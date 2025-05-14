FROM alpine:latest

# Установка зависимостей
RUN apk add --no-cache openjdk8-jre bash curl wget

# Переменные окружения
ENV KAFKA_VERSION=4.0.0 \
    SCALA_VERSION=2.13

# Скачиваем и распаковываем Kafka CLI
WORKDIR /opt

RUN wget -q https://downloads.apache.org/kafka/ ${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} kafka && \
    rm -f kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

WORKDIR /opt/kafka

# Копируем скрипт запуска
COPY run.sh .
RUN chmod +x run.sh

EXPOSE 9364

CMD ["sh", "-c", "./run.sh"]