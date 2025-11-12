# Проект добавления sql-представлений в kafka для clickhouse в продюсере и считывания их в консумере с помощью sql (пока делается)

## Общая схема работы:
Producer  → Consumer 1 → Kafka (topic=events) → ClickHouse Kafka Engine (kafka_input)
→ Materialized View → MergeTree (events) → Consumer 2 (поймаем из событий ClickHouse)


## Сборка и запуск контейнеров
Сборка
```bash
docker compose up --build
```
Запуск
```bash
docker compose up -d
```


## Запуск консумера
```bash
docker exec -it clickhouse-kafka-consumer1-1 sh
```
В контейнере
```bash
./consumer
```

## Запуск продюсера
```bash
docker exec -it clickhouse-kafka-producer-1 sh
```
В контейнере
```bash
./producer
```