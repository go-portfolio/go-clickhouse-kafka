# ClickHouse + Kafka

Этот проект демонстрирует интеграцию Kafka с ClickHouse, включая автоматическую генерацию событий и хранение данных.

Сообщения приходят в Kafka, затем потребляются и сохраняются в основной таблице my_table, одновременно создаются события в таблице events.

## Структура проекта
Producer  → Kafka (topic=events) → Consumer → Kafka (topic=events) → ClickHouse Kafka Engine (kafka_input)
→ Materialized View → MergeTree (events) → Events Watcher (события ClickHouse)


- **producer** – Go-продюсер, который отправляет JSON-сообщения в Kafka-топик `data_topic`.
- **consumer** – Go-приложение, которое:
  1. Создаёт необходимые таблицы ClickHouse.
  2. Подключается к Kafka-топику и читает сообщения.
  3. Вставляет данные в `my_table`.
  4. Создаёт запись в `events` для каждого вставленного объекта.
  5. Materialized View `kafka_to_events` автоматически переносит данные из таблицы Kafka Engine `kafka_input` в `events`.

- **events_watcher** – Go-приложение, которое наблюдает за событиями в таблице `events` ClickHouse:
  1. Периодически опрашивает таблицу `events` (например, каждые 3 секунды).
  2. Выводит информацию о новых событиях в консоль или лог.
  3. Позволяет отслеживать, какие записи были добавлены в `my_table` и с какими данными.
  4. Не взаимодействует напрямую с Kafka, работает только с ClickHouse.
  
## Сборка и запуск контейнеров
Сборка
```bash
docker compose up --build
```
Запуск
```bash
docker compose up -d
```

## Запуск наблюдателя событий из clickhouse
```bash
docker exec -it clickhouse-kafka-events_watcher-1 sh
```
В контейнере
```bash
./events_watcher
```
![title](images/events_watcher.png)

## Запуск консумера
```bash
docker exec -it clickhouse-kafka-consumer-1 sh
```
В контейнере
```bash
./consumer
```
![title](images/consumer.png)

## Запуск продюсера
```bash
docker exec -it clickhouse-kafka-producer-1 sh
```
В контейнере
```bash
./producer
```
![title](images/producer.png)


## Подключение tabix к clickhouse
http://localhost:8124

![title](images/login.png)

## UI для Apache Kafka
http://localhost:8080/

![title](images/kafka.png)

## Созданные таблицы

### 1. `my_table`  

Основная таблица для хранения объектов из Kafka:

```sql
CREATE TABLE IF NOT EXISTS default.my_table
(
    id UInt64,
    value String
)
ENGINE = MergeTree()
ORDER BY id;
```
- id — уникальный идентификатор объекта.

- value — строковое значение/payload.

### 2. kafka_input

Kafka Engine — “труба” между Kafka и ClickHouse:
```SQL
CREATE TABLE IF NOT EXISTS default.kafka_input
(
    id UInt64,
    value String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9093',
    kafka_topic_list = 'data_topic',
    kafka_group_name = 'ch_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

```

### 3. events
Журнал событий, связанных с объектами из my_table:
```SQL
CREATE TABLE IF NOT EXISTS default.events
(
    event_id UInt64,
    my_table_id UInt64,
    value String,
    event_type String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY event_id;
```
- event_id — уникальный идентификатор события.
- my_table_id — ссылка на объект из my_table.
- value — значение из my_table.
- event_type — тип события (INSERTED, INSERTED_FROM_KAFKA).
- created_at — время создания события.

## Пример переданных данных в Clickhouse
http://localhost:8124
![title](images/data.png)
![title](images/kafka_input.png)