# Проект добавления sql-представлений в kafka для clickhouse в продюсере и считывания их в консумере с помощью sql (пока делается)

## Запуск контейнеров
docker compose up --build



## Сборка образа продюсера
Перейдите в папку `producer` с Dockerfile и создайте образ:
```bash
    docker build -t go-kafka-producer .
```    

Запустите продюсер:
```bash
docker run --rm --name go-kafka-producer --network clickhouse-kafka_clickhouse_network go-kafka-producer
```

## Сборка образа консумера
Перейдите в папку с Dockerfile для консюмера и создайте образ:
```bash
docker build -t go-kafka-consumer .
```

Запустите консюмера:
```bash
docker run --rm --name go-kafka-consumer --network clickhouse-kafka_clickhouse_network go-kafka-consumer
```

## Запуск консумера
```bash
docker exec -it clickhouse-kafka-consumer-1 sh
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