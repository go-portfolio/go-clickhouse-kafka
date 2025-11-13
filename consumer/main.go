package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Shopify/sarama"
)

func main() {
	kafkaBroker := "kafka:9093"
	topic := "data_topic"

	// Подключение к ClickHouse
	clickhouseClient, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"clickhouse:9000"},
		Auth: clickhouse.Auth{Username: "default", Password: "", Database: "default"},
	})
	if err != nil {
		log.Fatalf("Ошибка подключения к ClickHouse: %v", err)
	}

	ctx := context.Background()

	// ----------------------- Создание таблиц -----------------------

	// Kafka Engine для событий
	clickhouseClient.Exec(ctx, `
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
	`)

	// Основная таблица данных
	clickhouseClient.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS default.my_table
		(
			id UInt64,
			value String
		)
		ENGINE = MergeTree()
		ORDER BY id;
	`)

	// Таблица событий
	clickhouseClient.Exec(ctx, `
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
	`)

	// Materialized View для автоматической вставки из kafka_input в events
	clickhouseClient.Exec(ctx, `
		CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_to_events
		TO events
		AS
		SELECT
			now64() AS event_id,
			id AS my_table_id,
			value,
			'INSERTED_FROM_KAFKA' AS event_type,
			now() AS created_at
		FROM kafka_input;
	`)

	// ----------------------- Kafka Consumer -----------------------

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	consumer, err := sarama.NewConsumerGroup([]string{kafkaBroker}, "consumer_my_table", config)
	if err != nil {
		log.Fatalf("Ошибка создания ConsumerGroup: %v", err)
	}
	defer consumer.Close()

	handler := &ConsumerHandler{conn: clickhouseClient}

	for {
		if err := consumer.Consume(context.Background(), []string{topic}, handler); err != nil {
			log.Printf("Ошибка при потреблении: %v", err)
		}
	}
}

// ----------------------- Consumer -----------------------

type ConsumerHandler struct {
	conn clickhouse.Conn
}

func (h *ConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.Background()
	for msg := range claim.Messages() {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			log.Printf("Ошибка JSON: %v", err)
			continue
		}

		// Вставка в my_table
		if err := h.conn.Exec(ctx, "INSERT INTO my_table (id, value) VALUES (?, ?)", data["id"], data["value"]); err != nil {
			log.Printf("Ошибка вставки в my_table: %v", err)
			continue
		}

		// Создание события в events
		eventID := uint64(time.Now().UnixNano())
		if err := h.conn.Exec(ctx,
			"INSERT INTO events (event_id, my_table_id, value, event_type) VALUES (?, ?, ?, ?)",
			eventID, data["id"], data["value"], "INSERTED"); err != nil {
			log.Printf("Ошибка вставки события: %v", err)
			continue
		}

		log.Printf("✅ id=%v вставлено в my_table и создано событие в events", data["id"])
		sess.MarkMessage(msg, "")
	}
	return nil
}
