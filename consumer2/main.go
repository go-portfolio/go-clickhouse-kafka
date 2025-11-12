package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Shopify/sarama"
)

func main() {
	// Настройка Kafka
	kafkaBroker := "kafka:9093" // Брокер Kafka
	topic := "data_topic"       // Топик
	groupID := "clickhouse_consumer_group"

	// Конфигурация консумера
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Создание consumer group
	consumer, err := sarama.NewConsumerGroup([]string{kafkaBroker}, groupID, config)
	if err != nil {
		log.Fatalf("Ошибка при создании консюмера: %v", err)
	}
	defer consumer.Close()

	// Подключение к ClickHouse
	clickhouseOptions := clickhouse.Options{
		Addr: []string{"clickhouse:9000"},
		Auth: clickhouse.Auth{
			Username: "default",
			Password: "",
			Database: "default",
		},
	}

	clickhouseClient, err := clickhouse.Open(&clickhouseOptions)
	if err != nil {
		log.Fatalf("Ошибка подключения к ClickHouse: %v", err)
	}

	handler := &ConsumerHandler{client: clickhouseClient}

	// Основной loop для потребления сообщений
	ctx := context.Background()
	for {
		if err := consumer.Consume(ctx, []string{topic}, handler); err != nil {
			log.Printf("Ошибка при потреблении сообщений: %v", err)
		}
	}
}

// ConsumerHandler - обработчик сообщений
type ConsumerHandler struct {
	client clickhouse.Conn
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var message map[string]interface{}
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("Ошибка при десериализации сообщения: %v", err)
			continue
		}

		// Вставка в ClickHouse
		err := h.client.Exec(context.Background(), `
			INSERT INTO my_table (id, value) VALUES (?, ?)
		`, message["id"], message["value"])

		if err != nil {
			log.Printf("Ошибка при вставке в ClickHouse: %v", err)
			continue
		}

		// Отметка сообщения как обработанного
		session.MarkMessage(msg, "")
	}
	return nil
}
