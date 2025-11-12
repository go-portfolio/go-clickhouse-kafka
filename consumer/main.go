package main

import (
	"context"  // Import context package
	"log"
	"github.com/Shopify/sarama"
	"github.com/ClickHouse/clickhouse-go/v2"
	"encoding/json"
)

func main() {
	// Настройка Kafka
	kafkaBroker := "kafka:9093" // Брокер Kafka, указанный в Docker Compose
	topic := "data_topic"        // Топик для получения сообщений
	groupID := "clickhouse_consumer_group"

	// Создание конфигурации для консюмера
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Подключение к Kafka как Consumer
	consumer, err := sarama.NewConsumerGroup([]string{kafkaBroker}, groupID, config)
	if err != nil {
		log.Fatalf("Ошибка при создании консюмера: %v", err)
	}
	defer consumer.Close()

	// Настройка подключения к ClickHouse через Options struct
	clickhouseOptions := clickhouse.Options{
		Addr: []string{"clickhouse:9000"}, // Адрес вашего сервера ClickHouse
		Auth: clickhouse.Auth{
			Username: "default", // Имя пользователя
			Password: "",        // Пароль, если необходимо
			Database: "default", // База данных
		},
	
	}

	// Используем clickhouse.Open, передавая Options
	clickhouseClient, err := clickhouse.Open(&clickhouseOptions)
	if err != nil {
		log.Fatalf("Ошибка подключения к ClickHouse: %v", err)
	}

	// Чтение сообщений из Kafka
	for {
		if err := consumer.Consume(nil, []string{topic}, &ConsumerHandler{client: clickhouseClient}); err != nil {
			log.Fatalf("Ошибка при потреблении сообщений: %v", err)
		}
	}
}

// ConsumerHandler - обработчик сообщений
type ConsumerHandler struct {
	client clickhouse.Conn // Correct the type here to `clickhouse.Conn`
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// Обрабатываем сообщение
		var message map[string]interface{}
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("Ошибка при десериализации сообщения: %v", err)
			continue
		}

		// Вставка в ClickHouse
		// Use context.Background() as the first argument
		err := h.client.Exec(context.Background(), `
			INSERT INTO my_table (id, value) VALUES (?, ?)
		`, message["id"], message["value"])

		if err != nil {
			log.Printf("Ошибка при вставке в ClickHouse: %v", err)
			continue
		}

		session.MarkMessage(msg, "")
	}
	return nil
}
