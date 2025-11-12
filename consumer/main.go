// consumer/main.go
package main

import (
	"fmt"
	"log"
	"os"
	"github.com/Shopify/sarama"
	"github.com/ClickHouse/clickhouse-go"
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

	// Настройка подключения к ClickHouse
	clickhouseDSN := "tcp://clickhouse:9000?username=default&password=&database=default"
	clickhouseClient, err := clickhouse.OpenDirect(clickhouseDSN)
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
	client clickhouse.Conn
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
		_, err := h.client.Exec(`
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
