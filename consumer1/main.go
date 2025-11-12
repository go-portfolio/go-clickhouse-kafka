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

	// ✅ Проверка и создание таблицы, если её нет
	if err := ensureTableExists(clickhouseClient); err != nil {
		log.Fatalf("Ошибка при проверке/создании таблицы: %v", err)
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

// ensureTableExists проверяет наличие таблицы и создаёт её, если её нет
func ensureTableExists(conn clickhouse.Conn) error {
	ctx := context.Background()

	// Проверяем наличие таблицы
	rows, err := conn.Query(ctx, "EXISTS TABLE default.my_table")
	if err != nil {
		return err
	}
	defer rows.Close()

	var exists uint8
	if rows.Next() {
		if err := rows.Scan(&exists); err != nil {
			return err
		}
	}

	// Если таблицы нет — создаём
	if exists == 0 {
		log.Println("Таблица my_table не найдена — создаю новую...")
		createTableSQL := `
			CREATE TABLE default.my_table
			(
				id UInt64,
				value String
			)
			ENGINE = MergeTree()
			ORDER BY id
		`
		if err := conn.Exec(ctx, createTableSQL); err != nil {
			return err
		}
		log.Println("✅ Таблица my_table успешно создана")
	}

	return nil
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

		// ✅ Сообщение о успешной вставке
		log.Printf("✅ Успешная вставка в ClickHouse: id=%v, value=%v", message["id"], message["value"])

		// Отметка сообщения как обработанного
		session.MarkMessage(msg, "")
	}
	return nil
}
