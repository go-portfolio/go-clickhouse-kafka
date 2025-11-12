// producer/main.go
package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"time"
	"encoding/json"
)

func main() {
	// Настройка Kafka
	kafkaBroker := "kafka:9093" // Брокер Kafka, указанный в Docker Compose
	topic := "data_topic"        // Топик для отправки сообщений

	// Создание конфигурации для продюсера
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	// Создание продюсера
	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, config)
	if err != nil {
		log.Fatalf("Ошибка при создании продюсера: %v", err)
	}
	defer producer.Close()

	// Генерация и отправка сообщений в Kafka
	for i := 0; i < 100; i++ {
		message := map[string]interface{}{
			"id":    i,
			"value": fmt.Sprintf("data_%d", i),
		}

		// Преобразуем сообщение в JSON
		messageJSON, err := json.Marshal(message)
		if err != nil {
			log.Fatalf("Ошибка при сериализации сообщения: %v", err)
		}

		// Отправляем сообщение
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(messageJSON),
		}

		// Отправка сообщения синхронно
		_, _, err = producer.SendMessage(msg)
		if err != nil {
			log.Printf("Ошибка при отправке сообщения: %v", err)
		} else {
			log.Printf("Отправлено сообщение: %s", messageJSON)
		}

		// Пауза между отправками сообщений
		time.Sleep(1 * time.Second)
	}
}
