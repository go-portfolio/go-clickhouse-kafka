package main

import (
	"context"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	// Подключение к ClickHouse
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"clickhouse:9000"},
		Auth: clickhouse.Auth{
			Username: "default",
			Password: "",
			Database: "default",
		},
	})
	if err != nil {
		log.Fatalf("Ошибка подключения к ClickHouse: %v", err)
	}

	ctx := context.Background()
	ticker := time.NewTicker(3 * time.Second) // проверка каждые 3 секунды
	defer ticker.Stop()

	var lastEventID uint64 = 0

	for range ticker.C {
		rows, err := conn.Query(ctx, `
			SELECT event_id, my_table_id, value, event_type, created_at
			FROM events
			WHERE event_id > ?
			ORDER BY event_id ASC
		`, lastEventID)
		if err != nil {
			log.Printf("Ошибка чтения событий: %v", err)
			continue
		}

		var eventID, myTableID uint64
		var value, eventType string
		var createdAt time.Time

		for rows.Next() {
			if err := rows.Scan(&eventID, &myTableID, &value, &eventType, &createdAt); err != nil {
				log.Printf("Ошибка сканирования: %v", err)
				continue
			}
			log.Printf("Новое событие: event_id=%v, my_table_id=%v, value=%s, type=%s, created_at=%v",
				eventID, myTableID, value, eventType, createdAt)

			if eventID > lastEventID {
				lastEventID = eventID
			}
		}
		rows.Close()
	}
}
