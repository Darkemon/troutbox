package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// addMessages simulates adding messages to the outbox in a loop.
// Stops when the context is done.
func addMessages(ctx context.Context, outboxService *Outbox) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for i := 0; true; i++ {
		select {
		case <-ctx.Done():
			log.Println("Received shutdown signal, stop adding messages...")
			return
		case <-timer.C:
			tr := (*sql.Tx)(nil) // replace with actual transaction if needed
			val := fmt.Sprintf("message-%d", i)

			if err := outboxService.AddMessage(ctx, tr, "key", []byte(val)); err != nil {
				log.Printf("Failed to add message to outbox: %v", err)
			} else {
				log.Printf("Added message to outbox: %s", val)
			}

			// Reset the timer for the next message.
			timer.Reset(time.Duration(500+rand.Intn(1500)) * time.Millisecond)
		}
	}
}

func main() {
	log.Println("Connecting to PostgreSQL...")
	db, err := sql.Open("postgres", "user=postgres password=postgres dbname=test_db sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	log.Println("Connecting to RabbitMQ...")
	rabbitConn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	rabbitSender, err := NewRabbitMQSender(rabbitConn, "outbox_queue")
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ sender: %v", err)
	}

	outboxService, err := NewOutbox(db, rabbitSender)
	if err != nil {
		log.Fatalf("Failed to create outbox service: %v", err)
	}

	// Set up signal handling for graceful shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go addMessages(ctx, outboxService)

	log.Println("Starting outbox service...")
	if err := outboxService.Run(ctx); err != nil {
		log.Fatalf("Outbox service failed: %v", err)
	}
}
