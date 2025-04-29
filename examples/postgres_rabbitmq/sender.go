package main

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Darkemon/troutbox"
)

var _ troutbox.Sender = (*RabbitMQSender)(nil)

// RabbitMQSender implements the Sender interface for RabbitMQ.
type RabbitMQSender struct {
	channel *amqp.Channel
	queue   string
}

// NewRabbitMQSender creates a new RabbitMQSender.
func NewRabbitMQSender(conn *amqp.Connection, queue string) (*RabbitMQSender, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Declare the queue
	_, err = channel.QueueDeclare(
		queue, // name
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	return &RabbitMQSender{
		channel: channel,
		queue:   queue,
	}, nil
}

// Send sends a batch of messages to RabbitMQ.
func (s *RabbitMQSender) Send(ctx context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
	for i, msg := range messages {
		err := s.channel.Publish(
			"",      // exchange
			s.queue, // routing key
			false,   // mandatory
			false,   // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msg.Value,
			},
		)
		if err != nil {
			messages[i].MarkAsFailed()
		} else {
			messages[i].MarkAsSent()
		}
	}
	return messages, nil
}
