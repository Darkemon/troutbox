package troutbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type SendStatus uint8

const (
	StatusNone SendStatus = 0 // not tried to send
	StatusSent SendStatus = 1 // sent successfully
	StatusFail SendStatus = 2 // failed to send
	StatusDead SendStatus = 3 // retries exceeded, message is dead
)

type Message struct {
	ID        uint64
	Key       string
	Value     []byte
	Timestamp time.Time
	Retries   uint

	status SendStatus // by default it's statusNone
}

func (m *Message) MarkAsSent() {
	m.status = StatusSent
}

func (m *Message) MarkAsFailed() {
	m.status = StatusFail
}

type Sender interface {
	// Send sends a batch of messages to the message broker. The passed messages are
	// sorted by timestamp in ascending order.
	// It should return the same list of messages with updated statuses
	// (method SetStatusSent or SetStatusFail of the Message). Returned error should be
	// returned if the sending failed completely or partially.
	Send(ctx context.Context, msg []Message) ([]Message, error)
}

// MessageRepository describes an interface for interacting with the outbox storage (e.g., a database).
type MessageRepository interface {
	// FetchAndLock fetches a batch of ready-to-sent messages from the outbox and locks them for processing.
	// It fetches at most batchSize messages sorted by timestamp in ascending order.
	FetchAndLock(ctx context.Context, batchSize uint) ([]Message, error)
	// UpdateRetryCount updates the retry count for the message with the given ID.
	// It's called when the message is retried.
	UpdateRetryCount(ctx context.Context, ids []uint64) error
	// MarkAsDead marks the message as dead so that it won't be retried anymore.
	// It's called when the message cannot be sent after maxRetries.
	MarkAsDead(ctx context.Context, ids []uint64) error
	// MarkAsSent marks the message as sent.
	MarkAsSent(ctx context.Context, ids []uint64) error
}

type InTransactionFn func(ctx context.Context, s MessageRepository) error

// TransactionalMessageRepository extends [MessageRepository] to support transactional operations.
type TransactionalMessageRepository[T any] interface {
	MessageRepository

	// AddMessage adds a message to the outbox.
	AddMessage(ctx context.Context, tx T, msg Message) error
	// WithTransaction executes a function within a transaction.
	// The function should return an error if the transaction should be rolled back.
	WithTransaction(ctx context.Context, cb InTransactionFn) error
}

type Outbox[T any] struct {
	storage TransactionalMessageRepository[T]
	sender  Sender
	opts    options

	// OpenTelemetry metrics.
	messagesSent    metric.Int64Counter
	messagesFailed  metric.Int64Counter
	messagesRetried metric.Int64Counter
	messagesDead    metric.Int64Counter
}

// NewOutbox creates a new Outbox instance.
func NewOutbox[T any](storage TransactionalMessageRepository[T], sender Sender, opts ...Option) (*Outbox[T], error) {
	o := Outbox[T]{
		storage: storage,
		sender:  sender,
		opts:    applyOptions(opts...),
	}

	if err := o.createCounters(); err != nil {
		return nil, fmt.Errorf("failed to create counters: %w", err)
	}

	return &o, nil
}

func (o *Outbox[T]) createCounters() error {
	var err error

	if o.messagesSent, err = o.opts.meter.Int64Counter(
		"outbox_messages_sent",
		metric.WithDescription("Number of messages successfully sent"),
	); err != nil {
		return fmt.Errorf("messages sent: %w", err)
	}

	if o.messagesFailed, err = o.opts.meter.Int64Counter(
		"outbox_messages_failed",
		metric.WithDescription("Number of messages that failed to send"),
	); err != nil {
		return fmt.Errorf("messages failed: %w", err)
	}

	if o.messagesRetried, err = o.opts.meter.Int64Counter(
		"outbox_messages_retried",
		metric.WithDescription("Number of messages retried"),
	); err != nil {
		return fmt.Errorf("messages retried: %w", err)
	}

	if o.messagesDead, err = o.opts.meter.Int64Counter(
		"outbox_messages_dead",
		metric.WithDescription("Number of messages marked as dead"),
	); err != nil {
		return fmt.Errorf("messages dead: %w", err)
	}

	return nil
}

// AddMessage adds a message to the outbox.
// It sets the timestamp to the current UTC time.
func (o *Outbox[T]) AddMessage(ctx context.Context, tx T, key string, value []byte) error {
	msg := Message{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UTC(),
		Retries:   0,
	}

	return o.storage.AddMessage(ctx, tx, msg)
}

// Run starts the outbox loop. It fetches a batch of messages from the DB every period (5 seconds by default)
// and sends them at least once. It marks the messages as sent after they are successfully sent.
// The loop can be stopped by cancelling the context.
func (o *Outbox[T]) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	timer := time.NewTimer(0) // start immediately
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			if err := o.storage.WithTransaction(ctx, o.sendBatch); err != nil {
				o.opts.errorHandler(fmt.Errorf("sending failed: %w", err))
			}
			timer.Reset(o.opts.period)
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

func (o *Outbox[T]) sendBatch(ctx context.Context, s MessageRepository) error {
	var err error

	ctx, span := o.opts.tracer.Start(
		ctx,
		"send batch",
		trace.WithAttributes(attribute.Int("batch_size", int(o.opts.batchSize))),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to send batch")
		}
		span.End()
	}()

	ctx, cancel := context.WithTimeout(ctx, o.opts.sendTimeout)
	defer cancel()

	// Fetch batch of messages.
	messages, err := s.FetchAndLock(ctx, o.opts.batchSize)
	if err != nil {
		return fmt.Errorf("failed to fetch messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	// Send messages.
	messages, err = o.sender.Send(ctx, messages)
	if err != nil {
		o.opts.errorHandler(fmt.Errorf("couldn't send messages: %w", err))
	}

	sent := make([]uint64, 0)
	toRetry := make([]uint64, 0)
	toDead := make([]uint64, 0)

	for _, msg := range messages {
		switch msg.status {
		case StatusSent:
			sent = append(sent, msg.ID)
		case StatusFail:
			// If the message failed to send, we need to check if we should retry or mark it as dead.
			if msg.Retries+1 < o.opts.maxRetries {
				toRetry = append(toRetry, msg.ID)
			} else {
				toDead = append(toDead, msg.ID)
			}
		case StatusNone:
			// The message was not sent, so we need to retry it but not increase the retry count.
			// So basically we do nothing with it.
		}
	}

	if len(sent) > 0 {
		o.messagesSent.Add(ctx, int64(len(sent)))

		if err := s.MarkAsSent(ctx, sent); err != nil {
			return fmt.Errorf("failed to mark messages as sent: %w", err)
		}
	}

	if len(toRetry) > 0 {
		o.messagesRetried.Add(ctx, int64(len(toRetry)))

		if err := s.UpdateRetryCount(ctx, toRetry); err != nil {
			return fmt.Errorf("failed to update retry count: %w", err)
		}
	}

	if len(toDead) > 0 {
		o.messagesDead.Add(ctx, int64(len(toDead)))

		if err := s.MarkAsDead(ctx, toDead); err != nil {
			return fmt.Errorf("failed to mark messages as dead: %w", err)
		}
	}

	return nil
}
