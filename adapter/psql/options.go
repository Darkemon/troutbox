package psql

import (
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type options struct {
	tracer           trace.Tracer
	tableName        string
	futurePartitions uint // how many future partitions to create
	pastPartitions   uint // how many past partitions to keep
	jobPeriod        time.Duration
	errorHandler     func(error)
}

type Option func(*options)

// WithOtelTracer sets the OpenTelemetry tracer for the outbox.
// By default, the default OpenTelemetry tracer is used.
func WithOtelTracer(tracer trace.Tracer) Option {
	return func(o *options) {
		if tracer != nil {
			o.tracer = tracer
		}
	}
}

// WithTableName sets the table name for the outbox messages.
// Default is "outbox_messages".
func WithTableName(name string) Option {
	return func(o *options) {
		o.tableName = name
	}
}

// WithFuturePartitions sets the number of future partitions to create including the current one.
// Default is 2.
func WithFuturePartitions(n uint) Option {
	return func(o *options) {
		if n > 0 {
			o.futurePartitions = n
		}
	}
}

// WithPastPartitions sets the number of past partitions to keep.
// Default is 1.
func WithPastPartitions(n uint) Option {
	return func(o *options) {
		o.pastPartitions = n
	}
}

// WithJobPeriod sets the job period for partition management.
// Default is 1 hour.
func WithJobPeriod(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.jobPeriod = d
		}
	}
}

// WithErrorHandler sets the error handler.
// The error handler is called when an error occurs while creating partitions.
// The default error handler logs the error to the standard logger.
// Be careful, it blocks the job's loop.
func WithErrorHandler(handler func(error)) Option {
	return func(o *options) {
		o.errorHandler = handler
	}
}

// applyOptions applies the provided options to the default options.
func applyOptions(opts ...Option) options {
	defualtErrorHandler := func(err error) {
		log.Println(err)
	}

	defaultOpts := options{
		tracer:           otel.GetTracerProvider().Tracer("github.com/Darkemon/troutbox/adapter/psql"),
		tableName:        "outbox_messages",
		futurePartitions: 2,
		pastPartitions:   1,
		jobPeriod:        time.Hour,
		errorHandler:     defualtErrorHandler,
	}

	for _, opt := range opts {
		opt(&defaultOpts)
	}

	return defaultOpts
}
