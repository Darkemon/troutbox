package troutbox

import (
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type options struct {
	tracer       trace.Tracer
	meter        metric.Meter
	batchSize    uint
	maxRetries   uint
	period       time.Duration
	sendTimeout  time.Duration
	errorHandler func(error)
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

// WithOtelMeter sets the OpenTelemetry meter for the outbox.
// By default, the default OpenTelemetry meter is used.
func WithOtelMeter(meter metric.Meter) Option {
	return func(o *options) {
		if meter != nil {
			o.meter = meter
		}
	}
}

// WithBatchSize sets the batch size to fetch messages from the repository.
// The default batch size is 10.
func WithBatchSize(size uint) Option {
	return func(o *options) {
		if size > 0 {
			o.batchSize = size
		}
	}
}

// WithMaxRetries sets the maximum number of retries for sending messages.
// The default maximum number of retries is 3.
func WithMaxRetries(retries uint) Option {
	return func(o *options) {
		o.maxRetries = retries
	}
}

// WithPeriod sets the period for sending messages.
// The default period is 5 seconds.
func WithPeriod(period time.Duration) Option {
	return func(o *options) {
		if period > 0 {
			o.period = period
		}
	}
}

// WithSendTimeout sets the timeout for sending messages, including communication with db.
// The default timeout is 2 second.
func WithSendTimeout(timeout time.Duration) Option {
	return func(o *options) {
		if timeout > 0 {
			o.sendTimeout = timeout
		}
	}
}

// WithErrorHandler sets the error handler for the outbox.
// The error handler is called when an error occurs while sending messages.
// The default error handler logs the error to the standard logger.
// Be careful, it blocks the main sending loop.
func WithErrorHandler(handler func(error)) Option {
	return func(o *options) {
		o.errorHandler = handler
	}
}

// applyOptions applies the provided options to the default options.
func applyOptions(opts ...Option) options {
	defaultErrorHandler := func(err error) {
		log.Println(err)
	}

	defaultOpts := options{
		tracer:       otel.GetTracerProvider().Tracer("github.com/Darkemon/troutbox"),
		meter:        otel.GetMeterProvider().Meter("github.com/Darkemon/troutbox"),
		batchSize:    10,
		maxRetries:   3,
		period:       5 * time.Second,
		sendTimeout:  2 * time.Second,
		errorHandler: defaultErrorHandler,
	}

	for _, opt := range opts {
		opt(&defaultOpts)
	}

	return defaultOpts
}
