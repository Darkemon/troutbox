module github.com/Darkemon/troutbox/examples/postgresrabbitmq

go 1.24.0

replace github.com/Darkemon/troutbox => ../..

require (
	github.com/Darkemon/troutbox v0.0.0
	github.com/lib/pq v1.11.1
	github.com/rabbitmq/amqp091-go v1.10.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.40.0 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
)
