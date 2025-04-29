package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Darkemon/troutbox"
	"github.com/Darkemon/troutbox/adapter/psql"
)

const lockID = 42

type Outbox struct {
	*troutbox.Outbox[*sql.Tx]
	repo *psql.PostgresMessageRepository
}

func NewOutbox(db *sql.DB, sender troutbox.Sender) (*Outbox, error) {
	o := Outbox{}

	// Create the PostgreSQL adapter.
	o.repo = psql.NewPostgresMessageRepository(
		db,
		lockID,
		psql.WithTableName("outbox_messages"),
		psql.WithFuturePartitions(3),
		psql.WithPastPartitions(1),
		psql.WithJobPeriod(1*time.Hour),
	)

	outboxService, err := troutbox.NewOutbox(
		o.repo,
		sender,
		troutbox.WithPeriod(time.Second),
		troutbox.WithBatchSize(10),
		troutbox.WithMaxRetries(5),
		troutbox.WithSendTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}

	o.Outbox = outboxService

	return &o, nil
}

func (o *Outbox) Run(ctx context.Context) error {
	err := o.repo.Migrate(ctx)
	if err != nil {
		return fmt.Errorf("failed to migrate: %w", err)
	}

	// Run partitioning job.
	go func() {
		_ = o.repo.RunPartitioningJob(ctx)
	}()

	return o.Run(ctx)
}
