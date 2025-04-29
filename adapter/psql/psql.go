package psql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Darkemon/troutbox"
)

var _ troutbox.TransactionalMessageRepository[*sql.Tx] = (*PostgresMessageRepository)(nil)

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// PostgresMessageRepository is a PostgreSQL implementation of the outbox.MessageRepository interface.
type PostgresMessageRepository struct {
	db     *sql.DB
	q      querier
	lockID int64 // a random lock ID for advisory locks
	opts   options
}

// NewPostgresMessageRepository creates a new PostgresMessageRepository instance.
// It takes a *sql.DB instance and a lock ID for advisory locks. If you have multiple
// instances of the outbox running, they should use the same lock ID to avoid conflicts.
// It panics if the db is nil.
func NewPostgresMessageRepository(db *sql.DB, lockID int64, opts ...Option) *PostgresMessageRepository {
	if db == nil {
		panic("db cannot be nil")
	}

	r := PostgresMessageRepository{
		db:     db,
		q:      db,
		lockID: lockID,
	}

	r.opts = applyOptions(opts...)

	return &r
}

// Migrate sets up the outbox table. The table is properly partitioned by timestamp with necessary indexes.
// Additionally, it creates future partitions and removes outdated ones.
func (r *PostgresMessageRepository) Migrate(ctx context.Context) error {
	query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id SERIAL NOT NULL,
            key TEXT NOT NULL,
            value BYTEA NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            retries INT NOT NULL DEFAULT 0,
            status SMALLINT NOT NULL DEFAULT 0,
			PRIMARY KEY (id, timestamp)
        ) PARTITION BY RANGE (timestamp);

        CREATE INDEX IF NOT EXISTS idx_%s_timestamp ON %s (timestamp);
        CREATE INDEX IF NOT EXISTS idx_%s_status ON %s (status);
    `, r.opts.tableName, r.opts.tableName, r.opts.tableName, r.opts.tableName, r.opts.tableName)

	_, err := r.q.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to migrate table %s: %w", r.opts.tableName, err)
	}

	return r.updatePartitions(ctx, "public")
}

// RunPartitioningJob runs a partitioning job, the call is blocking, stops when the context is done.
// On each run it creates a few partitions in the future and removes old ones.
// It uses a distributed lock to ensure that only one instance creates partitions at a time.
func (r *PostgresMessageRepository) RunPartitioningJob(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ticker := time.NewTicker(r.opts.jobPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case <-ticker.C:
			if err := r.updatePartitions(ctx, "public"); err != nil {
				r.opts.errorHandler(fmt.Errorf("failed to update partitions: %w", err))
			}
		}
	}
}

// FetchAndLock fetches a batch of messages and locks them for processing.
func (r *PostgresMessageRepository) FetchAndLock(ctx context.Context, batchSize uint) ([]troutbox.Message, error) {
	query := fmt.Sprintf(`
		SELECT id, key, value, timestamp, retries
		FROM %s
		WHERE status = %d
		ORDER BY timestamp ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
    `, r.opts.tableName, troutbox.StatusNone)

	rows, err := r.q.QueryContext(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close rows: %w", closeErr))
		}
	}()

	messages := make([]troutbox.Message, 0, batchSize)

	for rows.Next() {
		var msg troutbox.Message
		if scanErr := rows.Scan(&msg.ID, &msg.Key, &msg.Value, &msg.Timestamp, &msg.Retries); scanErr != nil {
			err = fmt.Errorf("failed to scan message: %w", scanErr)
			return nil, err
		}
		messages = append(messages, msg)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		err = fmt.Errorf("failed to iterate over rows: %w", rowsErr)
		return nil, err
	}

	return messages, nil
}

// UpdateRetryCount increments the retry count for the given message IDs.
func (r *PostgresMessageRepository) UpdateRetryCount(ctx context.Context, ids []uint64) error {
	query := fmt.Sprintf(`
        UPDATE %s
        SET retries = retries + 1
        WHERE id = ANY($1)
    `, r.opts.tableName)

	_, err := r.q.ExecContext(ctx, query, pq.Array(ids))

	return err
}

// MarkAsDead marks the given message IDs as dead.
func (r *PostgresMessageRepository) MarkAsDead(ctx context.Context, ids []uint64) error {
	query := fmt.Sprintf(`
        UPDATE %s
        SET status = %d
        WHERE id = ANY($1)
    `, r.opts.tableName, troutbox.StatusDead)

	_, err := r.q.ExecContext(ctx, query, pq.Array(ids))

	return err
}

// MarkAsSent marks the given message IDs as sent.
func (r *PostgresMessageRepository) MarkAsSent(ctx context.Context, ids []uint64) error {
	query := fmt.Sprintf(`
        UPDATE %s
        SET status = %d
        WHERE id = ANY($1)
    `, r.opts.tableName, troutbox.StatusSent)

	_, err := r.q.ExecContext(ctx, query, pq.Array(ids))

	return err
}

// AddMessage adds a new message to the outbox.
// If tx is nil, it will use the db of the repository, otherwise it will use the provided transaction.
func (r *PostgresMessageRepository) AddMessage(ctx context.Context, tx *sql.Tx, msg troutbox.Message) error {
	query := fmt.Sprintf(`
        INSERT INTO %s (key, value, timestamp)
        VALUES ($1, $2, $3)
    `, r.opts.tableName)

	var err error

	if tx != nil {
		_, err = tx.ExecContext(ctx, query, msg.Key, msg.Value, msg.Timestamp)
	} else {
		_, err = r.q.ExecContext(ctx, query, msg.Key, msg.Value, msg.Timestamp)
	}

	return err
}

// WithTransaction executes a function within a transaction.
func (r *PostgresMessageRepository) WithTransaction(ctx context.Context, cb troutbox.InTransactionFn) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	repoCopy := *r
	repoCopy.q = tx

	if err := cb(ctx, &repoCopy); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("failed to rollback transaction: %w", rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetPartitionName generates a partition name in the format <table_name>_<YYYYMMDD>.
func (r *PostgresMessageRepository) GetPartitionName(startTime time.Time) string {
	return fmt.Sprintf("%s_%s", r.opts.tableName, startTime.Format("20060102"))
}

// updatePartitions creates multiple partitions for future time ranges and removes old partitions.
// It uses disitributed locks to ensure that only one instance creates partitions at a time.
func (r *PostgresMessageRepository) updatePartitions(
	ctx context.Context,
	schemaName string,
) error {
	var errs error

	ctx, span := r.opts.tracer.Start(ctx, "update partitions")
	defer func() {
		if errs != nil {
			span.RecordError(errs)
			span.SetStatus(codes.Error, "failed to update partitions")
		}
		span.End()
	}()

	daysPeriod := 1                                        // TODO: make it configurable?
	startTime := time.Now().UTC().Truncate(24 * time.Hour) // current day at midnight
	period := time.Duration(daysPeriod) * 24 * time.Hour

	acquired, errs := r.acquireLock(ctx)
	if errs != nil {
		return fmt.Errorf("failed to acquire lock: %w", errs)
	}

	if !acquired {
		span.AddEvent("lock already held by another instance")
		return nil
	}

	for i := uint(0); i < r.opts.futurePartitions; i++ {
		partitionStart := startTime.Add(time.Duration(i) * period)
		partitionEnd := partitionStart.Add(period)

		if err := r.CreatePartition(ctx, partitionStart, partitionEnd); err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to create partition: %w", err))
			break
		}
	}

	currentPartition := r.GetPartitionName(startTime)

	if err := r.removeOldPartitions(ctx, schemaName, currentPartition); err != nil {
		errs = errors.Join(
			errs,
			fmt.Errorf("failed to remove old partitions: %w", err),
		)
	}

	if err := r.releaseLock(ctx); err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to release lock: %w", err))
	}

	return errs
}

// CreatePartition creates a new partition if not exists for the given time range.
// It doesn't use any locks, so it should be called only from the partitioning job.
func (r *PostgresMessageRepository) CreatePartition(ctx context.Context, startTime, endTime time.Time) error {
	partitionName := r.GetPartitionName(startTime)

	var err error

	ctx, span := r.opts.tracer.Start(
		ctx,
		"create partition",
		trace.WithAttributes(attribute.String("partition_name", partitionName)),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to create partition")
		}
		span.End()
	}()

	from := startTime.Format("2006-01-02 15:04:05")
	to := endTime.Format("2006-01-02 15:04:05")
	query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s PARTITION OF %s
        FOR VALUES FROM ('%s') TO ('%s');
    `, partitionName, r.opts.tableName, from, to)

	if _, err = r.q.ExecContext(ctx, query); err != nil {
		err = fmt.Errorf("partition %s [%s - %s]: %w", partitionName, from, to, err)
	}

	return nil
}

// removeOldPartitions takes outdated partitions, keeps the most N recent ones and removes the rest.
func (r *PostgresMessageRepository) removeOldPartitions(
	ctx context.Context,
	schemaName,
	currentPartition string,
) error {
	var err error

	ctx, span := r.opts.tracer.Start(ctx, "remove old partitions")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to remove old partitions")
		}
		span.End()
	}()

	query := fmt.Sprintf(`
		DO $$
		DECLARE
			partition_name TEXT;
		BEGIN
			FOR partition_name IN
				SELECT tablename
				FROM pg_tables
				WHERE schemaname = '%s'
				AND tablename LIKE '%s_%%'
				AND tablename < '%s'
				ORDER BY tablename DESC
				OFFSET %d
			LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || partition_name;
			END LOOP;
		END $$;
	`, schemaName, r.opts.tableName, currentPartition, r.opts.pastPartitions)

	_, err = r.q.ExecContext(ctx, query)
	return err
}

// acquireLock acquires a distributed lock using PostgreSQL's pg_advisory_lock.
// Reutrns true if the lock was acquired, false if it was already held by another instance.
func (r *PostgresMessageRepository) acquireLock(ctx context.Context) (bool, error) {
	query := "SELECT pg_try_advisory_lock($1)"
	var acquired bool
	err := r.q.QueryRowContext(ctx, query, r.lockID).Scan(&acquired)
	return acquired, err
}

// releaseLock releases a distributed lock using PostgreSQL's pg_advisory_unlock.
func (r *PostgresMessageRepository) releaseLock(ctx context.Context) error {
	query := "SELECT pg_advisory_unlock($1)"
	if _, err := r.q.ExecContext(ctx, query, r.lockID); err != nil {
		return err
	}
	return nil
}
