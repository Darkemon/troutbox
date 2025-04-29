//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/Darkemon/troutbox"
	"github.com/Darkemon/troutbox/adapter/psql"
	"github.com/Darkemon/troutbox/internal/mocks"
)

const (
	lockID      = 12345
	maxRetries  = uint(3)
	batchSize   = uint(5)
	sendTimeout = 200 * time.Millisecond
)

type OutboxPsqlTestSuite struct {
	suite.Suite
	db         *sql.DB
	mockSender *mocks.MockSender
	cancelFn   []context.CancelFunc
	wg         sync.WaitGroup
}

// Run the test suite.
func TestOutboxPsqlTestSuite(t *testing.T) {
	suite.Run(t, new(OutboxPsqlTestSuite))
}

func (s *OutboxPsqlTestSuite) SetupTest() {
	s.cancelFn = nil
	s.mockSender = mocks.NewMockSender(s.T())

	s.db = s.newDBConn()

	if _, err := s.db.Exec("DROP TABLE IF EXISTS outbox_messages"); err != nil {
		s.T().Fatalf("Failed to drop outbox table: %v", err)
	}

	if _, err := s.db.Exec("DROP TABLE IF EXISTS outbox_messages_partitions_test"); err != nil {
		s.T().Fatalf("Failed to drop outbox table: %v", err)
	}

	repo := psql.NewPostgresMessageRepository(s.db, lockID)

	if err := repo.Migrate(s.T().Context()); err != nil {
		s.T().Fatalf("Failed to migrate database: %v", err)
	}
}

func (s *OutboxPsqlTestSuite) TearDownTest() {
	defer s.db.Close()

	if len(s.cancelFn) == 0 {
		return
	}

	for _, cancel := range s.cancelFn {
		cancel()
	}

	closeCh := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(closeCh)
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case <-closeCh:
	case <-timer.C:
		s.T().Error("Timeout waiting for outbox to finish")
	}
}

func (s *OutboxPsqlTestSuite) manageDBContainer(cmd string) {
	if cmd != "start" && cmd != "stop" {
		s.T().Fatalf("Invalid command: %s", cmd)
	}

	dockerCmd := exec.Command("docker", "compose", cmd, "postgres")
	dockerCmd.Stdout = os.Stdout
	dockerCmd.Stderr = os.Stderr

	if err := dockerCmd.Run(); err != nil {
		s.T().Fatalf("Failed to %s PostgreSQL container: %v", cmd, err)
	}
}

func (s *OutboxPsqlTestSuite) newDBConn() *sql.DB {
	db, err := sql.Open("postgres", "user=postgres password=postgres dbname=test_db sslmode=disable")
	if err != nil {
		s.T().Fatalf("Failed to connect to database: %v", err)
	}
	return db
}

// newOutbox creates a new outbox instance for testing.
func (s *OutboxPsqlTestSuite) newOutbox(sender troutbox.Sender) *troutbox.Outbox[*sql.Tx] {
	s.wg.Add(1)

	if sender == nil {
		sender = s.mockSender
	}

	o, err := troutbox.NewOutbox(
		psql.NewPostgresMessageRepository(s.db, lockID, psql.WithTableName("outbox_messages")),
		sender,
		troutbox.WithMaxRetries(maxRetries),
		troutbox.WithBatchSize(batchSize),
		troutbox.WithPeriod(50*time.Millisecond),
		troutbox.WithSendTimeout(sendTimeout),
	)
	s.Require().NoError(err, "Failed to create outbox")

	return o
}

func (s *OutboxPsqlTestSuite) startOutbox(ctx context.Context, outb *troutbox.Outbox[*sql.Tx]) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancelFn = append(s.cancelFn, cancel)

	go func() {
		defer func() {
			s.wg.Done()

			if r := recover(); r != nil {
				switch r := r.(type) {
				case string:
					if r == "test panic" {
						cancel() // simulate the process stopped completely
						return
					}
				}
				panic(r)
			}
		}()

		if err := outb.Run(ctx); err != nil {
			s.T().Errorf("Outbox run failed: %v", err)
		}
	}()
}

func (s *OutboxPsqlTestSuite) getPartitions(ctx context.Context, tableName string) []string {
	query := fmt.Sprintf(`
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
		AND tablename LIKE '%s_%%'
		ORDER BY tablename DESC
	`, tableName)

	rows, err := s.db.QueryContext(ctx, query)
	s.Require().NoError(err, "failed to query partitions")

	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		s.Require().NoError(err, "failed to scan table name")
		tables = append(tables, table)
	}

	err = rows.Err()
	s.Require().NoError(err, "failed to iterate over rows")

	return tables
}

// assertMessagesCount checks the number of messages in the outbox table with the given status.
func (s *OutboxPsqlTestSuite) assertMessagesCount(expectedCount int, status troutbox.SendStatus) {
	db := s.newDBConn()
	defer db.Close()

	rows, err := db.Query("SELECT COUNT(*) FROM outbox_messages WHERE status = $1", status)
	if err != nil {
		s.T().Fatalf("Failed to query sent messages: %v", err)
	}
	defer rows.Close()

	var actualCount int
	if rows.Next() {
		if err := rows.Scan(&actualCount); err != nil {
			s.T().Fatalf("Failed to scan sent messages count: %v", err)
		}
	}

	s.Assert().Equal(expectedCount, actualCount, "Expected %d messages, got %d", expectedCount, actualCount)
}

func (s *OutboxPsqlTestSuite) assertMessageEquals(msg troutbox.Message, key string, value []byte, timestamp time.Time) {
	s.Assert().Equal(key, msg.Key)
	s.Assert().Equal(value, msg.Value)
	s.Assert().WithinDuration(timestamp, msg.Timestamp, time.Millisecond)
}

// Ensure messages are fetched, sent, and marked as sent successfully.
func (s *OutboxPsqlTestSuite) TestBasic() {
	outb := s.newOutbox(nil)

	// Message to be sent.
	key1 := "key1"
	key2 := "key2"
	value1 := []byte("value1")

	var (
		expTimestamp1 *time.Time
		expTimestamp2 *time.Time
	)

	// Expected call to the sender.
	s.mockSender.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 2)
			s.assertMessageEquals(messages[0], key1, value1, *expTimestamp1)
			s.assertMessageEquals(messages[1], key2, []byte{}, *expTimestamp2)
			messages[0].MarkAsSent()
			messages[1].MarkAsSent()
			return messages, nil
		}).Times(1)

	// Add the message to the outbox.
	now1 := time.Now()
	expTimestamp1 = &now1
	if err := outb.AddMessage(s.T().Context(), nil, key1, value1); err != nil {
		s.T().Fatalf("Failed to add message 1: %v", err)
	}

	now2 := time.Now()
	expTimestamp2 = &now2
	if err := outb.AddMessage(s.T().Context(), nil, key2, nil); err != nil {
		s.T().Fatalf("Failed to add message 2: %v", err)
	}

	// Run the outbox after adding the messages to ensure they are processed at once.
	s.startOutbox(s.T().Context(), outb)

	// Wait for a short period to allow the outbox to process the message.
	time.Sleep(100 * time.Millisecond)
	s.assertMessagesCount(2, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure messages that fail to send are retried up to the maximum retry count.
func (s *OutboxPsqlTestSuite) TestRetry() {
	outb := s.newOutbox(nil)

	key1 := "key1"
	key2 := "key2"
	value1 := []byte("value1")
	value2 := []byte("value2")

	var (
		expTimestamp1 *time.Time
		expTimestamp2 *time.Time
	)

	s.mockSender.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 2)
			s.Assert().Less(messages[0].Retries, maxRetries)
			s.Assert().Less(messages[1].Retries, maxRetries)
			s.assertMessageEquals(messages[0], key1, value1, *expTimestamp1)
			s.assertMessageEquals(messages[1], key2, value2, *expTimestamp2)

			if messages[0].Retries == maxRetries-1 { // last retry
				messages[0].MarkAsSent()
			} else {
				messages[0].MarkAsFailed()
			}

			if messages[1].Retries == maxRetries-1 { // last retry
				messages[1].MarkAsSent()
			} else {
				messages[1].MarkAsFailed()
			}

			return messages, nil
		}).Times(int(maxRetries))

	now1 := time.Now()
	expTimestamp1 = &now1
	if err := outb.AddMessage(s.T().Context(), nil, key1, value1); err != nil {
		s.T().Fatalf("Failed to add message 1: %v", err)
	}

	now2 := time.Now()
	expTimestamp2 = &now2
	if err := outb.AddMessage(s.T().Context(), nil, key2, value2); err != nil {
		s.T().Fatalf("Failed to add message 2: %v", err)
	}

	s.startOutbox(s.T().Context(), outb)

	time.Sleep(200 * time.Millisecond)
	s.assertMessagesCount(2, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure messages that exceed the maximum retry count are marked as dead.
func (s *OutboxPsqlTestSuite) TestDead() {
	outb := s.newOutbox(nil)

	key1 := "key1"
	key2 := "key2"
	value1 := []byte("value1")
	value2 := []byte("value2")

	var (
		expTimestamp1 *time.Time
		expTimestamp2 *time.Time
	)

	s.mockSender.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 2)
			s.Assert().Less(messages[0].Retries, maxRetries)
			s.Assert().Less(messages[1].Retries, maxRetries)
			s.assertMessageEquals(messages[0], key1, value1, *expTimestamp1)
			s.assertMessageEquals(messages[1], key2, value2, *expTimestamp2)

			messages[0].MarkAsFailed()
			messages[1].MarkAsFailed()

			return messages, nil
		}).Times(int(maxRetries))

	now1 := time.Now()
	expTimestamp1 = &now1
	if err := outb.AddMessage(s.T().Context(), nil, key1, value1); err != nil {
		s.T().Fatalf("Failed to add message 1: %v", err)
	}

	now2 := time.Now()
	expTimestamp2 = &now2
	if err := outb.AddMessage(s.T().Context(), nil, key2, value2); err != nil {
		s.T().Fatalf("Failed to add message 2: %v", err)
	}

	s.startOutbox(s.T().Context(), outb)

	time.Sleep(200 * time.Millisecond)
	s.assertMessagesCount(0, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(2, troutbox.StatusDead)
}

// Ensure some messages are sent successfully while others fail.
func (s *OutboxPsqlTestSuite) TestPartialRetry() {
	outb := s.newOutbox(nil)

	key1 := "key1"
	key2 := "key2"
	value1 := []byte("value1")
	value2 := []byte("value2")
	i := 0

	var (
		expTimestamp1 *time.Time
		expTimestamp2 *time.Time
	)

	s.mockSender.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			switch i {
			case 0:
				s.Assert().Len(messages, 2)
				s.Assert().Less(messages[0].Retries, maxRetries)
				s.Assert().Less(messages[1].Retries, maxRetries)
				s.assertMessageEquals(messages[0], key1, value1, *expTimestamp1)
				s.assertMessageEquals(messages[1], key2, value2, *expTimestamp2)
				messages[0].MarkAsSent()
				messages[1].MarkAsFailed()
				i++
			case 1, 2:
				s.Assert().Len(messages, 1)
				s.Assert().Less(messages[0].Retries, maxRetries)
				s.assertMessageEquals(messages[0], key2, value2, *expTimestamp2)
				messages[0].MarkAsFailed()
				i++
			}

			return messages, nil
		}).Times(int(maxRetries))

	now1 := time.Now()
	expTimestamp1 = &now1
	if err := outb.AddMessage(s.T().Context(), nil, key1, value1); err != nil {
		s.T().Fatalf("Failed to add message 1: %v", err)
	}

	now2 := time.Now()
	expTimestamp2 = &now2
	if err := outb.AddMessage(s.T().Context(), nil, key2, value2); err != nil {
		s.T().Fatalf("Failed to add message 2: %v", err)
	}

	s.startOutbox(s.T().Context(), outb)

	time.Sleep(200 * time.Millisecond)
	s.assertMessagesCount(1, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(1, troutbox.StatusDead)
}

// Ensure no errors occur when there are no messages to process.
func (s *OutboxPsqlTestSuite) TestNoMessages() {
	outb := s.newOutbox(nil)
	s.startOutbox(s.T().Context(), outb)

	s.mockSender.AssertNotCalled(s.T(), "Send")

	time.Sleep(100 * time.Millisecond)
	s.assertMessagesCount(0, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure messages are added to the outbox within a transaction.
func (s *OutboxPsqlTestSuite) TestAddMessageInTransaction() {
	outb := s.newOutbox(nil)
	s.startOutbox(s.T().Context(), outb)

	key1 := "key1"
	key2 := "key2"
	value1 := []byte("value1")
	value2 := []byte("value2")

	var expTimestamp1 *time.Time

	s.mockSender.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 1)
			s.assertMessageEquals(messages[0], key1, value1, *expTimestamp1)
			messages[0].MarkAsSent()
			return messages, nil
		}).Times(1)

	// Add the message 1 and commit the transaction.
	tx, err := s.db.BeginTx(s.T().Context(), nil)
	s.Require().NoError(err)

	now1 := time.Now()
	expTimestamp1 = &now1
	if err := outb.AddMessage(s.T().Context(), tx, key1, value1); err != nil {
		s.T().Fatalf("Failed to add message 1: %v", err)
	}

	err = tx.Commit()
	s.Require().NoError(err)

	// Add the message 2 and rollback the transaction.
	tx, err = s.db.BeginTx(s.T().Context(), nil)
	s.Require().NoError(err)

	if err := outb.AddMessage(s.T().Context(), tx, key2, value2); err != nil {
		s.T().Fatalf("Failed to add message 2: %v", err)
	}

	err = tx.Rollback()
	s.Require().NoError(err)

	// Wait for a short period to allow the outbox to process the message.
	time.Sleep(100 * time.Millisecond)
	s.assertMessagesCount(1, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure multiple workers can process the same outbox table without conflicts.
func (s *OutboxPsqlTestSuite) TestMultipleWorkers() {
	sender1 := mocks.NewMockSender(s.T())
	outb1 := s.newOutbox(sender1)

	sender2 := mocks.NewMockSender(s.T())
	outb2 := s.newOutbox(sender2)

	// It'll get only one batch.
	sender1.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 5)
			time.Sleep(100 * time.Millisecond) // simulate some latency
			for i := 0; i < len(messages); i++ {
				messages[i].MarkAsSent()
			}
			return messages, nil
		}).Times(1)

	// It'll handle two batches. It won't get the 3rd batch because the first worker has locked it.
	sender2.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 5)
			for i := 0; i < len(messages); i++ {
				messages[i].MarkAsSent()
			}
			return messages, nil
		}).Times(2)

	// We add 15 messages, batch size is 5, so 3 batches will be sent.
	for i := 1; i <= 15; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		if err := outb1.AddMessage(s.T().Context(), nil, key, value); err != nil {
			s.T().Fatalf("Failed to add message 1: %v", err)
		}
	}

	// Start the outboxes after adding the messages to ensure they are processed.
	s.startOutbox(s.T().Context(), outb1)
	s.startOutbox(s.T().Context(), outb2)

	time.Sleep(500 * time.Millisecond)
	s.assertMessagesCount(15, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure messages locked by a crashed worker are retried by another worker.
func (s *OutboxPsqlTestSuite) TestCrashedWorker() {
	sender1 := mocks.NewMockSender(s.T())
	outb1 := s.newOutbox(sender1)

	sender2 := mocks.NewMockSender(s.T())
	outb2 := s.newOutbox(sender2)

	// It'll get only one batch and crach.
	sender1.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 5)
			panic("test panic") // simulate a crash
		}).Times(1)

	// It'll handle all batches as the first worker crashed.
	sender2.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			time.Sleep(100 * time.Millisecond) // simulate some latency, let the first worker get a batch
			s.Assert().Len(messages, 5)
			for i := 0; i < len(messages); i++ {
				messages[i].MarkAsSent()
			}
			return messages, nil
		}).Times(3)

	// We add 15 messages, batch size is 5, so 3 batches will be sent.
	for i := 1; i <= 15; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		if err := outb1.AddMessage(s.T().Context(), nil, key, value); err != nil {
			s.T().Fatalf("Failed to add message 1: %v", err)
		}
	}

	// Start the outboxes after adding the messages to ensure they are processed.
	s.startOutbox(s.T().Context(), outb1)
	s.startOutbox(s.T().Context(), outb2)

	time.Sleep(500 * time.Millisecond)
	s.assertMessagesCount(15, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure the outbox handles database connection failures gracefully.
func (s *OutboxPsqlTestSuite) TestDatabaseConnectionFailure() {
	outb := s.newOutbox(nil)
	i := 0

	s.mockSender.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 1)

			if i == 0 {
				// Simulate database connection failure.
				s.manageDBContainer("stop")
				i++
			}

			messages[0].MarkAsSent()
			return messages, nil
		}).Times(2)

	if err := outb.AddMessage(s.T().Context(), nil, "key", []byte("value")); err != nil {
		s.T().Fatalf("Failed to add message: %v", err)
	}

	s.startOutbox(s.T().Context(), outb)
	time.Sleep(time.Second)

	// Restart the database container.
	s.manageDBContainer("start")

	time.Sleep(time.Second)
	s.assertMessagesCount(1, troutbox.StatusSent)
	s.assertMessagesCount(0, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure the outbox handles message processing timeouts gracefully.
func (s *OutboxPsqlTestSuite) TestMessageProcessingTimeout() {
	outb := s.newOutbox(nil)
	s.startOutbox(s.T().Context(), outb)

	s.mockSender.EXPECT().
		Send(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, messages []troutbox.Message) ([]troutbox.Message, error) {
			s.Assert().Len(messages, 1)

			select {
			case <-ctx.Done():
				// context.Canceled appears when the test is being finished
				s.Assert().True(errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled))
				messages[0].MarkAsFailed() // it doesn't matter if we mark it as sent or failed
			case <-time.After(2 * sendTimeout):
				s.T().Error("should not reach here")
			}

			return messages, ctx.Err()
		})

	if err := outb.AddMessage(s.T().Context(), nil, "key", []byte("value")); err != nil {
		s.T().Fatalf("Failed to add message: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	s.assertMessagesCount(0, troutbox.StatusSent)
	s.assertMessagesCount(1, troutbox.StatusNone)
	s.assertMessagesCount(0, troutbox.StatusDead)
}

// Ensure the repository adapter updates partitions correctly.
func (s *OutboxPsqlTestSuite) TestUpdatePartitions() {
	const tableName = "outbox_messages_partitions_test"

	// 1. create repository with default options
	repo := psql.NewPostgresMessageRepository(
		s.db,
		lockID,
		psql.WithTableName(tableName),
		psql.WithFuturePartitions(2),
		psql.WithPastPartitions(2),
	)

	// 2. create the table with partitions
	err := repo.Migrate(s.T().Context())
	s.Require().NoError(err, "failed to migrate database")

	// 3. check if the tables are created
	now := time.Now().UTC().Truncate(24 * time.Hour)
	expPartitions := []string{
		repo.GetPartitionName(now.Add(24 * time.Hour)),
		repo.GetPartitionName(now),
	}
	partitions := s.getPartitions(s.T().Context(), tableName)
	s.Require().Equal(expPartitions, partitions)

	// 4. create outdated partitions
	endTime := now
	for i := 0; i < 5; i++ {
		startTime := endTime.Add(-24 * time.Hour)
		expPartitions = append(expPartitions, repo.GetPartitionName(startTime))
		err = repo.CreatePartition(s.T().Context(), startTime, endTime)
		s.Require().NoError(err, "failed to create partition")
		endTime = startTime
	}
	partitions = s.getPartitions(s.T().Context(), tableName)
	s.Require().Equal(expPartitions, partitions)

	// 5. clean up outdated partitions
	err = repo.Migrate(s.T().Context())
	s.Require().NoError(err, "failed to migrate database")

	// 6. check if the tables are removed
	expPartitions = []string{
		repo.GetPartitionName(now.Add(24 * time.Hour)),
		repo.GetPartitionName(now),
		repo.GetPartitionName(now.Add(-24 * time.Hour)),
		repo.GetPartitionName(now.Add(-48 * time.Hour)),
	}
	partitions = s.getPartitions(s.T().Context(), tableName)
	s.Require().Equal(expPartitions, partitions)
}

func (s *OutboxPsqlTestSuite) TestZeroOutdatedPartitions() {
	const tableName = "outbox_messages_partitions_test"

	// 1. create repository with default options
	repo := psql.NewPostgresMessageRepository(
		s.db,
		lockID,
		psql.WithTableName(tableName),
		psql.WithFuturePartitions(2),
		psql.WithPastPartitions(0),
	)

	// 2. create the table with partitions
	err := repo.Migrate(s.T().Context())
	s.Require().NoError(err, "failed to migrate database")

	// 3. check if the tables are created
	now := time.Now().UTC().Truncate(24 * time.Hour)
	expPartitions := []string{
		repo.GetPartitionName(now.Add(24 * time.Hour)),
		repo.GetPartitionName(now),
	}
	partitions := s.getPartitions(s.T().Context(), tableName)
	s.Require().Equal(expPartitions, partitions)

	// 4. create outdated partitions
	endTime := now
	for i := 0; i < 5; i++ {
		startTime := endTime.Add(-24 * time.Hour)
		expPartitions = append(expPartitions, repo.GetPartitionName(startTime))
		err = repo.CreatePartition(s.T().Context(), startTime, endTime)
		s.Require().NoError(err, "failed to create partition")
		endTime = startTime
	}
	partitions = s.getPartitions(s.T().Context(), tableName)
	s.Require().Equal(expPartitions, partitions)

	// 5. clean up outdated partitions
	err = repo.Migrate(s.T().Context())
	s.Require().NoError(err, "failed to migrate database")

	// 6. check if the tables are removed
	expPartitions = []string{
		repo.GetPartitionName(now.Add(24 * time.Hour)),
		repo.GetPartitionName(now),
	}
	partitions = s.getPartitions(s.T().Context(), tableName)
	s.Require().Equal(expPartitions, partitions)
}
