package troutbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/Darkemon/troutbox"
	"github.com/Darkemon/troutbox/internal/mocks"
)

const (
	maxRetries = uint(3)
	batchSize  = uint(10)
)

type OutboxTestSuite struct {
	suite.Suite
	mockTrRepo *mocks.MockTransactionalMessageRepository[struct{}]
	mockRepo   *mocks.MockMessageRepository
	mockSender *mocks.MockSender
	outbox     *troutbox.Outbox[struct{}]
}

// Run the test suite.
func TestOutboxTestSuite(t *testing.T) {
	suite.Run(t, new(OutboxTestSuite))
}

func (ots *OutboxTestSuite) SetupTest() {
	ots.mockTrRepo = mocks.NewMockTransactionalMessageRepository[struct{}](ots.T())
	ots.mockRepo = mocks.NewMockMessageRepository(ots.T())
	ots.mockSender = mocks.NewMockSender(ots.T())

	var err error
	ots.outbox, err = troutbox.NewOutbox(
		ots.mockTrRepo,
		ots.mockSender,
		troutbox.WithMaxRetries(maxRetries),
		troutbox.WithBatchSize(batchSize),
	)
	ots.Require().NoError(err)
}

// TestBasicFunctionality tests that all messages are sent successfully.
func (ots *OutboxTestSuite) TestBasicFunctionality() {
	messages := []troutbox.Message{
		{ID: 1, Key: "key1", Value: []byte("value1"), Retries: 0},
		{ID: 2, Key: "key2", Value: []byte("value2"), Retries: 0},
	}

	sentMessages := make([]troutbox.Message, 0, len(messages))
	for _, msg := range messages {
		msg.MarkAsSent()
		sentMessages = append(sentMessages, msg)
	}

	ots.mockRepo.EXPECT().FetchAndLock(mock.Anything, batchSize).Return(messages, nil)
	ots.mockSender.EXPECT().Send(mock.Anything, messages).Return(sentMessages, nil)
	ots.mockRepo.EXPECT().MarkAsSent(mock.Anything, []uint64{1, 2}).Return(nil)

	err := ots.outbox.SendBatch(context.Background(), ots.mockRepo)

	assert.NoError(ots.T(), err)
}

func (ots *OutboxTestSuite) TestAddMessage() {
	expMessages := []troutbox.Message{
		{Key: "key1", Value: []byte("value1"), Timestamp: time.Now().UTC()},
		{Key: "key2", Value: []byte("value2"), Timestamp: time.Now().UTC()},
	}
	i := 0

	ots.mockTrRepo.EXPECT().
		AddMessage(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ struct{}, msg troutbox.Message) error {
			ots.Assert().Equal(msg.Key, expMessages[i].Key)
			ots.Assert().Equal(msg.Value, expMessages[i].Value)
			ots.Assert().WithinDuration(expMessages[i].Timestamp, msg.Timestamp, time.Millisecond)
			i++
			return nil
		}).Times(2)

	err := ots.outbox.AddMessage(context.Background(), struct{}{}, "key1", []byte("value1"))
	ots.Assert().NoError(err)
	err = ots.outbox.AddMessage(context.Background(), struct{}{}, "key2", []byte("value2"))
	ots.Assert().NoError(err)
}

// TestEmptyBatch tests that no messages are sent when the batch is empty.
func (ots *OutboxTestSuite) TestEmptyBatch() {
	ots.mockRepo.EXPECT().FetchAndLock(mock.Anything, batchSize).Return([]troutbox.Message{}, nil)

	err := ots.outbox.SendBatch(context.Background(), ots.mockRepo)
	assert.NoError(ots.T(), err)
}

// TestRetryLogic tests that failed messages are retried or marked as dead.
func (ots *OutboxTestSuite) TestRetryLogic() {
	messages := []troutbox.Message{
		{ID: 1, Key: "key1", Value: []byte("value1"), Retries: 1},
		{ID: 2, Key: "key2", Value: []byte("value2"), Retries: 2},
	}

	sentMessages := make([]troutbox.Message, 0, len(messages))
	for _, msg := range messages {
		msg.MarkAsFailed()
		sentMessages = append(sentMessages, msg)
	}

	ots.mockRepo.EXPECT().FetchAndLock(mock.Anything, batchSize).Return(messages, nil)
	ots.mockSender.EXPECT().Send(mock.Anything, messages).Return(sentMessages, errors.New("send failed"))
	ots.mockRepo.EXPECT().MarkAsDead(mock.Anything, []uint64{2}).Return(nil)
	ots.mockRepo.EXPECT().UpdateRetryCount(mock.Anything, []uint64{1}).Return(nil)

	err := ots.outbox.SendBatch(context.Background(), ots.mockRepo)
	assert.NoError(ots.T(), err)
}

// TestPartialRetryLogic tests that some messages are sent successfully while others fail.
func (ots *OutboxTestSuite) TestPartialRetryLogic() {
	messages := []troutbox.Message{
		{ID: 1, Key: "key1", Value: []byte("value1"), Retries: 1}, // will be sent successfully
		{ID: 2, Key: "key2", Value: []byte("value2"), Retries: 2}, // will fail and exceed maxRetries
		{ID: 3, Key: "key3", Value: []byte("value3"), Retries: 1}, // will fail but can retry
	}

	updatedMessages := make([]troutbox.Message, len(messages))
	copy(updatedMessages, messages)

	updatedMessages[0].MarkAsSent()
	updatedMessages[1].MarkAsFailed()
	updatedMessages[2].MarkAsFailed()

	ots.mockRepo.EXPECT().FetchAndLock(mock.Anything, batchSize).Return(messages, nil)
	ots.mockSender.EXPECT().Send(mock.Anything, messages).Return(updatedMessages, errors.New("partial failure"))
	ots.mockRepo.EXPECT().MarkAsSent(mock.Anything, []uint64{1}).Return(nil)
	ots.mockRepo.EXPECT().MarkAsDead(mock.Anything, []uint64{2}).Return(nil)
	ots.mockRepo.EXPECT().UpdateRetryCount(mock.Anything, []uint64{3}).Return(nil)

	err := ots.outbox.SendBatch(context.Background(), ots.mockRepo)
	assert.NoError(ots.T(), err)
}

// TestMarkAsDead tests that messages that exceed the max retries are marked as dead.
func (ots *OutboxTestSuite) TestMarkAsDead() {
	messages := []troutbox.Message{
		{ID: 1, Key: "key1", Value: []byte("value1"), Retries: 2},
	}

	sentMessages := make([]troutbox.Message, 0, len(messages))
	for _, msg := range messages {
		msg.MarkAsFailed()
		sentMessages = append(sentMessages, msg)
	}

	ots.mockRepo.EXPECT().FetchAndLock(mock.Anything, batchSize).Return(messages, nil)
	ots.mockSender.EXPECT().Send(mock.Anything, messages).Return(sentMessages, errors.New("send failed"))
	ots.mockRepo.EXPECT().MarkAsDead(mock.Anything, []uint64{1}).Return(nil)

	err := ots.outbox.SendBatch(context.Background(), ots.mockRepo)
	assert.NoError(ots.T(), err)
}

// TestContextCancellation tests that the outbox stops when the context is canceled.
func (ots *OutboxTestSuite) TestContextCancellation() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel the context immediately

	err := ots.outbox.Run(ctx)
	assert.ErrorIs(ots.T(), err, context.Canceled)
}
