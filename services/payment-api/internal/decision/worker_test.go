package decision

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestWorkerCommitsOnlyAfterDatabaseApply(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	order := make([]string, 0, 2)
	consumer := &fakeConsumer{record: validSourceRecord(), commit: func() error {
		order = append(order, "commit")
		cancel()
		return nil
	}}
	store := &fakeStore{apply: func() (ApplyResult, error) {
		order = append(order, "apply")
		return ApplyResult{Applied: true}, nil
	}}
	worker := newTestWorker(t, consumer, store)
	metrics := &capturingDecisionMetrics{}
	worker.metrics = metrics

	if err := worker.Run(ctx); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if len(order) != 2 || order[0] != "apply" || order[1] != "commit" {
		t.Fatalf("operation order = %v", order)
	}
	if consumer.allowCalls != 1 || consumer.closeCalls != 1 {
		t.Fatalf("allow/close calls = %d/%d", consumer.allowCalls, consumer.closeCalls)
	}
	if len(metrics.records) != 1 || metrics.records[0].disposition != dispositionApplied || metrics.records[0].record.Lag != 7 {
		t.Fatalf("record metrics = %+v", metrics.records)
	}
}

func TestWorkerRetainsOffsetWhenDatabaseFails(t *testing.T) {
	t.Parallel()

	consumer := &fakeConsumer{record: validSourceRecord()}
	store := &fakeStore{apply: func() (ApplyResult, error) {
		return ApplyResult{}, errors.New("database unavailable")
	}}
	worker := newTestWorker(t, consumer, store)
	metrics := &capturingDecisionMetrics{}
	worker.metrics = metrics
	worker.wait = func(context.Context, time.Duration) bool { return false }

	if err := worker.Run(context.Background()); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if consumer.commitCalls != 0 {
		t.Fatalf("commit calls = %d, want 0", consumer.commitCalls)
	}
	if store.applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", store.applyCalls)
	}
	if len(metrics.retries) != 1 || metrics.retries[0] != "persistence" {
		t.Fatalf("retry metrics = %v", metrics.retries)
	}
}

func TestWorkerPersistsInvalidRecordBeforeCommit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &fakeConsumer{record: SourceRecord{Topic: "risk.decisions", Partition: 0, Offset: 1, Value: []byte(`{"bad":true}`)}, commit: func() error {
		cancel()
		return nil
	}}
	store := &fakeStore{}
	worker := newTestWorker(t, consumer, store)

	if err := worker.Run(ctx); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if store.applyCalls != 0 || store.rejectCalls != 1 || store.rejectionCode != "invalid_event" {
		t.Fatalf("apply/reject/code = %d/%d/%q", store.applyCalls, store.rejectCalls, store.rejectionCode)
	}
	if consumer.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", consumer.commitCalls)
	}
}

func TestWorkerQuarantinesPermanentStateConflict(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &fakeConsumer{record: validSourceRecord(), commit: func() error {
		cancel()
		return nil
	}}
	store := &fakeStore{apply: func() (ApplyResult, error) {
		return ApplyResult{}, ErrPaymentNotFound
	}}
	worker := newTestWorker(t, consumer, store)

	if err := worker.Run(ctx); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if store.rejectCalls != 1 || store.rejectionCode != "payment_not_found" || consumer.commitCalls != 1 {
		t.Fatalf("reject/code/commit = %d/%q/%d", store.rejectCalls, store.rejectionCode, consumer.commitCalls)
	}
}

func TestWorkerReturnsCommitFailureForProcessRestart(t *testing.T) {
	t.Parallel()

	commitFailure := errors.New("coordinator unavailable")
	consumer := &fakeConsumer{record: validSourceRecord(), commit: func() error {
		return commitFailure
	}}
	store := &fakeStore{apply: func() (ApplyResult, error) {
		return ApplyResult{Applied: true}, nil
	}}
	worker := newTestWorker(t, consumer, store)

	err := worker.Run(context.Background())
	if !errors.Is(err, commitFailure) {
		t.Fatalf("error = %v, want commit failure", err)
	}
	if consumer.allowCalls != 1 {
		t.Fatalf("allow calls = %d, want 1", consumer.allowCalls)
	}
}

func newTestWorker(t *testing.T, consumer Consumer, store Store) *Worker {
	t.Helper()
	worker, err := NewWorker(consumer, store, WorkerConfig{
		ProcessTimeout: time.Second,
		RetryBackoff:   time.Millisecond,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	return worker
}

func validSourceRecord() SourceRecord {
	return SourceRecord{Topic: "risk.decisions", Partition: 1, Offset: 42, Lag: 7, Value: validEventValue()}
}

type observedDecisionRecord struct {
	disposition string
	record      SourceRecord
}

type capturingDecisionMetrics struct {
	retries []string
	records []observedDecisionRecord
}

func (m *capturingDecisionMetrics) IncrementRetry(stage string) {
	m.retries = append(m.retries, stage)
}

func (m *capturingDecisionMetrics) ObserveRecord(disposition string, _ time.Duration, _ time.Duration, _ bool, record SourceRecord) {
	m.records = append(m.records, observedDecisionRecord{disposition: disposition, record: record})
}

type fakeConsumer struct {
	record      SourceRecord
	polled      bool
	commit      func() error
	commitCalls int
	allowCalls  int
	closeCalls  int
}

func (c *fakeConsumer) Poll(ctx context.Context) (SourceRecord, error) {
	if !c.polled {
		c.polled = true
		return c.record, nil
	}
	<-ctx.Done()
	return SourceRecord{}, ctx.Err()
}

func (c *fakeConsumer) Commit(context.Context, SourceRecord) error {
	c.commitCalls++
	if c.commit != nil {
		return c.commit()
	}
	return nil
}

func (c *fakeConsumer) AllowRebalance() { c.allowCalls++ }
func (c *fakeConsumer) Close()          { c.closeCalls++ }

type fakeStore struct {
	apply         func() (ApplyResult, error)
	applyCalls    int
	rejectCalls   int
	rejectionCode string
	rejectErr     error
}

func (s *fakeStore) Apply(context.Context, Event, SourceRecord) (ApplyResult, error) {
	s.applyCalls++
	if s.apply != nil {
		return s.apply()
	}
	return ApplyResult{}, nil
}

func (s *fakeStore) Reject(_ context.Context, _ SourceRecord, code string, _ error) error {
	s.rejectCalls++
	s.rejectionCode = code
	return s.rejectErr
}
