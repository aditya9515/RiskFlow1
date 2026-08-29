package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"
)

type capturingPublisher struct {
	message Message
	err     error
	calls   int
}

func (p *capturingPublisher) Publish(_ context.Context, message Message) error {
	p.calls++
	p.message = message
	return p.err
}

type scriptedStore struct {
	mu    sync.Mutex
	steps []storeStep
	calls int
}

type storeStep struct {
	event *Event
	err   error
}

func (s *scriptedStore) ProcessNext(ctx context.Context, publish func(context.Context, Event) error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.steps) == 0 {
		return false, nil
	}
	step := s.steps[0]
	s.steps = s.steps[1:]
	if step.err != nil {
		return false, step.err
	}
	if step.event == nil {
		return false, nil
	}
	return true, publish(ctx, *step.event)
}

func TestWorkerPublishesVersionedMessage(t *testing.T) {
	t.Parallel()

	publisher := &capturingPublisher{}
	worker, err := NewWorker(&scriptedStore{}, publisher, validWorkerConfig(), discardOutboxLogger())
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	event := validEvent()
	if err := worker.publish(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if publisher.calls != 1 || publisher.message.Topic != "payments.created" || publisher.message.Key != event.AggregateID {
		t.Fatalf("unexpected Kafka message: %+v", publisher.message)
	}

	var envelope Envelope
	if err := json.Unmarshal(publisher.message.Value, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.EventID != event.ID || envelope.SchemaVersion != event.SchemaVersion {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
	if len(publisher.message.Headers) != 4 || publisher.message.Headers[0].Value != event.ID {
		t.Fatalf("unexpected headers: %+v", publisher.message.Headers)
	}
}

func TestWorkerUsesBoundedBackoffAndStopsOnCancellation(t *testing.T) {
	t.Parallel()

	event := validEvent()
	store := &scriptedStore{steps: []storeStep{
		{err: errors.New("Kafka unavailable")},
		{err: errors.New("Kafka unavailable")},
		{err: errors.New("Kafka unavailable")},
		{event: &event},
		{},
	}}
	publisher := &capturingPublisher{}
	config := validWorkerConfig()
	config.RetryMin = 10 * time.Millisecond
	config.RetryMax = 20 * time.Millisecond
	worker, err := NewWorker(store, publisher, config, discardOutboxLogger())
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var delays []time.Duration
	worker.sleep = func(_ context.Context, delay time.Duration) bool {
		delays = append(delays, delay)
		if len(delays) == 4 {
			cancel()
			return false
		}
		return true
	}

	if err := worker.Run(ctx); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if !slices.Equal(delays, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond, config.PollInterval}) {
		t.Fatalf("retry delays = %v", delays)
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.calls)
	}
}

func TestNewWorkerRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	_, err := NewWorker(&scriptedStore{}, &capturingPublisher{}, WorkerConfig{}, discardOutboxLogger())
	if err == nil {
		t.Fatal("NewWorker returned nil error")
	}
}

func validWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Topic:          "payments.created",
		PollInterval:   5 * time.Millisecond,
		PublishTimeout: time.Second,
		RetryMin:       10 * time.Millisecond,
		RetryMax:       100 * time.Millisecond,
	}
}

func discardOutboxLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
