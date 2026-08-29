package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

// WorkerConfig controls polling, bounded retries, and per-event deadlines.
type WorkerConfig struct {
	Topic          string
	PollInterval   time.Duration
	PublishTimeout time.Duration
	RetryMin       time.Duration
	RetryMax       time.Duration
}

// Worker continuously moves durable outbox events to a broker.
type Worker struct {
	store     Store
	publisher Publisher
	config    WorkerConfig
	logger    *slog.Logger
	sleep     func(context.Context, time.Duration) bool
}

// NewWorker validates dependencies and constructs an outbox worker.
func NewWorker(store Store, publisher Publisher, config WorkerConfig, logger *slog.Logger) (*Worker, error) {
	if store == nil {
		return nil, fmt.Errorf("outbox store is required")
	}
	if publisher == nil {
		return nil, fmt.Errorf("outbox publisher is required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("outbox topic is required")
	}
	if config.PollInterval <= 0 || config.PublishTimeout <= 0 || config.RetryMin <= 0 || config.RetryMax < config.RetryMin {
		return nil, fmt.Errorf("outbox timing configuration is invalid")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		store:     store,
		publisher: publisher,
		config:    config,
		logger:    logger,
		sleep:     sleepContext,
	}, nil
}

// Run publishes until cancellation. Cancellation aborts an in-flight publish,
// causing PostgreSQL to roll back so the event remains available after restart.
func (w *Worker) Run(ctx context.Context) error {
	retryDelay := w.config.RetryMin
	for {
		if ctx.Err() != nil {
			return nil
		}

		attemptCtx, cancel := context.WithTimeout(ctx, w.config.PublishTimeout)
		processed, err := w.store.ProcessNext(attemptCtx, w.publish)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.ErrorContext(ctx, "outbox publish attempt failed",
				slog.String("error", err.Error()),
				slog.Duration("retry_in", retryDelay),
			)
			if !w.sleep(ctx, retryDelay) {
				return nil
			}
			retryDelay = nextBackoff(retryDelay, w.config.RetryMax)
			continue
		}

		retryDelay = w.config.RetryMin
		if !processed && !w.sleep(ctx, w.config.PollInterval) {
			return nil
		}
	}
}

func (w *Worker) publish(ctx context.Context, event Event) error {
	value, err := marshalEnvelope(event)
	if err != nil {
		return err
	}

	message := Message{
		Topic: w.config.Topic,
		Key:   event.AggregateID,
		Value: value,
		Headers: []Header{
			{Key: "event_id", Value: event.ID},
			{Key: "event_type", Value: event.EventType},
			{Key: "schema_version", Value: strconv.Itoa(event.SchemaVersion)},
			{Key: "trace_id", Value: event.TraceID},
		},
	}
	if err := w.publisher.Publish(ctx, message); err != nil {
		return err
	}

	w.logger.InfoContext(ctx, "published outbox event",
		slog.String("event_id", event.ID),
		slog.String("event_type", event.EventType),
		slog.String("aggregate_id", event.AggregateID),
	)
	return nil
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
