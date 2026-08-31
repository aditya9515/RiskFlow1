package decision

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Consumer exposes the small broker surface the worker needs and makes commit
// ordering directly testable.
type Consumer interface {
	Poll(context.Context) (SourceRecord, error)
	Commit(context.Context, SourceRecord) error
	AllowRebalance()
	Close()
}

// WorkerConfig bounds each database/commit attempt and retry interval.
type WorkerConfig struct {
	ProcessTimeout time.Duration
	RetryBackoff   time.Duration
}

// Metrics is the bounded observability surface used by the persistence loop.
type Metrics interface {
	IncrementRetry(string)
	ObserveRecord(string, time.Duration, time.Duration, bool, SourceRecord)
}

type noopMetrics struct{}

func (noopMetrics) IncrementRetry(string) {}
func (noopMetrics) ObserveRecord(string, time.Duration, time.Duration, bool, SourceRecord) {
}

// Worker consumes risk decisions one record at a time.
type Worker struct {
	consumer Consumer
	store    Store
	config   WorkerConfig
	logger   *slog.Logger
	wait     func(context.Context, time.Duration) bool
	metrics  Metrics
	now      func() time.Time
}

func NewWorker(consumer Consumer, store Store, config WorkerConfig, logger *slog.Logger, metricSets ...Metrics) (*Worker, error) {
	if consumer == nil {
		return nil, errors.New("decision consumer is required")
	}
	if store == nil {
		return nil, errors.New("decision store is required")
	}
	if config.ProcessTimeout <= 0 || config.RetryBackoff <= 0 {
		return nil, errors.New("decision worker timeouts must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}
	metrics := Metrics(noopMetrics{})
	if len(metricSets) > 0 && metricSets[0] != nil {
		metrics = metricSets[0]
	}
	return &Worker{
		consumer: consumer,
		store:    store,
		config:   config,
		logger:   logger,
		wait:     waitContext,
		metrics:  metrics,
		now:      time.Now,
	}, nil
}

// Run advances a Kafka offset only after Apply or Reject commits to PostgreSQL.
func (w *Worker) Run(ctx context.Context) error {
	defer w.consumer.Close()
	w.logger.InfoContext(ctx, "risk decision persistence worker started")
	defer w.logger.Info("risk decision persistence worker stopped")

	for {
		if ctx.Err() != nil {
			return nil
		}
		record, err := w.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.metrics.IncrementRetry("poll")
			w.logger.ErrorContext(ctx, "poll risk decision", slog.String("error", err.Error()))
			if !w.wait(ctx, w.config.RetryBackoff) {
				return nil
			}
			continue
		}

		startedAt := w.now()
		for {
			attemptCtx, cancel := context.WithTimeout(ctx, w.config.ProcessTimeout)
			result, handleErr := w.handle(attemptCtx, record)
			cancel()
			if handleErr != nil {
				if ctx.Err() != nil {
					w.consumer.AllowRebalance()
					return nil
				}
				w.metrics.IncrementRetry("persistence")
				w.logger.ErrorContext(ctx, "persist risk decision; offset retained for retry",
					slog.String("error", handleErr.Error()),
					slog.String("topic", record.Topic),
					slog.Int("partition", int(record.Partition)),
					slog.Int64("offset", record.Offset),
				)
				if !w.wait(ctx, w.config.RetryBackoff) {
					w.consumer.AllowRebalance()
					return nil
				}
				continue
			}

			commitCtx, commitCancel := context.WithTimeout(ctx, w.config.ProcessTimeout)
			commitErr := w.consumer.Commit(commitCtx, record)
			commitCancel()
			w.consumer.AllowRebalance()
			if commitErr != nil {
				// Stop so a fresh group session resumes from Kafka's last durable
				// offset. PostgreSQL idempotency makes the redelivery harmless.
				w.metrics.IncrementRetry("offset_commit")
				return fmt.Errorf("commit risk decision Kafka offset: %w", commitErr)
			}

			completedAt := w.now()
			processing := completedAt.Sub(startedAt)
			if processing < 0 {
				processing = 0
			}
			endToEnd := time.Duration(0)
			hasEndToEnd := !result.decisionAt.IsZero()
			if hasEndToEnd {
				endToEnd = completedAt.Sub(result.decisionAt)
				if endToEnd < 0 {
					endToEnd = 0
				}
			}
			w.metrics.ObserveRecord(result.disposition, processing, endToEnd, hasEndToEnd, record)

			w.logger.InfoContext(ctx, "risk decision record persisted",
				slog.String("disposition", result.disposition),
				slog.String("event_id", result.eventID),
				slog.String("payment_id", result.paymentID),
				slog.String("trace_id", result.traceID),
				slog.String("topic", record.Topic),
				slog.Int("partition", int(record.Partition)),
				slog.Int64("offset", record.Offset),
			)
			break
		}
	}
}

type handleResult struct {
	disposition string
	eventID     string
	paymentID   string
	traceID     string
	decisionAt  time.Time
}

func (w *Worker) handle(ctx context.Context, record SourceRecord) (handleResult, error) {
	event, err := ParseEvent(record.Value)
	if err != nil {
		if rejectErr := w.store.Reject(ctx, record, "invalid_event", err); rejectErr != nil {
			return handleResult{}, rejectErr
		}
		return handleResult{disposition: dispositionRejected}, nil
	}

	result, err := w.store.Apply(ctx, event, record)
	if err != nil {
		code, permanent := applicationRejectionCode(err)
		if !permanent {
			return handleResult{}, err
		}
		if rejectErr := w.store.Reject(ctx, record, code, err); rejectErr != nil {
			return handleResult{}, rejectErr
		}
		return handleResult{
			disposition: dispositionRejected,
			eventID:     event.EventID,
			paymentID:   event.Payload.PaymentID,
			traceID:     event.TraceID,
			decisionAt:  event.Payload.DecisionAt,
		}, nil
	}

	disposition := dispositionApplied
	if result.Replayed {
		disposition = dispositionReplayed
	}
	return handleResult{
		disposition: disposition,
		eventID:     event.EventID,
		paymentID:   event.Payload.PaymentID,
		traceID:     event.TraceID,
		decisionAt:  event.Payload.DecisionAt,
	}, nil
}

func applicationRejectionCode(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrPaymentNotFound):
		return "payment_not_found", true
	case errors.Is(err, ErrDecisionConflict):
		return "decision_conflict", true
	case errors.Is(err, ErrPaymentStateConflict):
		return "payment_state_conflict", true
	default:
		return "", false
	}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
