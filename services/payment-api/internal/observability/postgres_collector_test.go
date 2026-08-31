package observability

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type fakeOperationalSource struct {
	load func(context.Context) (operationalSnapshot, error)
}

func (s fakeOperationalSource) Load(ctx context.Context) (operationalSnapshot, error) {
	return s.load(ctx)
}

func TestPostgresCollectorReportsBoundedOperationalState(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	registry.MustRegister(newPostgresCollector(fakeOperationalSource{load: func(context.Context) (operationalSnapshot, error) {
		return operationalSnapshot{
			outboxPending: 2, outboxRetrying: 1, outboxDeadLettered: 3,
			decisionEventsRejected: 4, manualReviewsPending: 5,
		}, nil
	}}, time.Second, discardLogger()))

	assertMetricValue(t, registry, "riskflow_postgres_metrics_collection_success", nil, 1)
	assertMetricValue(t, registry, "riskflow_outbox_events", map[string]string{"state": "pending"}, 2)
	assertMetricValue(t, registry, "riskflow_outbox_events", map[string]string{"state": "retrying"}, 1)
	assertMetricValue(t, registry, "riskflow_outbox_events", map[string]string{"state": "dead_lettered"}, 3)
	assertMetricValue(t, registry, "riskflow_decision_events_rejected", nil, 4)
	assertMetricValue(t, registry, "riskflow_manual_reviews_pending", nil, 5)
}

func TestPostgresCollectorExposesFailureWithoutStaleQueueValues(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	registry.MustRegister(newPostgresCollector(fakeOperationalSource{load: func(context.Context) (operationalSnapshot, error) {
		return operationalSnapshot{}, errors.New("database unavailable")
	}}, time.Second, discardLogger()))

	assertMetricValue(t, registry, "riskflow_postgres_metrics_collection_success", nil, 0)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "riskflow_outbox_events" {
			t.Fatal("failed collection exported stale outbox values")
		}
	}
}

func TestPostgresCollectorBoundsCollectionTime(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	registry.MustRegister(newPostgresCollector(fakeOperationalSource{load: func(ctx context.Context) (operationalSnapshot, error) {
		<-ctx.Done()
		return operationalSnapshot{}, ctx.Err()
	}}, 20*time.Millisecond, discardLogger()))

	startedAt := time.Now()
	assertMetricValue(t, registry, "riskflow_postgres_metrics_collection_success", nil, 0)
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("collection took %s, want under 250ms", elapsed)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}
