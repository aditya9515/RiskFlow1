package observability

import (
	"testing"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/decision"
	"github.com/prometheus/client_golang/prometheus"
)

func TestNormalizedDispositionUsesFixedLabelSet(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"APPLIED":   "applied",
		"REPLAYED":  "replayed",
		"REJECTED":  "rejected",
		"arbitrary": "unknown",
	}
	for input, want := range tests {
		if got := normalizedDisposition(input); got != want {
			t.Fatalf("normalizedDisposition(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestDecisionMetricsRecordsDispositionRetryAndLag(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics := NewDecisionMetrics(registry)
	record := decision.SourceRecord{Topic: "risk.decisions", Partition: 2, Lag: 9}
	metrics.ObserveRecord("APPLIED", 20*time.Millisecond, 100*time.Millisecond, true, record)
	metrics.IncrementRetry("persistence")

	assertMetricValue(t, registry, "riskflow_decision_records_total", map[string]string{"disposition": "applied"}, 1)
	assertMetricValue(t, registry, "riskflow_decision_retries_total", map[string]string{"stage": "persistence"}, 1)
	assertMetricValue(t, registry, "riskflow_kafka_consumer_lag_records", map[string]string{"topic": "risk.decisions", "partition": "2"}, 9)
}
