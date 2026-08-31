package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestOutboxMetricsRecordsPublishAndRetry(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics := NewOutboxMetrics(registry)
	metrics.ObservePublish("success", 25*time.Millisecond)
	metrics.IncrementRetry("delivery")

	assertMetricValue(t, registry, "riskflow_outbox_publish_attempts_total", map[string]string{"result": "success"}, 1)
	assertMetricValue(t, registry, "riskflow_outbox_retries_total", map[string]string{"stage": "delivery"}, 1)
}
