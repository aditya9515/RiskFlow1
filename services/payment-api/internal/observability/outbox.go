package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// OutboxMetrics implements the outbox worker's narrow metrics interface.
type OutboxMetrics struct {
	attempts *prometheus.CounterVec
	duration *prometheus.HistogramVec
	retries  *prometheus.CounterVec
}

func NewOutboxMetrics(registerer prometheus.Registerer) *OutboxMetrics {
	metrics := &OutboxMetrics{
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "riskflow",
			Subsystem: "outbox",
			Name:      "publish_attempts_total",
			Help:      "Outbox publish attempts by bounded result.",
		}, []string{"result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "riskflow",
			Subsystem: "outbox",
			Name:      "publish_duration_seconds",
			Help:      "Outbox publish duration by bounded result.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"result"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "riskflow",
			Subsystem: "outbox",
			Name:      "retries_total",
			Help:      "Retryable outbox processing failures by bounded stage.",
		}, []string{"stage"}),
	}
	registerer.MustRegister(metrics.attempts, metrics.duration, metrics.retries)
	return metrics
}

func (m *OutboxMetrics) ObservePublish(result string, duration time.Duration) {
	m.attempts.WithLabelValues(result).Inc()
	m.duration.WithLabelValues(result).Observe(duration.Seconds())
}

func (m *OutboxMetrics) IncrementRetry(stage string) {
	m.retries.WithLabelValues(stage).Inc()
}
