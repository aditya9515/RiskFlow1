package observability

import (
	"strconv"
	"strings"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/decision"
	"github.com/prometheus/client_golang/prometheus"
)

// DecisionMetrics implements the decision worker's narrow metrics interface.
type DecisionMetrics struct {
	records    *prometheus.CounterVec
	processing *prometheus.HistogramVec
	endToEnd   *prometheus.HistogramVec
	retries    *prometheus.CounterVec
	lag        *prometheus.GaugeVec
}

func NewDecisionMetrics(registerer prometheus.Registerer) *DecisionMetrics {
	metrics := &DecisionMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "riskflow",
			Subsystem: "decision",
			Name:      "records_total",
			Help:      "Committed risk-decision Kafka records by bounded disposition.",
		}, []string{"disposition"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "riskflow",
			Subsystem: "decision",
			Name:      "processing_duration_seconds",
			Help:      "Time from polling a decision record through durable offset commit.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"disposition"}),
		endToEnd: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "riskflow",
			Subsystem: "decision",
			Name:      "end_to_end_latency_seconds",
			Help:      "Time from risk decision creation through durable persistence.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
		}, []string{"disposition"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "riskflow",
			Subsystem: "decision",
			Name:      "retries_total",
			Help:      "Risk-decision retries or restart-required failures by bounded stage.",
		}, []string{"stage"}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "riskflow",
			Subsystem: "kafka",
			Name:      "consumer_lag_records",
			Help:      "Approximate remaining records after the most recently polled record.",
		}, []string{"topic", "partition"}),
	}
	registerer.MustRegister(metrics.records, metrics.processing, metrics.endToEnd, metrics.retries, metrics.lag)
	return metrics
}

func (m *DecisionMetrics) IncrementRetry(stage string) {
	m.retries.WithLabelValues(stage).Inc()
}

func (m *DecisionMetrics) ObserveRecord(disposition string, processing time.Duration, endToEnd time.Duration, hasEndToEnd bool, record decision.SourceRecord) {
	disposition = normalizedDisposition(disposition)
	m.records.WithLabelValues(disposition).Inc()
	m.processing.WithLabelValues(disposition).Observe(processing.Seconds())
	if hasEndToEnd {
		m.endToEnd.WithLabelValues(disposition).Observe(endToEnd.Seconds())
	}
	m.lag.WithLabelValues(record.Topic, strconv.FormatInt(int64(record.Partition), 10)).Set(float64(record.Lag))
}

func normalizedDisposition(disposition string) string {
	switch strings.ToUpper(disposition) {
	case "APPLIED":
		return "applied"
	case "REPLAYED":
		return "replayed"
	case "REJECTED":
		return "rejected"
	default:
		return "unknown"
	}
}
