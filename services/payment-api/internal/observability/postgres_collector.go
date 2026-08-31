package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type operationalSnapshot struct {
	outboxPending          int64
	outboxRetrying         int64
	outboxDeadLettered     int64
	decisionEventsRejected int64
	manualReviewsPending   int64
}

type operationalSource interface {
	Load(context.Context) (operationalSnapshot, error)
}

type postgresOperationalSource struct {
	pool *pgxpool.Pool
}

func (s postgresOperationalSource) Load(ctx context.Context) (operationalSnapshot, error) {
	var snapshot operationalSnapshot
	err := s.pool.QueryRow(ctx, operationalMetricsSQL).Scan(
		&snapshot.outboxPending,
		&snapshot.outboxRetrying,
		&snapshot.outboxDeadLettered,
		&snapshot.decisionEventsRejected,
		&snapshot.manualReviewsPending,
	)
	return snapshot, err
}

// PostgresCollector reports current operational queues without caching stale
// values. Every scrape is bounded by timeout and reports its own success.
type PostgresCollector struct {
	source         operationalSource
	timeout        time.Duration
	logger         *slog.Logger
	outbox         *prometheus.Desc
	rejected       *prometheus.Desc
	reviews        *prometheus.Desc
	collectSuccess *prometheus.Desc
	collectSeconds *prometheus.Desc
}

func NewPostgresCollector(pool *pgxpool.Pool, timeout time.Duration, logger *slog.Logger) *PostgresCollector {
	return newPostgresCollector(postgresOperationalSource{pool: pool}, timeout, logger)
}

func newPostgresCollector(source operationalSource, timeout time.Duration, logger *slog.Logger) *PostgresCollector {
	if logger == nil {
		logger = slog.Default()
	}
	return &PostgresCollector{
		source:  source,
		timeout: timeout,
		logger:  logger,
		outbox: prometheus.NewDesc(
			"riskflow_outbox_events",
			"Current outbox rows by bounded delivery state.",
			[]string{"state"}, nil,
		),
		rejected: prometheus.NewDesc(
			"riskflow_decision_events_rejected",
			"Current rejected risk-decision Kafka records.",
			nil, nil,
		),
		reviews: prometheus.NewDesc(
			"riskflow_manual_reviews_pending",
			"Current payments awaiting manual review.",
			nil, nil,
		),
		collectSuccess: prometheus.NewDesc(
			"riskflow_postgres_metrics_collection_success",
			"Whether the most recent PostgreSQL operational metrics collection succeeded.",
			nil, nil,
		),
		collectSeconds: prometheus.NewDesc(
			"riskflow_postgres_metrics_collection_duration_seconds",
			"Duration of the PostgreSQL operational metrics collection.",
			nil, nil,
		),
	}
}

func (c *PostgresCollector) Describe(channel chan<- *prometheus.Desc) {
	channel <- c.outbox
	channel <- c.rejected
	channel <- c.reviews
	channel <- c.collectSuccess
	channel <- c.collectSeconds
}

func (c *PostgresCollector) Collect(channel chan<- prometheus.Metric) {
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	snapshot, err := c.source.Load(ctx)
	channel <- prometheus.MustNewConstMetric(c.collectSeconds, prometheus.GaugeValue, time.Since(startedAt).Seconds())
	if err != nil {
		channel <- prometheus.MustNewConstMetric(c.collectSuccess, prometheus.GaugeValue, 0)
		c.logger.Warn("PostgreSQL metrics collection failed", slog.String("error", err.Error()))
		return
	}

	channel <- prometheus.MustNewConstMetric(c.collectSuccess, prometheus.GaugeValue, 1)
	channel <- prometheus.MustNewConstMetric(c.outbox, prometheus.GaugeValue, float64(snapshot.outboxPending), "pending")
	channel <- prometheus.MustNewConstMetric(c.outbox, prometheus.GaugeValue, float64(snapshot.outboxRetrying), "retrying")
	channel <- prometheus.MustNewConstMetric(c.outbox, prometheus.GaugeValue, float64(snapshot.outboxDeadLettered), "dead_lettered")
	channel <- prometheus.MustNewConstMetric(c.rejected, prometheus.GaugeValue, float64(snapshot.decisionEventsRejected))
	channel <- prometheus.MustNewConstMetric(c.reviews, prometheus.GaugeValue, float64(snapshot.manualReviewsPending))
}

const operationalMetricsSQL = `
SELECT
    (SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL AND delivery_attempts = 0),
    (SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL AND delivery_attempts > 0),
    (SELECT count(*) FROM outbox_events WHERE dead_lettered_at IS NOT NULL),
    (SELECT count(*) FROM decision_ingestion_records WHERE disposition = 'REJECTED'),
    (SELECT count(*) FROM manual_review_queue WHERE status = 'PENDING')`
