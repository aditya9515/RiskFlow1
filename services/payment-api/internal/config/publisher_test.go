package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoadPublisherValidDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := loadPublisher(mapLookup(map[string]string{
		"DATABASE_URL":  "postgres://localhost/riskflow",
		"KAFKA_BROKERS": "kafka-1:9092, kafka-2:9092",
	}))
	if err != nil {
		t.Fatalf("load publisher config: %v", err)
	}

	if !slices.Equal(cfg.KafkaBrokers, []string{"kafka-1:9092", "kafka-2:9092"}) {
		t.Fatalf("KafkaBrokers = %v", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "payments.created" {
		t.Fatalf("KafkaTopic = %q", cfg.KafkaTopic)
	}
	if cfg.PollInterval != 250*time.Millisecond || cfg.PublishTimeout != 5*time.Second {
		t.Fatalf("poll/publish timeout = %s/%s", cfg.PollInterval, cfg.PublishTimeout)
	}
	if cfg.RetryMin != 100*time.Millisecond || cfg.RetryMax != 5*time.Second {
		t.Fatalf("retry min/max = %s/%s", cfg.RetryMin, cfg.RetryMax)
	}
	if cfg.MetricsAddr != ":9091" || cfg.MetricsShutdownTimeout != 5*time.Second {
		t.Fatalf("metrics addr/shutdown = %q/%s", cfg.MetricsAddr, cfg.MetricsShutdownTimeout)
	}
}

func TestLoadPublisherRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string]string
		wantError string
	}{
		{name: "missing database", overrides: map[string]string{"DATABASE_URL": ""}, wantError: "DATABASE_URL"},
		{name: "missing brokers", overrides: map[string]string{"KAFKA_BROKERS": ""}, wantError: "KAFKA_BROKERS"},
		{name: "invalid broker", overrides: map[string]string{"KAFKA_BROKERS": "kafka"}, wantError: "host:port"},
		{name: "invalid broker port", overrides: map[string]string{"KAFKA_BROKERS": "kafka:70000"}, wantError: "ports"},
		{name: "invalid topic", overrides: map[string]string{"KAFKA_TOPIC": "payments created"}, wantError: "KAFKA_TOPIC"},
		{name: "invalid poll", overrides: map[string]string{"OUTBOX_POLL_INTERVAL": "0s"}, wantError: "OUTBOX_POLL_INTERVAL"},
		{name: "invalid timeout", overrides: map[string]string{"OUTBOX_PUBLISH_TIMEOUT": "soon"}, wantError: "OUTBOX_PUBLISH_TIMEOUT"},
		{name: "invalid metrics address", overrides: map[string]string{"OUTBOX_METRICS_ADDR": "9091"}, wantError: "OUTBOX_METRICS_ADDR"},
		{name: "invalid metrics shutdown", overrides: map[string]string{"METRICS_SHUTDOWN_TIMEOUT": "0s"}, wantError: "METRICS_SHUTDOWN_TIMEOUT"},
		{name: "reversed retry bounds", overrides: map[string]string{
			"OUTBOX_RETRY_MIN_BACKOFF": "2s",
			"OUTBOX_RETRY_MAX_BACKOFF": "1s",
		}, wantError: "OUTBOX_RETRY_MAX_BACKOFF"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			values := map[string]string{
				"DATABASE_URL":  "postgres://localhost/riskflow",
				"KAFKA_BROKERS": "kafka:9092",
			}
			for key, value := range tt.overrides {
				values[key] = value
			}

			_, err := loadPublisher(mapLookup(values))
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want text %q", err, tt.wantError)
			}
		})
	}
}
