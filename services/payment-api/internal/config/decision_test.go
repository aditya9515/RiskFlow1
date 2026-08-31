package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDecisionConsumerDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := loadDecisionConsumer(mapLookup(map[string]string{
		"DATABASE_URL":  "postgres://localhost/riskflow",
		"KAFKA_BROKERS": "kafka:9092",
	}))
	if err != nil {
		t.Fatalf("load decision consumer: %v", err)
	}
	if cfg.Topic != "risk.decisions" || cfg.ConsumerGroup != "riskflow-decision-persistence-v1" {
		t.Fatalf("topic/group = %q/%q", cfg.Topic, cfg.ConsumerGroup)
	}
	if cfg.AutoOffsetReset != "earliest" {
		t.Fatalf("AutoOffsetReset = %q", cfg.AutoOffsetReset)
	}
	if cfg.ProcessTimeout != 5*time.Second || cfg.RetryBackoff != time.Second {
		t.Fatalf("process/retry = %s/%s", cfg.ProcessTimeout, cfg.RetryBackoff)
	}
	if cfg.MetricsAddr != ":9092" || cfg.MetricsShutdownTimeout != 5*time.Second {
		t.Fatalf("metrics addr/shutdown = %q/%s", cfg.MetricsAddr, cfg.MetricsShutdownTimeout)
	}
}

func TestLoadDecisionConsumerRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string]string
		want      string
	}{
		{name: "database", overrides: map[string]string{"DATABASE_URL": ""}, want: "DATABASE_URL"},
		{name: "brokers", overrides: map[string]string{"KAFKA_BROKERS": ""}, want: "KAFKA_BROKERS"},
		{name: "topic", overrides: map[string]string{"RISK_DECISIONS_TOPIC": "risk decisions"}, want: "RISK_DECISIONS_TOPIC"},
		{name: "group", overrides: map[string]string{"DECISION_CONSUMER_GROUP": ".."}, want: "DECISION_CONSUMER_GROUP"},
		{name: "offset reset", overrides: map[string]string{"DECISION_AUTO_OFFSET_RESET": "middle"}, want: "DECISION_AUTO_OFFSET_RESET"},
		{name: "timeout", overrides: map[string]string{"DECISION_PROCESS_TIMEOUT": "0s"}, want: "DECISION_PROCESS_TIMEOUT"},
		{name: "retry", overrides: map[string]string{"DECISION_RETRY_BACKOFF": "later"}, want: "DECISION_RETRY_BACKOFF"},
		{name: "metrics address", overrides: map[string]string{"DECISION_METRICS_ADDR": "9092"}, want: "DECISION_METRICS_ADDR"},
		{name: "metrics shutdown", overrides: map[string]string{"METRICS_SHUTDOWN_TIMEOUT": "0s"}, want: "METRICS_SHUTDOWN_TIMEOUT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := map[string]string{
				"DATABASE_URL":  "postgres://localhost/riskflow",
				"KAFKA_BROKERS": "kafka:9092",
			}
			for key, value := range test.overrides {
				values[key] = value
			}
			_, err := loadDecisionConsumer(mapLookup(values))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadReconcilerDefaultsAndValidation(t *testing.T) {
	t.Parallel()

	cfg, err := loadReconciler(mapLookup(map[string]string{
		"DATABASE_URL": "postgres://localhost/riskflow",
	}))
	if err != nil {
		t.Fatalf("load reconciler: %v", err)
	}
	if cfg.GracePeriod != 30*time.Second || cfg.Timeout != 10*time.Second {
		t.Fatalf("grace/timeout = %s/%s", cfg.GracePeriod, cfg.Timeout)
	}

	_, err = loadReconciler(mapLookup(map[string]string{
		"DATABASE_URL":                "postgres://localhost/riskflow",
		"RECONCILIATION_GRACE_PERIOD": "-1s",
	}))
	if err == nil || !strings.Contains(err.Error(), "RECONCILIATION_GRACE_PERIOD") {
		t.Fatalf("error = %v, want grace-period validation", err)
	}
}
