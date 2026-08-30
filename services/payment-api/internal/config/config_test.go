package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadValidDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := load(mapLookup(map[string]string{
		"DATABASE_URL":                 "postgres://localhost/riskflow",
		"REVIEW_AUTH_CREDENTIALS_JSON": validReviewCredentialsJSON,
	}))
	if err != nil {
		t.Fatalf("load valid config: %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.ReadinessTimeout != time.Second {
		t.Fatalf("ReadinessTimeout = %s, want 1s", cfg.ReadinessTimeout)
	}
	if cfg.PaymentTimeout != 3*time.Second {
		t.Fatalf("PaymentTimeout = %s, want 3s", cfg.PaymentTimeout)
	}
	if cfg.ReviewTimeout != 3*time.Second {
		t.Fatalf("ReviewTimeout = %s, want 3s", cfg.ReviewTimeout)
	}
	if len(cfg.ReviewCredentials) != 2 {
		t.Fatalf("ReviewCredentials count = %d, want 2", len(cfg.ReviewCredentials))
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("ShutdownTimeout = %s, want 10s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Fatalf("LogLevel = %s, want INFO", cfg.LogLevel)
	}
}

func TestLoadRejectsInvalidReviewConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
	}{
		{name: "missing", raw: ""},
		{name: "invalid JSON", raw: "["},
		{name: "unknown field", raw: `[{"reviewer_id":"reviewer","role":"risk_reviewer","token":"12345678901234567890123456789012","extra":true}]`},
		{name: "unsupported role", raw: `[{"reviewer_id":"reviewer","role":"administrator","token":"12345678901234567890123456789012"}]`},
		{name: "short token", raw: `[{"reviewer_id":"reviewer","role":"risk_reviewer","token":"short"}]`},
		{name: "duplicate identity", raw: `[{"reviewer_id":"same","role":"risk_reviewer","token":"12345678901234567890123456789012"},{"reviewer_id":"same","role":"risk_auditor","token":"abcdefghijklmnopqrstuvwxyzABCDEF"}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := load(mapLookup(map[string]string{
				"DATABASE_URL":                 "postgres://localhost/riskflow",
				"REVIEW_AUTH_CREDENTIALS_JSON": tt.raw,
			}))
			if err == nil || !strings.Contains(err.Error(), "REVIEW_AUTH_CREDENTIALS_JSON") {
				t.Fatalf("error = %v, want review credential error", err)
			}
		})
	}
}

func TestLoadRejectsInvalidReviewTimeout(t *testing.T) {
	t.Parallel()

	_, err := load(mapLookup(map[string]string{
		"DATABASE_URL":                 "postgres://localhost/riskflow",
		"REVIEW_REQUEST_TIMEOUT":       "0s",
		"REVIEW_AUTH_CREDENTIALS_JSON": validReviewCredentialsJSON,
	}))
	if err == nil || !strings.Contains(err.Error(), "REVIEW_REQUEST_TIMEOUT") {
		t.Fatalf("error = %v, want invalid review timeout error", err)
	}
}

func TestLoadRejectsMissingDatabaseURL(t *testing.T) {
	t.Parallel()

	_, err := load(mapLookup(nil))
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("error = %v, want missing DATABASE_URL error", err)
	}
}

func TestLoadRejectsInvalidDurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "invalid readiness duration",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/riskflow",
				"READINESS_TIMEOUT": "quickly",
			},
		},
		{
			name: "zero readiness duration",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/riskflow",
				"READINESS_TIMEOUT": "0s",
			},
		},
		{
			name: "invalid payment timeout",
			env: map[string]string{
				"DATABASE_URL":           "postgres://localhost/riskflow",
				"PAYMENT_CREATE_TIMEOUT": "eventually",
			},
		},
		{
			name: "negative shutdown duration",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/riskflow",
				"SHUTDOWN_TIMEOUT": "-1s",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := load(mapLookup(tt.env)); err == nil {
				t.Fatal("load returned nil error")
			}
		})
	}
}

func TestLoadRejectsInvalidHTTPAddress(t *testing.T) {
	t.Parallel()

	_, err := load(mapLookup(map[string]string{
		"DATABASE_URL": "postgres://localhost/riskflow",
		"HTTP_ADDR":    "8080",
	}))
	if err == nil || !strings.Contains(err.Error(), "HTTP_ADDR") {
		t.Fatalf("error = %v, want invalid HTTP_ADDR error", err)
	}
}

func mapLookup(values map[string]string) lookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

const validReviewCredentialsJSON = `[{"reviewer_id":"reviewer-1","role":"risk_reviewer","token":"12345678901234567890123456789012"},{"reviewer_id":"auditor-1","role":"risk_auditor","token":"abcdefghijklmnopqrstuvwxyzABCDEF"}]`
