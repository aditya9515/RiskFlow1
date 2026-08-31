package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/review"
)

const (
	defaultHTTPAddr         = ":8080"
	defaultReadinessTimeout = time.Second
	defaultPaymentTimeout   = 3 * time.Second
	defaultReviewTimeout    = 3 * time.Second
	defaultDashboardTimeout = 5 * time.Second
	defaultMetricsTimeout   = time.Second
	defaultMetricsShutdown  = 5 * time.Second
	defaultShutdownTimeout  = 10 * time.Second
)

// Config contains the process configuration loaded from environment variables.
type Config struct {
	DatabaseURL         string
	HTTPAddr            string
	ReadinessTimeout    time.Duration
	PaymentTimeout      time.Duration
	ReviewTimeout       time.Duration
	DashboardTimeout    time.Duration
	MetricsTimeout      time.Duration
	ReconciliationGrace time.Duration
	ShutdownTimeout     time.Duration
	LogLevel            slog.Level
	ReviewCredentials   []review.Credential
}

// Load reads and validates configuration from the process environment.
func Load() (Config, error) {
	return load(os.LookupEnv)
}

type lookupEnv func(string) (string, bool)

func load(lookup lookupEnv) (Config, error) {
	databaseURL := strings.TrimSpace(value(lookup, "DATABASE_URL", ""))
	if databaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}

	httpAddr := strings.TrimSpace(value(lookup, "HTTP_ADDR", defaultHTTPAddr))
	if err := validateHTTPAddr(httpAddr); err != nil {
		return Config{}, fmt.Errorf("HTTP_ADDR: %w", err)
	}

	readinessTimeout, err := duration(lookup, "READINESS_TIMEOUT", defaultReadinessTimeout)
	if err != nil {
		return Config{}, err
	}

	paymentTimeout, err := duration(lookup, "PAYMENT_CREATE_TIMEOUT", defaultPaymentTimeout)
	if err != nil {
		return Config{}, err
	}
	reviewTimeout, err := duration(lookup, "REVIEW_REQUEST_TIMEOUT", defaultReviewTimeout)
	if err != nil {
		return Config{}, err
	}
	dashboardTimeout, err := duration(lookup, "DASHBOARD_REQUEST_TIMEOUT", defaultDashboardTimeout)
	if err != nil {
		return Config{}, err
	}
	metricsTimeout, err := duration(lookup, "METRICS_COLLECTION_TIMEOUT", defaultMetricsTimeout)
	if err != nil {
		return Config{}, err
	}
	reconciliationGrace, err := duration(lookup, "RECONCILIATION_GRACE_PERIOD", defaultReconciliationGrace)
	if err != nil {
		return Config{}, err
	}

	shutdownTimeout, err := duration(lookup, "SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}

	reviewCredentials, err := parseReviewCredentials(value(lookup, "REVIEW_AUTH_CREDENTIALS_JSON", ""))
	if err != nil {
		return Config{}, err
	}

	logLevel, err := parseLogLevel(value(lookup, "LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}

	return Config{
		DatabaseURL:         databaseURL,
		HTTPAddr:            httpAddr,
		ReadinessTimeout:    readinessTimeout,
		PaymentTimeout:      paymentTimeout,
		ReviewTimeout:       reviewTimeout,
		DashboardTimeout:    dashboardTimeout,
		MetricsTimeout:      metricsTimeout,
		ReconciliationGrace: reconciliationGrace,
		ShutdownTimeout:     shutdownTimeout,
		LogLevel:            logLevel,
		ReviewCredentials:   reviewCredentials,
	}, nil
}

func parseReviewCredentials(raw string) ([]review.Credential, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("REVIEW_AUTH_CREDENTIALS_JSON is required")
	}
	var encoded []struct {
		ReviewerID string `json:"reviewer_id"`
		Role       string `json:"role"`
		Token      string `json:"token"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&encoded); err != nil {
		return nil, fmt.Errorf("REVIEW_AUTH_CREDENTIALS_JSON must be a valid credential array: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("REVIEW_AUTH_CREDENTIALS_JSON must contain exactly one JSON array")
	}
	credentials := make([]review.Credential, 0, len(encoded))
	for _, item := range encoded {
		credentials = append(credentials, review.Credential{ReviewerID: item.ReviewerID, Role: item.Role, Token: item.Token})
	}
	if len(credentials) > 100 {
		return nil, fmt.Errorf("REVIEW_AUTH_CREDENTIALS_JSON supports at most 100 credentials")
	}
	if _, err := review.NewTokenAuthenticator(credentials); err != nil {
		return nil, fmt.Errorf("REVIEW_AUTH_CREDENTIALS_JSON: %w", err)
	}
	return credentials, nil
}

func value(lookup lookupEnv, key, fallback string) string {
	if raw, ok := lookup(key); ok {
		return raw
	}

	return fallback
}

func duration(lookup lookupEnv, key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(value(lookup, key, fallback.String()))
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}

	return parsed, nil
}

func validateHTTPAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("must be in host:port form: %w", err)
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	return nil
}

func parseLogLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("LOG_LEVEL must be one of debug, info, warn, or error")
	}
}
