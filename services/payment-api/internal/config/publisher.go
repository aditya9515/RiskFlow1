package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultKafkaTopic           = "payments.created"
	defaultOutboxPollInterval   = 250 * time.Millisecond
	defaultOutboxPublishTimeout = 5 * time.Second
	defaultOutboxRetryMin       = 100 * time.Millisecond
	defaultOutboxRetryMax       = 5 * time.Second
	defaultOutboxMetricsAddr    = ":9091"
)

// PublisherConfig contains the outbox-publisher process configuration.
type PublisherConfig struct {
	DatabaseURL            string
	KafkaBrokers           []string
	KafkaTopic             string
	PollInterval           time.Duration
	PublishTimeout         time.Duration
	RetryMin               time.Duration
	RetryMax               time.Duration
	MetricsAddr            string
	MetricsShutdownTimeout time.Duration
	LogLevel               slog.Level
}

// LoadPublisher reads and validates outbox-publisher configuration from the
// process environment.
func LoadPublisher() (PublisherConfig, error) {
	return loadPublisher(os.LookupEnv)
}

func loadPublisher(lookup lookupEnv) (PublisherConfig, error) {
	databaseURL := strings.TrimSpace(value(lookup, "DATABASE_URL", ""))
	if databaseURL == "" {
		return PublisherConfig{}, fmt.Errorf("DATABASE_URL is required")
	}

	brokers, err := parseKafkaBrokers(value(lookup, "KAFKA_BROKERS", ""))
	if err != nil {
		return PublisherConfig{}, err
	}

	topic := strings.TrimSpace(value(lookup, "KAFKA_TOPIC", defaultKafkaTopic))
	if !validKafkaTopic(topic) {
		return PublisherConfig{}, fmt.Errorf("KAFKA_TOPIC must contain 1-249 letters, digits, dots, underscores, or hyphens")
	}

	pollInterval, err := duration(lookup, "OUTBOX_POLL_INTERVAL", defaultOutboxPollInterval)
	if err != nil {
		return PublisherConfig{}, err
	}
	publishTimeout, err := duration(lookup, "OUTBOX_PUBLISH_TIMEOUT", defaultOutboxPublishTimeout)
	if err != nil {
		return PublisherConfig{}, err
	}
	retryMin, err := duration(lookup, "OUTBOX_RETRY_MIN_BACKOFF", defaultOutboxRetryMin)
	if err != nil {
		return PublisherConfig{}, err
	}
	retryMax, err := duration(lookup, "OUTBOX_RETRY_MAX_BACKOFF", defaultOutboxRetryMax)
	if err != nil {
		return PublisherConfig{}, err
	}
	if retryMax < retryMin {
		return PublisherConfig{}, fmt.Errorf("OUTBOX_RETRY_MAX_BACKOFF must be greater than or equal to OUTBOX_RETRY_MIN_BACKOFF")
	}
	metricsAddr := strings.TrimSpace(value(lookup, "OUTBOX_METRICS_ADDR", defaultOutboxMetricsAddr))
	if err := validateHTTPAddr(metricsAddr); err != nil {
		return PublisherConfig{}, fmt.Errorf("OUTBOX_METRICS_ADDR: %w", err)
	}
	metricsShutdownTimeout, err := duration(lookup, "METRICS_SHUTDOWN_TIMEOUT", defaultMetricsShutdown)
	if err != nil {
		return PublisherConfig{}, err
	}

	logLevel, err := parseLogLevel(value(lookup, "LOG_LEVEL", "info"))
	if err != nil {
		return PublisherConfig{}, err
	}

	return PublisherConfig{
		DatabaseURL:            databaseURL,
		KafkaBrokers:           brokers,
		KafkaTopic:             topic,
		PollInterval:           pollInterval,
		PublishTimeout:         publishTimeout,
		RetryMin:               retryMin,
		RetryMax:               retryMax,
		MetricsAddr:            metricsAddr,
		MetricsShutdownTimeout: metricsShutdownTimeout,
		LogLevel:               logLevel,
	}, nil
}

func parseKafkaBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			continue
		}
		host, port, err := net.SplitHostPort(broker)
		if err != nil || host == "" {
			return nil, fmt.Errorf("KAFKA_BROKERS entries must use host:port form")
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, fmt.Errorf("KAFKA_BROKERS ports must be between 1 and 65535")
		}
		brokers = append(brokers, broker)
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS is required")
	}

	return brokers, nil
}

func validKafkaTopic(topic string) bool {
	if len(topic) == 0 || len(topic) > 249 || topic == "." || topic == ".." {
		return false
	}
	for _, character := range topic {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}
