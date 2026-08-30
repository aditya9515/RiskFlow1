package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	defaultRiskDecisionsTopic     = "risk.decisions"
	defaultDecisionConsumerGroup  = "riskflow-decision-persistence-v1"
	defaultDecisionProcessTimeout = 5 * time.Second
	defaultDecisionRetryBackoff   = time.Second
	defaultReconciliationGrace    = 30 * time.Second
	defaultReconciliationTimeout  = 10 * time.Second
)

// DecisionConsumerConfig contains the persistence worker configuration.
type DecisionConsumerConfig struct {
	DatabaseURL     string
	KafkaBrokers    []string
	Topic           string
	ConsumerGroup   string
	AutoOffsetReset string
	ProcessTimeout  time.Duration
	RetryBackoff    time.Duration
	LogLevel        slog.Level
}

// ReconcilerConfig contains the one-shot reconciliation job configuration.
type ReconcilerConfig struct {
	DatabaseURL string
	GracePeriod time.Duration
	Timeout     time.Duration
	LogLevel    slog.Level
}

func LoadDecisionConsumer() (DecisionConsumerConfig, error) {
	return loadDecisionConsumer(os.LookupEnv)
}

func loadDecisionConsumer(lookup lookupEnv) (DecisionConsumerConfig, error) {
	databaseURL := strings.TrimSpace(value(lookup, "DATABASE_URL", ""))
	if databaseURL == "" {
		return DecisionConsumerConfig{}, fmt.Errorf("DATABASE_URL is required")
	}
	brokers, err := parseKafkaBrokers(value(lookup, "KAFKA_BROKERS", ""))
	if err != nil {
		return DecisionConsumerConfig{}, err
	}
	topic := strings.TrimSpace(value(lookup, "RISK_DECISIONS_TOPIC", defaultRiskDecisionsTopic))
	if !validKafkaTopic(topic) {
		return DecisionConsumerConfig{}, fmt.Errorf("RISK_DECISIONS_TOPIC must contain 1-249 letters, digits, dots, underscores, or hyphens")
	}
	group := strings.TrimSpace(value(lookup, "DECISION_CONSUMER_GROUP", defaultDecisionConsumerGroup))
	if !validKafkaTopic(group) {
		return DecisionConsumerConfig{}, fmt.Errorf("DECISION_CONSUMER_GROUP must contain 1-249 letters, digits, dots, underscores, or hyphens")
	}
	autoOffsetReset := strings.ToLower(strings.TrimSpace(value(lookup, "DECISION_AUTO_OFFSET_RESET", "earliest")))
	if autoOffsetReset != "earliest" && autoOffsetReset != "latest" {
		return DecisionConsumerConfig{}, fmt.Errorf("DECISION_AUTO_OFFSET_RESET must be earliest or latest")
	}
	processTimeout, err := duration(lookup, "DECISION_PROCESS_TIMEOUT", defaultDecisionProcessTimeout)
	if err != nil {
		return DecisionConsumerConfig{}, err
	}
	retryBackoff, err := duration(lookup, "DECISION_RETRY_BACKOFF", defaultDecisionRetryBackoff)
	if err != nil {
		return DecisionConsumerConfig{}, err
	}
	logLevel, err := parseLogLevel(value(lookup, "LOG_LEVEL", "info"))
	if err != nil {
		return DecisionConsumerConfig{}, err
	}

	return DecisionConsumerConfig{
		DatabaseURL:     databaseURL,
		KafkaBrokers:    brokers,
		Topic:           topic,
		ConsumerGroup:   group,
		AutoOffsetReset: autoOffsetReset,
		ProcessTimeout:  processTimeout,
		RetryBackoff:    retryBackoff,
		LogLevel:        logLevel,
	}, nil
}

func LoadReconciler() (ReconcilerConfig, error) {
	return loadReconciler(os.LookupEnv)
}

func loadReconciler(lookup lookupEnv) (ReconcilerConfig, error) {
	databaseURL := strings.TrimSpace(value(lookup, "DATABASE_URL", ""))
	if databaseURL == "" {
		return ReconcilerConfig{}, fmt.Errorf("DATABASE_URL is required")
	}
	grace, err := duration(lookup, "RECONCILIATION_GRACE_PERIOD", defaultReconciliationGrace)
	if err != nil {
		return ReconcilerConfig{}, err
	}
	timeout, err := duration(lookup, "RECONCILIATION_TIMEOUT", defaultReconciliationTimeout)
	if err != nil {
		return ReconcilerConfig{}, err
	}
	logLevel, err := parseLogLevel(value(lookup, "LOG_LEVEL", "info"))
	if err != nil {
		return ReconcilerConfig{}, err
	}
	return ReconcilerConfig{
		DatabaseURL: databaseURL,
		GracePeriod: grace,
		Timeout:     timeout,
		LogLevel:    logLevel,
	}, nil
}
