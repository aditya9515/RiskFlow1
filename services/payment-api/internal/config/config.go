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
	defaultHTTPAddr         = ":8080"
	defaultReadinessTimeout = time.Second
	defaultShutdownTimeout  = 10 * time.Second
)

// Config contains the process configuration loaded from environment variables.
type Config struct {
	DatabaseURL      string
	HTTPAddr         string
	ReadinessTimeout time.Duration
	ShutdownTimeout  time.Duration
	LogLevel         slog.Level
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

	shutdownTimeout, err := duration(lookup, "SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}

	logLevel, err := parseLogLevel(value(lookup, "LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}

	return Config{
		DatabaseURL:      databaseURL,
		HTTPAddr:         httpAddr,
		ReadinessTimeout: readinessTimeout,
		ShutdownTimeout:  shutdownTimeout,
		LogLevel:         logLevel,
	}, nil
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
