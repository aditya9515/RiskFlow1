package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/config"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/observability"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/outbox"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/server"
	"golang.org/x/sync/errgroup"
)

func main() {
	os.Exit(run())
}

func run() int {
	bootstrapLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadPublisher()
	if err != nil {
		bootstrapLogger.Error("invalid publisher configuration", slog.String("error", err.Error()))
		return 1
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("create database pool", slog.String("error", err.Error()))
		return 1
	}
	defer pool.Close()

	publisher, err := outbox.NewKafkaPublisher(cfg.KafkaBrokers)
	if err != nil {
		logger.Error("create Kafka publisher", slog.String("error", err.Error()))
		return 1
	}
	defer publisher.Close()

	store, err := outbox.NewPostgresStore(pool, cfg.RetryMin, cfg.RetryMax)
	if err != nil {
		logger.Error("create outbox store", slog.String("error", err.Error()))
		return 1
	}
	registry := observability.NewRegistry("outbox-publisher")
	outboxMetrics := observability.NewOutboxMetrics(registry)

	worker, err := outbox.NewWorker(
		store,
		publisher,
		outbox.WorkerConfig{
			Topic:          cfg.KafkaTopic,
			PollInterval:   cfg.PollInterval,
			PublishTimeout: cfg.PublishTimeout,
			RetryMin:       cfg.RetryMin,
			RetryMax:       cfg.RetryMax,
		},
		logger,
		outboxMetrics,
	)
	if err != nil {
		logger.Error("create outbox worker", slog.String("error", err.Error()))
		return 1
	}

	logger.Info("starting outbox publisher",
		slog.String("topic", cfg.KafkaTopic),
		slog.Any("brokers", cfg.KafkaBrokers),
	)
	metricsServer := server.New(cfg.MetricsAddr, observability.WorkerHandler(registry), cfg.MetricsShutdownTimeout, logger)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return worker.Run(groupCtx) })
	group.Go(func() error { return metricsServer.Run(groupCtx) })
	if err := group.Wait(); err != nil {
		logger.Error("outbox publisher stopped with an error", slog.String("error", err.Error()))
		return 1
	}
	logger.Info("outbox publisher stopped")
	return 0
}
