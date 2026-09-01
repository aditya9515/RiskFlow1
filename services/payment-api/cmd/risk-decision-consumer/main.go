package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/config"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/decision"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/observability"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/server"
	"golang.org/x/sync/errgroup"
)

func main() {
	os.Exit(run())
}

func run() int {
	bootstrapLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadDecisionConsumer()
	if err != nil {
		bootstrapLogger.Error("invalid decision consumer configuration", slog.String("error", err.Error()))
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
	store, err := decision.NewPostgresStore(pool)
	if err != nil {
		logger.Error("create decision store", slog.String("error", err.Error()))
		return 1
	}
	consumer, err := decision.NewKafkaConsumer(cfg.KafkaBrokers, cfg.Topic, cfg.ConsumerGroup, cfg.AutoOffsetReset)
	if err != nil {
		logger.Error("create decision Kafka consumer", slog.String("error", err.Error()))
		return 1
	}
	registry := observability.NewRegistry("risk-decision-consumer")
	decisionMetrics := observability.NewDecisionMetrics(registry)
	worker, err := decision.NewWorker(consumer, store, decision.WorkerConfig{
		PollTimeout:    cfg.PollTimeout,
		ProcessTimeout: cfg.ProcessTimeout,
		RetryBackoff:   cfg.RetryBackoff,
	}, logger, decisionMetrics)
	if err != nil {
		consumer.Close()
		logger.Error("create decision persistence worker", slog.String("error", err.Error()))
		return 1
	}

	logger.Info("starting risk decision consumer",
		slog.String("topic", cfg.Topic),
		slog.String("consumer_group", cfg.ConsumerGroup),
		slog.String("auto_offset_reset", cfg.AutoOffsetReset),
	)
	metricsServer := server.New(cfg.MetricsAddr, observability.WorkerHandler(registry), cfg.MetricsShutdownTimeout, logger)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return worker.Run(groupCtx) })
	group.Go(func() error { return metricsServer.Run(groupCtx) })
	if err := group.Wait(); err != nil {
		logger.Error("risk decision consumer stopped with an error", slog.String("error", err.Error()))
		return 1
	}
	return 0
}
