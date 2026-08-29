package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/config"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/httpapi"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/payment"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/server"
)

func main() {
	os.Exit(run())
}

func run() int {
	bootstrapLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		bootstrapLogger.Error("invalid configuration", slog.String("error", err.Error()))
		return 1
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("create database pool", slog.String("error", err.Error()))
		return 1
	}
	defer pool.Close()

	paymentRepository := payment.NewPostgresRepository(pool)
	paymentService := payment.NewService(paymentRepository)
	handler := httpapi.NewHandler(pool, paymentService, cfg.ReadinessTimeout, cfg.PaymentTimeout, logger)
	httpServer := server.New(cfg.HTTPAddr, handler, cfg.ShutdownTimeout, logger)

	logger.Info("starting payment API", slog.String("address", cfg.HTTPAddr))
	if err := httpServer.Run(ctx); err != nil {
		logger.Error("payment API stopped with an error", slog.String("error", err.Error()))
		return 1
	}
	logger.Info("payment API stopped")
	return 0
}
