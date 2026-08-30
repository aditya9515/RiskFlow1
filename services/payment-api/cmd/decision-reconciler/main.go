package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/config"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/decision"
)

func main() {
	os.Exit(run())
}

func run() int {
	bootstrapLogger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg, err := config.LoadReconciler()
	if err != nil {
		bootstrapLogger.Error("invalid reconciliation configuration", slog.String("error", err.Error()))
		return 1
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("create database pool", slog.String("error", err.Error()))
		return 1
	}
	defer pool.Close()
	reconciler, err := decision.NewReconciler(pool, cfg.GracePeriod)
	if err != nil {
		logger.Error("create decision reconciler", slog.String("error", err.Error()))
		return 1
	}
	report, err := reconciler.Run(ctx)
	if err != nil {
		logger.Error("run decision reconciliation", slog.String("error", err.Error()))
		return 1
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		logger.Error("encode decision reconciliation report", slog.String("error", err.Error()))
		return 1
	}
	return 0
}
