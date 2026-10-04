package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sambalbalado/relayflow/internal/config"
	"github.com/sambalbalado/relayflow/internal/migrations"
	"github.com/sambalbalado/relayflow/internal/store"
	"github.com/sambalbalado/relayflow/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrations.Run(ctx, pool); err != nil {
		return err
	}
	return worker.New(store.New(pool), logger, cfg.WorkerID, cfg.WorkerConcurrency,
		cfg.PollInterval, cfg.LeaseDuration, cfg.HeartbeatInterval, cfg.ShutdownTimeout).Run(ctx)
}
