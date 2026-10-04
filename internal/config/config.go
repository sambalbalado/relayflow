package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL       string
	HTTPAddress       string
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	WorkerID          string
	WorkerConcurrency int
	ShutdownTimeout   time.Duration
}

func Load() (Config, error) {
	poll, err := time.ParseDuration(env("POLL_INTERVAL", "250ms"))
	if err != nil {
		return Config{}, fmt.Errorf("parse POLL_INTERVAL: %w", err)
	}
	lease, err := time.ParseDuration(env("LEASE_DURATION", "10s"))
	if err != nil {
		return Config{}, fmt.Errorf("parse LEASE_DURATION: %w", err)
	}
	heartbeat, err := time.ParseDuration(env("HEARTBEAT_INTERVAL", "3s"))
	if err != nil {
		return Config{}, fmt.Errorf("parse HEARTBEAT_INTERVAL: %w", err)
	}
	if poll <= 0 || lease <= 0 || heartbeat <= 0 || heartbeat >= lease {
		return Config{}, fmt.Errorf("durations must be positive and HEARTBEAT_INTERVAL must be shorter than LEASE_DURATION")
	}
	shutdown, err := time.ParseDuration(env("SHUTDOWN_TIMEOUT", "15s"))
	if err != nil {
		return Config{}, fmt.Errorf("parse SHUTDOWN_TIMEOUT: %w", err)
	}
	if shutdown <= 0 || shutdown > 5*time.Minute {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be between 1ns and 5m")
	}
	concurrency, err := strconv.Atoi(env("WORKER_CONCURRENCY", "1"))
	if err != nil || concurrency < 1 || concurrency > 64 {
		return Config{}, fmt.Errorf("WORKER_CONCURRENCY must be an integer between 1 and 64")
	}
	return Config{
		DatabaseURL:       env("DATABASE_URL", "postgres://relayflow:relayflow@localhost:5432/relayflow?sslmode=disable"),
		HTTPAddress:       env("HTTP_ADDRESS", ":8080"),
		PollInterval:      poll,
		LeaseDuration:     lease,
		HeartbeatInterval: heartbeat,
		WorkerID:          env("WORKER_ID", hostname()),
		WorkerConcurrency: concurrency,
		ShutdownTimeout:   shutdown,
	}, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "worker-unknown"
	}
	return name
}
