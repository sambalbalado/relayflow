package config

import (
	"testing"
	"time"
)

func TestLoadWorkerConcurrency(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "8")
	config, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if config.WorkerConcurrency != 8 {
		t.Fatalf("WorkerConcurrency = %d", config.WorkerConcurrency)
	}
}

func TestLoadShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "20s")
	config, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if config.ShutdownTimeout != 20*time.Second {
		t.Fatalf("ShutdownTimeout = %s", config.ShutdownTimeout)
	}
}

func TestLoadRejectsInvalidShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "0s")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid shutdown timeout error")
	}
}

func TestLoadRejectsInvalidWorkerConcurrency(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "65")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid concurrency error")
	}
}
