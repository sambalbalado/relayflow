package main

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	samples := []time.Duration{40 * time.Millisecond, 10 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond}
	if got := percentile(samples, 0.50); got != 20 {
		t.Fatalf("p50 = %v, want 20", got)
	}
	if got := percentile(samples, 0.99); got != 30 {
		t.Fatalf("p99 = %v, want 30", got)
	}
}
