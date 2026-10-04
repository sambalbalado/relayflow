package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Prefix              string  `json:"prefix"`
	Requested           int     `json:"requested"`
	Submitted           int     `json:"submitted"`
	Completed           int     `json:"completed"`
	Failed              int     `json:"failed"`
	SubmitSeconds       float64 `json:"submit_seconds"`
	TotalSeconds        float64 `json:"total_seconds"`
	SubmitRPS           float64 `json:"submit_rps"`
	EndToEndRPS         float64 `json:"end_to_end_rps"`
	SubmitLatencyP50MS  float64 `json:"submit_latency_p50_ms"`
	SubmitLatencyP95MS  float64 `json:"submit_latency_p95_ms"`
	SubmitLatencyP99MS  float64 `json:"submit_latency_p99_ms"`
	PeakReadySteps      int64   `json:"peak_ready_steps"`
	WaitForTerminalDone bool    `json:"wait_for_terminal"`
}

type workflowResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func main() {
	apiURL := flag.String("url", env("RELAYFLOW_URL", "http://localhost:8080"), "RelayFlow API base URL")
	workflows := flag.Int("workflows", 200, "number of workflows")
	concurrency := flag.Int("concurrency", 32, "concurrent clients")
	timeout := flag.Duration("timeout", 60*time.Second, "overall timeout")
	prefix := flag.String("prefix", fmt.Sprintf("load-%d", time.Now().UnixNano()), "idempotency key prefix")
	wait := flag.Bool("wait", true, "wait for every workflow to reach a terminal state")
	flag.Parse()
	if *workflows < 1 || *concurrency < 1 || *concurrency > 512 {
		fmt.Fprintln(os.Stderr, "workflows and concurrency must be positive; concurrency must not exceed 512")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	benchmark, err := run(ctx, strings.TrimRight(*apiURL, "/"), *prefix, *workflows, *concurrency, *wait)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(benchmark); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, apiURL, prefix string, total, concurrency int, wait bool) (result, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	started := time.Now()
	sampleCtx, stopSampling := context.WithCancel(ctx)
	defer stopSampling()
	var peakReady atomic.Int64
	go sampleReadyQueue(sampleCtx, client, apiURL, &peakReady)

	jobs := make(chan int)
	ids := make(chan string, total)
	errorsFound := make(chan error, total)
	latencies := make(chan time.Duration, total)
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				requestStarted := time.Now()
				id, err := submit(ctx, client, apiURL, fmt.Sprintf("%s-%06d", prefix, index))
				latencies <- time.Since(requestStarted)
				if err != nil {
					errorsFound <- err
					continue
				}
				ids <- id
			}
		}()
	}
	go func() {
		defer close(jobs)
		for index := 0; index < total; index++ {
			select {
			case jobs <- index:
			case <-ctx.Done():
				return
			}
		}
	}()
	workers.Wait()
	close(ids)
	close(errorsFound)
	close(latencies)
	submitElapsed := time.Since(started)
	var workflowIDs []string
	for id := range ids {
		workflowIDs = append(workflowIDs, id)
	}
	for err := range errorsFound {
		if err != nil {
			return result{}, err
		}
	}
	var samples []time.Duration
	for latency := range latencies {
		samples = append(samples, latency)
	}
	benchmark := result{
		Prefix: prefix, Requested: total, Submitted: len(workflowIDs), SubmitSeconds: submitElapsed.Seconds(),
		SubmitRPS: float64(len(workflowIDs)) / submitElapsed.Seconds(), PeakReadySteps: peakReady.Load(),
		SubmitLatencyP50MS: percentile(samples, 0.50), SubmitLatencyP95MS: percentile(samples, 0.95),
		SubmitLatencyP99MS: percentile(samples, 0.99), WaitForTerminalDone: wait,
	}
	if !wait {
		benchmark.TotalSeconds = submitElapsed.Seconds()
		return benchmark, nil
	}
	terminal, failed, err := waitForTerminal(ctx, client, apiURL, workflowIDs, concurrency)
	if err != nil {
		return result{}, err
	}
	stopSampling()
	benchmark.Completed = terminal
	benchmark.Failed = failed
	benchmark.TotalSeconds = time.Since(started).Seconds()
	benchmark.EndToEndRPS = float64(terminal) / benchmark.TotalSeconds
	benchmark.PeakReadySteps = peakReady.Load()
	return benchmark, nil
}

func submit(ctx context.Context, client *http.Client, apiURL, key string) (string, error) {
	payload, _ := json.Marshal(map[string]any{
		"idempotency_key": key, "name": "load workflow",
		"steps": []map[string]any{{"key": "work", "kind": "noop", "input": map[string]any{}}},
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/v1/workflows", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return "", fmt.Errorf("submit returned %s: %s", response.Status, body)
	}
	var workflow workflowResponse
	if err := json.NewDecoder(response.Body).Decode(&workflow); err != nil {
		return "", err
	}
	if workflow.ID == "" {
		return "", errors.New("submit response omitted workflow id")
	}
	return workflow.ID, nil
}

func waitForTerminal(ctx context.Context, client *http.Client, apiURL string, ids []string, concurrency int) (int, int, error) {
	jobs := make(chan string)
	var terminal atomic.Int64
	var failed atomic.Int64
	errCh := make(chan error, len(ids))
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range jobs {
				for {
					status, err := getStatus(ctx, client, apiURL, id)
					if err != nil {
						errCh <- err
						break
					}
					if status == "succeeded" || status == "failed" || status == "cancelled" {
						terminal.Add(1)
						if status != "succeeded" {
							failed.Add(1)
						}
						break
					}
					select {
					case <-ctx.Done():
						errCh <- ctx.Err()
						return
					case <-time.After(25 * time.Millisecond):
					}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, id := range ids {
			select {
			case jobs <- id:
			case <-ctx.Done():
				return
			}
		}
	}()
	workers.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return int(terminal.Load()), int(failed.Load()), err
		}
	}
	return int(terminal.Load()), int(failed.Load()), nil
}

func getStatus(ctx context.Context, client *http.Client, apiURL, id string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/v1/workflows/"+id, nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status query returned %s", response.Status)
	}
	var workflow workflowResponse
	if err := json.NewDecoder(response.Body).Decode(&workflow); err != nil {
		return "", err
	}
	return workflow.Status, nil
}

func sampleReadyQueue(ctx context.Context, client *http.Client, apiURL string, peak *atomic.Int64) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/metrics", nil)
		if response, err := client.Do(request); err == nil {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			response.Body.Close()
			for _, line := range strings.Split(string(body), "\n") {
				var ready int64
				if _, err := fmt.Sscanf(line, `relayflow_steps{status="ready"} %d`, &ready); err == nil {
					for current := peak.Load(); ready > current; current = peak.Load() {
						if peak.CompareAndSwap(current, ready) {
							break
						}
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func percentile(samples []time.Duration, quantile float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	index := int(float64(len(samples)-1) * quantile)
	return float64(samples[index].Microseconds()) / 1000
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
