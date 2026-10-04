package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sambalbalado/relayflow/internal/domain"
)

type drainStore struct {
	claimed   chan struct{}
	completed chan struct{}
	issued    atomic.Bool
}

func (s *drainStore) ClaimStep(ctx context.Context, workerID string, _ time.Duration) (*domain.ClaimedStep, error) {
	if s.issued.Swap(true) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	close(s.claimed)
	return &domain.ClaimedStep{
		Step: domain.Step{
			ID: "step-1", WorkflowID: "workflow-1", OperationKey: "workflow-1:step-1",
			Kind: "sleep", Input: json.RawMessage(`{"duration":"75ms"}`), TimeoutMS: 2000,
		},
		AttemptID: "attempt-1", AttemptNo: 1, LeaseToken: "lease-1", Timeout: 2 * time.Second,
	}, nil
}

func (s *drainStore) Heartbeat(context.Context, domain.ClaimedStep, time.Duration) (bool, error) {
	return true, nil
}
func (s *drainStore) ReclaimExpiredSteps(context.Context, int) (int, error) { return 0, nil }
func (s *drainStore) CompleteStep(context.Context, domain.ClaimedStep, json.RawMessage) error {
	close(s.completed)
	return nil
}
func (s *drainStore) FailStep(context.Context, domain.ClaimedStep, domain.StepFailure) error {
	return errors.New("unexpected failure")
}

func TestExecuteEcho(t *testing.T) {
	input := json.RawMessage(`{"message":"durable"}`)
	output, err := execute(context.Background(), domain.ClaimedStep{Step: domain.Step{Kind: "echo", Input: input}})
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != string(input) {
		t.Fatalf("output = %s", output)
	}
}

func TestExecuteRejectsUnknownKind(t *testing.T) {
	if _, err := execute(context.Background(), domain.ClaimedStep{Step: domain.Step{Kind: "shell"}}); err == nil {
		t.Fatal("expected error")
	}
}

func TestExecuteSleepCanBeCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err := execute(ctx, domain.ClaimedStep{Step: domain.Step{Kind: "sleep", Input: json.RawMessage(`{"duration":"1m"}`)}})
	if err == nil {
		t.Fatal("expected cancellation")
	}
	if time.Since(started) > time.Second {
		t.Fatal("cancelled sleep did not stop promptly")
	}
}

func TestExecuteFlakyUsesAttemptNumber(t *testing.T) {
	claim := domain.ClaimedStep{Step: domain.Step{Kind: "flaky", Input: json.RawMessage(`{"fail_attempts":2}`)}, AttemptNo: 2}
	if _, err := execute(context.Background(), claim); err == nil {
		t.Fatal("expected planned transient failure")
	} else {
		var failure domain.StepFailure
		if !errors.As(err, &failure) || failure.Type != domain.FailureRetryable {
			t.Fatalf("failure = %#v, want retryable", err)
		}
	}
	claim.AttemptNo = 3
	if _, err := execute(context.Background(), claim); err != nil {
		t.Fatalf("recovery attempt failed: %v", err)
	}
}

func TestExecutePermanentFailure(t *testing.T) {
	_, err := execute(context.Background(), domain.ClaimedStep{Step: domain.Step{Kind: "fail"}})
	var failure domain.StepFailure
	if !errors.As(err, &failure) || failure.Type != domain.FailurePermanent {
		t.Fatalf("failure = %#v, want permanent", err)
	}
}

func TestWorkerDrainsClaimedStepOnShutdown(t *testing.T) {
	stepStore := &drainStore{claimed: make(chan struct{}), completed: make(chan struct{})}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := New(stepStore, logger, "worker-test", 1, 5*time.Millisecond, time.Second, 100*time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-stepStore.claimed:
	case <-time.After(time.Second):
		t.Fatal("worker did not claim step")
	}
	cancel()
	select {
	case <-stepStore.completed:
	case <-time.After(time.Second):
		t.Fatal("worker did not drain claimed step")
	}
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
}
