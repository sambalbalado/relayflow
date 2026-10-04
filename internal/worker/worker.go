package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sambalbalado/relayflow/internal/domain"
)

type StepStore interface {
	ClaimStep(context.Context, string, time.Duration) (*domain.ClaimedStep, error)
	Heartbeat(context.Context, domain.ClaimedStep, time.Duration) (bool, error)
	ReclaimExpiredSteps(context.Context, int) (int, error)
	CompleteStep(context.Context, domain.ClaimedStep, json.RawMessage) error
	FailStep(context.Context, domain.ClaimedStep, domain.StepFailure) error
}

type Worker struct {
	store             StepStore
	logger            *slog.Logger
	id                string
	concurrency       int
	pollInterval      time.Duration
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	shutdownTimeout   time.Duration
}

func New(stepStore StepStore, logger *slog.Logger, id string, concurrency int, pollInterval, leaseDuration, heartbeatInterval, shutdownTimeout time.Duration) *Worker {
	return &Worker{
		store: stepStore, logger: logger, id: id, concurrency: concurrency, pollInterval: pollInterval,
		leaseDuration: leaseDuration, heartbeatInterval: heartbeatInterval, shutdownTimeout: shutdownTimeout,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("worker started", "worker_id", w.id, "concurrency", w.concurrency)
	executionCtx, forceStop := context.WithCancel(context.Background())
	defer forceStop()
	var wait sync.WaitGroup
	wait.Add(w.concurrency + 1)
	for lane := 0; lane < w.concurrency; lane++ {
		go func(lane int) {
			defer wait.Done()
			w.executeLoop(ctx, executionCtx, lane)
		}(lane)
	}
	go func() {
		defer wait.Done()
		w.reclaimLoop(ctx)
	}()
	<-ctx.Done()
	w.logger.Info("worker draining", "worker_id", w.id, "timeout", w.shutdownTimeout)
	drained := make(chan struct{})
	go func() {
		wait.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		w.logger.Info("worker stopped", "worker_id", w.id, "drained", true)
	case <-time.After(w.shutdownTimeout):
		w.logger.Warn("worker drain deadline reached", "worker_id", w.id, "timeout", w.shutdownTimeout)
		forceStop()
		<-drained
		w.logger.Info("worker stopped", "worker_id", w.id, "drained", false)
	}
	return nil
}

func (w *Worker) executeLoop(claimCtx, executionCtx context.Context, lane int) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		worked, err := w.runOnce(claimCtx, executionCtx, lane)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("worker iteration failed", "worker_id", w.id, "lane", lane, "error", err)
		}
		if worked {
			continue
		}
		select {
		case <-claimCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) runOnce(claimCtx, executionCtx context.Context, lane int) (bool, error) {
	claim, err := w.store.ClaimStep(claimCtx, w.id, w.leaseDuration)
	if err != nil || claim == nil {
		return false, err
	}
	logger := w.logger.With("worker_id", w.id, "workflow_id", claim.WorkflowID, "step_id", claim.ID,
		"attempt_id", claim.AttemptID, "operation_key", claim.OperationKey, "lane", lane)
	logger.Info("step execution started", "kind", claim.Kind, "attempt_no", claim.AttemptNo)
	return true, w.executeWithHeartbeat(executionCtx, logger, *claim)
}

func (w *Worker) reclaimLoop(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		reclaimed, err := w.store.ReclaimExpiredSteps(ctx, 10)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("lease reclamation failed", "worker_id", w.id, "error", err)
		} else if reclaimed > 0 {
			w.logger.Warn("expired leases reclaimed", "worker_id", w.id, "count", reclaimed)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type executionResult struct {
	output json.RawMessage
	err    error
}

func (w *Worker) executeWithHeartbeat(ctx context.Context, logger *slog.Logger, claim domain.ClaimedStep) error {
	timeout := claim.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	executionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resultCh := make(chan executionResult, 1)
	go func() {
		output, err := execute(executionCtx, claim)
		resultCh <- executionResult{output: output, err: err}
	}()
	heartbeat := time.NewTicker(w.heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-resultCh:
			if result.err != nil {
				if errors.Is(result.err, context.Canceled) && ctx.Err() != nil {
					return ctx.Err()
				}
				failure := domain.StepFailure{Type: domain.FailurePermanent, Message: result.err.Error()}
				if errors.Is(result.err, context.DeadlineExceeded) {
					failure = domain.StepFailure{Type: domain.FailureTimeout, Message: "step execution timed out"}
				} else {
					var stepFailure domain.StepFailure
					if errors.As(result.err, &stepFailure) {
						failure = stepFailure
					}
				}
				if err := w.store.FailStep(ctx, claim, failure); err != nil {
					return fmt.Errorf("persist step failure: %w", err)
				}
				logger.Error("step execution failed", "error", failure.Message, "failure_type", failure.Type)
				return nil
			}
			if err := w.store.CompleteStep(ctx, claim, result.output); err != nil {
				return fmt.Errorf("persist step completion: %w", err)
			}
			logger.Info("step execution succeeded")
			return nil
		case <-heartbeat.C:
			renewed, err := w.store.Heartbeat(ctx, claim, w.leaseDuration)
			if err != nil {
				return err
			}
			if !renewed {
				cancel()
				logger.Warn("step lease lost")
				return fmt.Errorf("%w: step_id=%s", storeLeaseLost, claim.ID)
			}
			logger.Debug("step lease renewed")
		}
	}
}

var storeLeaseLost = errors.New("step lease lost")

func execute(ctx context.Context, claim domain.ClaimedStep) (json.RawMessage, error) {
	switch claim.Kind {
	case "noop":
		return json.RawMessage(`{"ok":true}`), nil
	case "echo":
		if !json.Valid(claim.Input) {
			return nil, errors.New("stored input is invalid JSON")
		}
		return claim.Input, nil
	case "sleep":
		var input struct {
			Duration string `json:"duration"`
		}
		if err := json.Unmarshal(claim.Input, &input); err != nil {
			return nil, fmt.Errorf("decode sleep input: %w", err)
		}
		duration, err := time.ParseDuration(input.Duration)
		if err != nil || duration <= 0 || duration > 5*time.Minute {
			return nil, errors.New("sleep duration must be between 1ns and 5m")
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return json.RawMessage(`{"slept":true}`), nil
		}
	case "flaky":
		var input struct {
			FailAttempts int `json:"fail_attempts"`
		}
		if err := json.Unmarshal(claim.Input, &input); err != nil {
			return nil, domain.StepFailure{Type: domain.FailurePermanent, Message: "decode flaky input: " + err.Error()}
		}
		if input.FailAttempts < 0 || input.FailAttempts > 9 {
			return nil, domain.StepFailure{Type: domain.FailurePermanent, Message: "fail_attempts must be between 0 and 9"}
		}
		if claim.AttemptNo <= input.FailAttempts {
			return nil, domain.StepFailure{Type: domain.FailureRetryable, Message: fmt.Sprintf("planned transient failure on attempt %d", claim.AttemptNo)}
		}
		return json.RawMessage(`{"recovered":true}`), nil
	case "fail":
		return nil, domain.StepFailure{Type: domain.FailurePermanent, Message: "planned permanent failure"}
	default:
		return nil, domain.StepFailure{Type: domain.FailurePermanent, Message: fmt.Sprintf("no handler registered for kind %q", claim.Kind)}
	}
}
