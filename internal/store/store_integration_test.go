//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sambalbalado/relayflow/internal/domain"
	"github.com/sambalbalado/relayflow/internal/migrations"
)

func integrationStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for integration tests")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return New(pool), pool
}

func testRequest(t *testing.T, suffix string) domain.SubmitWorkflowRequest {
	t.Helper()
	return domain.SubmitWorkflowRequest{
		IdempotencyKey: fmt.Sprintf("integration-%s-%d", suffix, time.Now().UnixNano()),
		Name:           "integration workflow",
		Steps:          []domain.StepRequest{{Key: "work", Kind: "echo", Input: json.RawMessage(`{"value":1}`)}},
	}
}

func claimWorkflow(t *testing.T, workflowStore *Store, workflowID string) domain.ClaimedStep {
	t.Helper()
	for i := 0; i < 20; i++ {
		claim, err := workflowStore.ClaimStep(context.Background(), "transition-test", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if claim == nil {
			t.Fatal("expected a claim")
		}
		if claim.WorkflowID == workflowID {
			return *claim
		}
		if err := workflowStore.CompleteStep(context.Background(), *claim, json.RawMessage(`{"cleanup":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("workflow was not claimable")
	return domain.ClaimedStep{}
}

func TestConcurrentClaimsHaveOneWinner(t *testing.T) {
	workflowStore, _ := integrationStore(t)
	workflow, _, err := workflowStore.CreateWorkflow(context.Background(), testRequest(t, "claim"))
	if err != nil {
		t.Fatal(err)
	}

	const contenders = 16
	claims := make(chan *domain.ClaimedStep, contenders)
	errs := make(chan error, contenders)
	var wait sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			claim, claimErr := workflowStore.ClaimStep(context.Background(), fmt.Sprintf("worker-%d", worker), time.Second)
			claims <- claim
			errs <- claimErr
		}(i)
	}
	wait.Wait()
	close(claims)
	close(errs)
	for claimErr := range errs {
		if claimErr != nil {
			t.Fatal(claimErr)
		}
	}
	var winners []*domain.ClaimedStep
	for claim := range claims {
		if claim != nil && claim.WorkflowID == workflow.ID {
			winners = append(winners, claim)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("claim winners = %d, want 1", len(winners))
	}
	if err := workflowStore.CompleteStep(context.Background(), *winners[0], json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
}

func TestIdempotencyKeyRejectsDifferentDefinition(t *testing.T) {
	workflowStore, _ := integrationStore(t)
	request := testRequest(t, "idempotency")
	first, created, err := workflowStore.CreateWorkflow(context.Background(), request)
	if err != nil || !created {
		t.Fatalf("first submission: created=%v err=%v", created, err)
	}
	second, created, err := workflowStore.CreateWorkflow(context.Background(), request)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("same submission: id=%s created=%v err=%v", second.ID, created, err)
	}
	request.Name = "different definition"
	_, _, err = workflowStore.CreateWorkflow(context.Background(), request)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different submission error = %v", err)
	}
	claim, err := workflowStore.ClaimStep(context.Background(), "idempotency-cleanup", time.Second)
	if err != nil || claim == nil || claim.WorkflowID != first.ID {
		t.Fatalf("cleanup claim: claim=%+v err=%v", claim, err)
	}
	if err := workflowStore.CompleteStep(context.Background(), *claim, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatAndConcurrentReclamation(t *testing.T) {
	workflowStore, pool := integrationStore(t)
	workflow, _, err := workflowStore.CreateWorkflow(context.Background(), testRequest(t, "lease"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := workflowStore.ClaimStep(context.Background(), "worker-a", 100*time.Millisecond)
	if err != nil || claim == nil {
		t.Fatalf("claim: claim=%v err=%v", claim, err)
	}
	renewed, err := workflowStore.Heartbeat(context.Background(), *claim, 300*time.Millisecond)
	if err != nil || !renewed {
		t.Fatalf("heartbeat: renewed=%v err=%v", renewed, err)
	}
	time.Sleep(150 * time.Millisecond)
	if reclaimed, err := workflowStore.ReclaimExpiredSteps(context.Background(), 10); err != nil || reclaimed != 0 {
		t.Fatalf("early reclaim: count=%d err=%v", reclaimed, err)
	}
	time.Sleep(200 * time.Millisecond)

	const reclaimers = 8
	counts := make(chan int, reclaimers)
	errs := make(chan error, reclaimers)
	var wait sync.WaitGroup
	for i := 0; i < reclaimers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			count, reclaimErr := workflowStore.ReclaimExpiredSteps(context.Background(), 10)
			counts <- count
			errs <- reclaimErr
		}()
	}
	wait.Wait()
	close(counts)
	close(errs)
	total := 0
	for count := range counts {
		total += count
	}
	for reclaimErr := range errs {
		if reclaimErr != nil {
			t.Fatal(reclaimErr)
		}
	}
	if total != 1 {
		t.Fatalf("total reclaims = %d, want 1", total)
	}

	second, err := workflowStore.ClaimStep(context.Background(), "worker-b", time.Second)
	if err != nil || second == nil || second.WorkflowID != workflow.ID || second.AttemptNo != 2 {
		t.Fatalf("second claim: claim=%+v err=%v", second, err)
	}
	var history string
	if err := pool.QueryRow(context.Background(), `SELECT string_agg(status || ':' || worker_id, ',' ORDER BY attempt_no)
		FROM attempts WHERE step_id = $1::uuid`, second.ID).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if history != "lease_expired:worker-a,running:worker-b" {
		t.Fatalf("attempt history = %q", history)
	}
	if err := workflowStore.CompleteStep(context.Background(), *second, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
}

func TestCancellationAndTerminalTransitions(t *testing.T) {
	workflowStore, _ := integrationStore(t)
	pending, _, err := workflowStore.CreateWorkflow(context.Background(), testRequest(t, "cancel-pending"))
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := workflowStore.CancelWorkflow(context.Background(), pending.ID)
	if err != nil || cancelled.Status != domain.WorkflowCancelled || cancelled.Steps[0].Status != domain.StepCancelled {
		t.Fatalf("cancel pending: workflow=%+v err=%v", cancelled, err)
	}
	repeated, err := workflowStore.CancelWorkflow(context.Background(), pending.ID)
	if err != nil || repeated.Status != domain.WorkflowCancelled {
		t.Fatalf("repeat cancellation: workflow=%+v err=%v", repeated, err)
	}

	succeeded, _, err := workflowStore.CreateWorkflow(context.Background(), testRequest(t, "cancel-succeeded"))
	if err != nil {
		t.Fatal(err)
	}
	claim := claimWorkflow(t, workflowStore, succeeded.ID)
	if err := workflowStore.CompleteStep(context.Background(), claim, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := workflowStore.CompleteStep(context.Background(), claim, json.RawMessage(`{"duplicate":true}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("duplicate completion error = %v, want ErrLeaseLost", err)
	}
	if _, err := workflowStore.CancelWorkflow(context.Background(), succeeded.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("cancel succeeded error = %v, want ErrInvalidTransition", err)
	}
}

func TestEventHistoryPagination(t *testing.T) {
	workflowStore, _ := integrationStore(t)
	workflow, _, err := workflowStore.CreateWorkflow(context.Background(), testRequest(t, "events"))
	if err != nil {
		t.Fatal(err)
	}
	claim := claimWorkflow(t, workflowStore, workflow.ID)
	if err := workflowStore.CompleteStep(context.Background(), claim, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	first, err := workflowStore.ListEvents(context.Background(), workflow.ID, 0, 2)
	if err != nil || len(first) != 2 || first[0].ID >= first[1].ID {
		t.Fatalf("first event page = %+v, err=%v", first, err)
	}
	second, err := workflowStore.ListEvents(context.Background(), workflow.ID, first[1].ID, 10)
	if err != nil || len(second) != 3 || second[0].ID <= first[1].ID {
		t.Fatalf("second event page = %+v, err=%v", second, err)
	}
}
