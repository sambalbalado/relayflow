package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sambalbalado/relayflow/internal/domain"
	"github.com/sambalbalado/relayflow/internal/observability"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyConflict = errors.New("idempotency key already used for a different workflow")
	ErrLeaseLost           = errors.New("step lease is no longer current")
	ErrInvalidTransition   = errors.New("invalid workflow state transition")
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) MetricsSnapshot(ctx context.Context) (observability.Snapshot, error) {
	snapshot := observability.Snapshot{
		Steps:            make(map[string]int64),
		Workflows:        make(map[string]int64),
		ExecutionBuckets: make([]int64, len(observability.DurationBuckets)),
	}
	stepRows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM steps GROUP BY status`)
	if err != nil {
		return snapshot, fmt.Errorf("count steps for metrics: %w", err)
	}
	for stepRows.Next() {
		var status string
		var count int64
		if err := stepRows.Scan(&status, &count); err != nil {
			stepRows.Close()
			return snapshot, fmt.Errorf("scan step metrics: %w", err)
		}
		snapshot.Steps[status] = count
	}
	if err := stepRows.Err(); err != nil {
		stepRows.Close()
		return snapshot, fmt.Errorf("iterate step metrics: %w", err)
	}
	stepRows.Close()
	workflowRows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM workflows GROUP BY status`)
	if err != nil {
		return snapshot, fmt.Errorf("count workflows for metrics: %w", err)
	}
	for workflowRows.Next() {
		var status string
		var count int64
		if err := workflowRows.Scan(&status, &count); err != nil {
			workflowRows.Close()
			return snapshot, fmt.Errorf("scan workflow metrics: %w", err)
		}
		snapshot.Workflows[status] = count
	}
	if err := workflowRows.Err(); err != nil {
		workflowRows.Close()
		return snapshot, fmt.Errorf("iterate workflow metrics: %w", err)
	}
	workflowRows.Close()
	if err := s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE event_type = 'step.claimed'),
		count(*) FILTER (WHERE event_type = 'step.retry_scheduled'),
		count(*) FILTER (WHERE event_type = 'step.failed'),
		count(*) FILTER (WHERE event_type = 'workflow.cancelled'),
		count(*) FILTER (WHERE event_type = 'step.lease_expired')
		FROM workflow_events`).Scan(&snapshot.Claims, &snapshot.Retries, &snapshot.Failures,
		&snapshot.Cancellations, &snapshot.LeaseExpirations); err != nil {
		return snapshot, fmt.Errorf("count event metrics: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE status = 'timed_out'`).Scan(&snapshot.Timeouts); err != nil {
		return snapshot, fmt.Errorf("count timeout metrics: %w", err)
	}
	durations, err := s.pool.Query(ctx, `SELECT extract(epoch FROM (finished_at - started_at))::double precision
		FROM attempts WHERE finished_at IS NOT NULL`)
	if err != nil {
		return snapshot, fmt.Errorf("load execution duration metrics: %w", err)
	}
	for durations.Next() {
		var duration float64
		if err := durations.Scan(&duration); err != nil {
			durations.Close()
			return snapshot, fmt.Errorf("scan execution duration: %w", err)
		}
		snapshot.ExecutionCount++
		snapshot.ExecutionSumSecond += duration
		for index, upper := range observability.DurationBuckets {
			if duration <= upper {
				snapshot.ExecutionBuckets[index]++
			}
		}
	}
	if err := durations.Err(); err != nil {
		durations.Close()
		return snapshot, fmt.Errorf("iterate execution durations: %w", err)
	}
	durations.Close()
	return snapshot, nil
}

func (s *Store) CreateWorkflow(ctx context.Context, request domain.SubmitWorkflowRequest) (domain.Workflow, bool, error) {
	definitionHash, err := hashDefinition(request)
	if err != nil {
		return domain.Workflow{}, false, fmt.Errorf("hash workflow definition: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Workflow{}, false, fmt.Errorf("begin create workflow: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var workflow domain.Workflow
	err = tx.QueryRow(ctx, `
		INSERT INTO workflows (idempotency_key, name, definition_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id::text, idempotency_key, name, status, created_at, updated_at, completed_at`,
		request.IdempotencyKey, request.Name, definitionHash,
	).Scan(&workflow.ID, &workflow.IdempotencyKey, &workflow.Name, &workflow.Status, &workflow.CreatedAt, &workflow.UpdatedAt, &workflow.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingID, existingHash string
		matchErr := tx.QueryRow(ctx, `SELECT id::text, definition_hash FROM workflows WHERE idempotency_key = $1`,
			request.IdempotencyKey).Scan(&existingID, &existingHash)
		if errors.Is(matchErr, pgx.ErrNoRows) {
			return domain.Workflow{}, false, ErrIdempotencyConflict
		}
		if matchErr != nil {
			return domain.Workflow{}, false, fmt.Errorf("compare idempotent submission: %w", matchErr)
		}
		if existingHash != definitionHash {
			return domain.Workflow{}, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.Workflow{}, false, fmt.Errorf("commit idempotent lookup: %w", err)
		}
		existing, getErr := s.GetWorkflow(ctx, existingID)
		return existing, false, getErr
	}
	if err != nil {
		return domain.Workflow{}, false, fmt.Errorf("insert workflow: %w", err)
	}

	if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, event_type, payload)
		VALUES ($1::uuid, 'workflow.submitted', jsonb_build_object('name', $2::text, 'idempotency_key', $3::text))`,
		workflow.ID, workflow.Name, workflow.IdempotencyKey); err != nil {
		return domain.Workflow{}, false, fmt.Errorf("append submission events: %w", err)
	}
	stepIDs := make(map[string]string, len(request.Steps))
	for _, requestedStep := range request.Steps {
		input := requestedStep.Input
		if len(input) == 0 {
			input = json.RawMessage(`{}`)
		}
		status := domain.StepReady
		if len(requestedStep.DependsOn) > 0 {
			status = domain.StepBlocked
		}
		operationKey := workflow.ID + ":" + requestedStep.Key
		var stepID string
		err = tx.QueryRow(ctx, `
			INSERT INTO steps (workflow_id, operation_key, step_key, kind, input, status,
			                   max_attempts, retry_backoff_ms, timeout_ms)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id::text`,
			workflow.ID, operationKey, requestedStep.Key, requestedStep.Kind, input, status,
			requestedStep.ResolvedMaxAttempts(), requestedStep.ResolvedRetryBackoff().Milliseconds(), requestedStep.ResolvedTimeout().Milliseconds(),
		).Scan(&stepID)
		if err != nil {
			return domain.Workflow{}, false, fmt.Errorf("insert step %q: %w", requestedStep.Key, err)
		}
		stepIDs[requestedStep.Key] = stepID
		eventType := "step.ready"
		if status == domain.StepBlocked {
			eventType = "step.blocked"
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, event_type, payload)
			VALUES ($1::uuid, $2::uuid, $3, jsonb_build_object('step_key', $4::text))`,
			workflow.ID, stepID, eventType, requestedStep.Key); err != nil {
			return domain.Workflow{}, false, fmt.Errorf("append step submission event: %w", err)
		}
	}
	for _, requestedStep := range request.Steps {
		for _, dependency := range requestedStep.DependsOn {
			if _, err := tx.Exec(ctx, `INSERT INTO step_dependencies (step_id, depends_on_step_id)
				VALUES ($1::uuid, $2::uuid)`, stepIDs[requestedStep.Key], stepIDs[dependency]); err != nil {
				return domain.Workflow{}, false, fmt.Errorf("insert dependency %s -> %s: %w", dependency, requestedStep.Key, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Workflow{}, false, fmt.Errorf("commit workflow: %w", err)
	}
	created, err := s.GetWorkflow(ctx, workflow.ID)
	return created, true, err
}

func (s *Store) GetWorkflowByIdempotencyKey(ctx context.Context, key string) (domain.Workflow, error) {
	var id string
	if err := s.pool.QueryRow(ctx, `SELECT id::text FROM workflows WHERE idempotency_key = $1`, key).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Workflow{}, ErrNotFound
		}
		return domain.Workflow{}, fmt.Errorf("find workflow by idempotency key: %w", err)
	}
	return s.GetWorkflow(ctx, id)
}

func (s *Store) GetWorkflow(ctx context.Context, id string) (domain.Workflow, error) {
	var workflow domain.Workflow
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, idempotency_key, name, status, created_at, updated_at, completed_at
		FROM workflows WHERE id = $1::uuid`, id,
	).Scan(&workflow.ID, &workflow.IdempotencyKey, &workflow.Name, &workflow.Status, &workflow.CreatedAt, &workflow.UpdatedAt, &workflow.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Workflow{}, ErrNotFound
	}
	if err != nil {
		return domain.Workflow{}, fmt.Errorf("get workflow: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id::text, workflow_id::text, operation_key, step_key, kind, input, output, status, attempt_count,
		       max_attempts, retry_backoff_ms, timeout_ms,
		       worker_id, error, created_at, updated_at
		FROM steps WHERE workflow_id = $1::uuid ORDER BY created_at, step_key`, id)
	if err != nil {
		return domain.Workflow{}, fmt.Errorf("list steps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var step domain.Step
		var input, output []byte
		if err := rows.Scan(&step.ID, &step.WorkflowID, &step.OperationKey, &step.Key, &step.Kind, &input, &output, &step.Status,
			&step.AttemptCount, &step.MaxAttempts, &step.RetryBackoffMS, &step.TimeoutMS,
			&step.WorkerID, &step.Error, &step.CreatedAt, &step.UpdatedAt); err != nil {
			return domain.Workflow{}, fmt.Errorf("scan step: %w", err)
		}
		step.Input = json.RawMessage(input)
		if output != nil {
			step.Output = json.RawMessage(output)
		}
		workflow.Steps = append(workflow.Steps, step)
	}
	if err := rows.Err(); err != nil {
		return domain.Workflow{}, fmt.Errorf("iterate steps: %w", err)
	}
	dependencyRows, err := s.pool.Query(ctx, `
		SELECT child.step_key, parent.step_key
		FROM step_dependencies d
		JOIN steps child ON child.id = d.step_id
		JOIN steps parent ON parent.id = d.depends_on_step_id
		WHERE child.workflow_id = $1::uuid ORDER BY child.step_key, parent.step_key`, id)
	if err != nil {
		return domain.Workflow{}, fmt.Errorf("list dependencies: %w", err)
	}
	defer dependencyRows.Close()
	dependencies := make(map[string][]string)
	for dependencyRows.Next() {
		var child, parent string
		if err := dependencyRows.Scan(&child, &parent); err != nil {
			return domain.Workflow{}, fmt.Errorf("scan dependency: %w", err)
		}
		dependencies[child] = append(dependencies[child], parent)
	}
	for index := range workflow.Steps {
		workflow.Steps[index].DependsOn = dependencies[workflow.Steps[index].Key]
	}
	return workflow, dependencyRows.Err()
}

func (s *Store) ClaimStep(ctx context.Context, workerID string, leaseDuration time.Duration) (*domain.ClaimedStep, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var claim domain.ClaimedStep
	var input []byte
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id FROM steps
			WHERE status = 'ready' AND available_at <= now()
			ORDER BY available_at, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE steps AS s
		SET status = 'running', worker_id = $1, lease_token = gen_random_uuid(),
		    lease_expires_at = now() + $2::interval, attempt_count = attempt_count + 1,
		    started_at = COALESCE(started_at, now()), updated_at = now()
		FROM candidate
		WHERE s.id = candidate.id
		RETURNING s.id::text, s.workflow_id::text, s.operation_key, s.step_key, s.kind, s.input, s.status,
		          s.attempt_count, s.max_attempts, s.retry_backoff_ms, s.timeout_ms,
		          s.worker_id, s.created_at, s.updated_at, s.lease_token::text`,
		workerID, leaseDuration.String(),
	).Scan(&claim.ID, &claim.WorkflowID, &claim.OperationKey, &claim.Key, &claim.Kind, &input, &claim.Status, &claim.AttemptCount,
		&claim.MaxAttempts, &claim.RetryBackoffMS, &claim.TimeoutMS,
		&claim.WorkerID, &claim.CreatedAt, &claim.UpdatedAt, &claim.LeaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim step: %w", err)
	}
	claim.Input = json.RawMessage(input)
	claim.AttemptNo = claim.AttemptCount
	claim.RetryBackoff = time.Duration(claim.RetryBackoffMS) * time.Millisecond
	claim.Timeout = time.Duration(claim.TimeoutMS) * time.Millisecond

	err = tx.QueryRow(ctx, `
		INSERT INTO attempts (step_id, attempt_no, worker_id, lease_token, status)
		VALUES ($1::uuid, $2, $3, $4::uuid, 'running') RETURNING id::text`,
		claim.ID, claim.AttemptNo, workerID, claim.LeaseToken,
	).Scan(&claim.AttemptID)
	if err != nil {
		return nil, fmt.Errorf("insert attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE workflows SET status = 'running', updated_at = now()
		WHERE id = $1::uuid AND status = 'pending'`, claim.WorkflowID); err != nil {
		return nil, fmt.Errorf("mark workflow running: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, attempt_id, event_type, payload)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'step.claimed', jsonb_build_object('worker_id', $4::text, 'attempt_no', $5::integer))`,
		claim.WorkflowID, claim.ID, claim.AttemptID, workerID, claim.AttemptNo); err != nil {
		return nil, fmt.Errorf("record claim: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return &claim, nil
}

func (s *Store) CompleteStep(ctx context.Context, claim domain.ClaimedStep, output json.RawMessage) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin completion: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT 1 FROM workflows WHERE id = $1::uuid FOR UPDATE`, claim.WorkflowID); err != nil {
		return fmt.Errorf("lock workflow for completion: %w", err)
	}

	result, err := tx.Exec(ctx, `
		UPDATE steps SET status = 'succeeded', output = $1, worker_id = NULL, lease_token = NULL,
		lease_expires_at = NULL, completed_at = now(), updated_at = now()
		WHERE id = $2::uuid AND status = 'running' AND lease_token = $3::uuid
		  AND lease_expires_at > now()`, output, claim.ID, claim.LeaseToken)
	if err != nil {
		return fmt.Errorf("complete step: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, `UPDATE attempts SET status = 'succeeded', finished_at = now() WHERE id = $1::uuid`, claim.AttemptID); err != nil {
		return fmt.Errorf("complete attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, attempt_id, event_type, payload)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'step.succeeded', jsonb_build_object('attempt_no', $4::integer))`,
		claim.WorkflowID, claim.ID, claim.AttemptID, claim.AttemptNo); err != nil {
		return fmt.Errorf("record completion: %w", err)
	}
	released, err := tx.Query(ctx, `
		UPDATE steps AS child SET status = 'ready', available_at = now(), updated_at = now()
		WHERE child.workflow_id = $1::uuid AND child.status = 'blocked'
		  AND NOT EXISTS (
			SELECT 1 FROM step_dependencies d
			JOIN steps dependency ON dependency.id = d.depends_on_step_id
			WHERE d.step_id = child.id AND dependency.status <> 'succeeded'
		  )
		RETURNING child.id::text, child.step_key`, claim.WorkflowID)
	if err != nil {
		return fmt.Errorf("release dependent steps: %w", err)
	}
	type releasedStep struct{ id, key string }
	var releasedSteps []releasedStep
	for released.Next() {
		var stepID, stepKey string
		if err := released.Scan(&stepID, &stepKey); err != nil {
			released.Close()
			return fmt.Errorf("scan released step: %w", err)
		}
		releasedSteps = append(releasedSteps, releasedStep{id: stepID, key: stepKey})
	}
	if err := released.Err(); err != nil {
		released.Close()
		return fmt.Errorf("iterate released steps: %w", err)
	}
	released.Close()
	for _, releasedStep := range releasedSteps {
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, event_type, payload)
			VALUES ($1::uuid, $2::uuid, 'step.ready', jsonb_build_object('step_key', $3::text, 'released_by_step_id', $4::text))`,
			claim.WorkflowID, releasedStep.id, releasedStep.key, claim.ID); err != nil {
			return fmt.Errorf("record dependency release: %w", err)
		}
	}
	var remaining int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM steps WHERE workflow_id = $1::uuid AND status <> 'succeeded'`, claim.WorkflowID).Scan(&remaining); err != nil {
		return fmt.Errorf("count unfinished steps: %w", err)
	}
	if remaining == 0 {
		result, err := tx.Exec(ctx, `UPDATE workflows SET status = 'succeeded', completed_at = now(), updated_at = now()
			WHERE id = $1::uuid AND status IN ('pending', 'running')`, claim.WorkflowID)
		if err != nil {
			return fmt.Errorf("complete workflow: %w", err)
		}
		if result.RowsAffected() == 1 {
			if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, event_type, payload)
				VALUES ($1::uuid, 'workflow.succeeded', '{}'::jsonb)`, claim.WorkflowID); err != nil {
				return fmt.Errorf("record workflow completion: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit completion: %w", err)
	}
	return nil
}

func (s *Store) FailStep(ctx context.Context, claim domain.ClaimedStep, failure domain.StepFailure) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin failure: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT 1 FROM workflows WHERE id = $1::uuid FOR UPDATE`, claim.WorkflowID); err != nil {
		return fmt.Errorf("lock workflow for failure: %w", err)
	}
	message := failure.Message
	if message == "" {
		message = string(failure.Type)
	}
	retryable := failure.Type == domain.FailureRetryable || failure.Type == domain.FailureTimeout
	attemptStatus := "failed"
	if failure.Type == domain.FailureTimeout {
		attemptStatus = "timed_out"
	}
	if retryable && claim.AttemptNo < claim.MaxAttempts {
		backoff := claim.RetryBackoff * time.Duration(1<<uint(claim.AttemptNo-1))
		if backoff > time.Hour {
			backoff = time.Hour
		}
		result, err := tx.Exec(ctx, `
			UPDATE steps SET status = 'ready', error = $1, worker_id = NULL, lease_token = NULL,
			lease_expires_at = NULL, available_at = now() + $2::interval, updated_at = now()
			WHERE id = $3::uuid AND status = 'running' AND lease_token = $4::uuid
			  AND lease_expires_at > now()`, message, backoff.String(), claim.ID, claim.LeaseToken)
		if err != nil {
			return fmt.Errorf("schedule retry: %w", err)
		}
		if result.RowsAffected() != 1 {
			return ErrLeaseLost
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET status = $1, error = $2, finished_at = now()
			WHERE id = $3::uuid AND status = 'running'`, attemptStatus, message, claim.AttemptID); err != nil {
			return fmt.Errorf("finish retryable attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, attempt_id, event_type, payload)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'step.retry_scheduled',
			jsonb_build_object('attempt_no', $4::integer, 'failure_type', $5::text, 'error', $6::text, 'backoff_ms', $7::bigint))`,
			claim.WorkflowID, claim.ID, claim.AttemptID, claim.AttemptNo, string(failure.Type), message, backoff.Milliseconds()); err != nil {
			return fmt.Errorf("record retry: %w", err)
		}
		return tx.Commit(ctx)
	}
	result, err := tx.Exec(ctx, `
		UPDATE steps SET status = 'failed', error = $1, worker_id = NULL, lease_token = NULL,
		lease_expires_at = NULL, completed_at = now(), updated_at = now()
		WHERE id = $2::uuid AND status = 'running' AND lease_token = $3::uuid
		  AND lease_expires_at > now()`, message, claim.ID, claim.LeaseToken)
	if err != nil {
		return fmt.Errorf("fail step: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, `UPDATE attempts SET status = $1, error = $2, finished_at = now()
		WHERE id = $3::uuid AND status = 'running'`, attemptStatus, message, claim.AttemptID); err != nil {
		return fmt.Errorf("fail attempt: %w", err)
	}
	if err := cancelActiveWork(ctx, tx, claim.WorkflowID, claim.ID, "workflow failed"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflows SET status = 'failed', completed_at = now(), updated_at = now()
		WHERE id = $1::uuid AND status IN ('pending', 'running')`, claim.WorkflowID); err != nil {
		return fmt.Errorf("fail workflow: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, attempt_id, event_type, payload)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'step.failed', jsonb_build_object('error', $4::text, 'failure_type', $5::text)),
		       ($1::uuid, NULL, NULL, 'workflow.failed', jsonb_build_object('failed_step_id', $2::text, 'failure_type', $5::text))`,
		claim.WorkflowID, claim.ID, claim.AttemptID, message, string(failure.Type)); err != nil {
		return fmt.Errorf("record failure: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *Store) Heartbeat(ctx context.Context, claim domain.ClaimedStep, leaseDuration time.Duration) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE steps
		SET lease_expires_at = now() + $1::interval, updated_at = now()
		WHERE id = $2::uuid AND status = 'running' AND worker_id = $3
		  AND lease_token = $4::uuid AND lease_expires_at > now()`,
		leaseDuration.String(), claim.ID, valueOrEmpty(claim.WorkerID), claim.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("renew lease: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

func (s *Store) ReclaimExpiredSteps(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin lease reclamation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	type expiredLease struct {
		stepID      string
		workflowID  string
		attemptID   string
		attemptNo   int
		workerID    string
		maxAttempts int
	}
	rows, err := tx.Query(ctx, `
		SELECT s.id::text, s.workflow_id::text, a.id::text, a.attempt_no, a.worker_id, s.max_attempts
		FROM steps s
		JOIN attempts a ON a.step_id = s.id AND a.status = 'running' AND a.lease_token = s.lease_token
		WHERE s.status = 'running' AND s.lease_expires_at <= now()
		ORDER BY s.lease_expires_at, s.id
		FOR UPDATE OF s SKIP LOCKED
		LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("select expired leases: %w", err)
	}
	var expired []expiredLease
	for rows.Next() {
		var lease expiredLease
		if err := rows.Scan(&lease.stepID, &lease.workflowID, &lease.attemptID, &lease.attemptNo, &lease.workerID, &lease.maxAttempts); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired lease: %w", err)
		}
		expired = append(expired, lease)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate expired leases: %w", err)
	}
	rows.Close()

	processed := 0
	for _, lease := range expired {
		result, err := tx.Exec(ctx, `UPDATE attempts SET status = 'lease_expired', finished_at = now(), error = 'worker lease expired'
			WHERE id = $1::uuid AND status = 'running'`, lease.attemptID)
		if err != nil {
			return 0, fmt.Errorf("expire attempt: %w", err)
		}
		if result.RowsAffected() != 1 {
			continue
		}
		processed++
		terminal := lease.attemptNo >= lease.maxAttempts
		if terminal {
			if _, err := tx.Exec(ctx, `UPDATE steps SET status = 'failed', error = 'worker lease expired', worker_id = NULL,
				lease_token = NULL, lease_expires_at = NULL, completed_at = now(), updated_at = now()
				WHERE id = $1::uuid AND status = 'running'`, lease.stepID); err != nil {
				return 0, fmt.Errorf("fail expired step: %w", err)
			}
			if err := cancelActiveWork(ctx, tx, lease.workflowID, lease.stepID, "workflow failed"); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `UPDATE workflows SET status = 'failed', completed_at = now(), updated_at = now()
				WHERE id = $1::uuid AND status IN ('pending', 'running')`, lease.workflowID); err != nil {
				return 0, fmt.Errorf("fail workflow after lease exhaustion: %w", err)
			}
		} else if _, err := tx.Exec(ctx, `UPDATE steps SET status = 'ready', worker_id = NULL, lease_token = NULL,
			lease_expires_at = NULL, available_at = now(), updated_at = now()
			WHERE id = $1::uuid AND status = 'running'`, lease.stepID); err != nil {
			return 0, fmt.Errorf("release expired step: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, attempt_id, event_type, payload)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'step.lease_expired',
			jsonb_build_object('worker_id', $4::text, 'attempt_no', $5::integer))`,
			lease.workflowID, lease.stepID, lease.attemptID, lease.workerID, lease.attemptNo); err != nil {
			return 0, fmt.Errorf("record expired lease: %w", err)
		}
		if terminal {
			if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, attempt_id, event_type, payload)
				VALUES ($1::uuid, $2::uuid, $3::uuid, 'step.failed', jsonb_build_object('error', 'worker lease expired', 'failure_type', 'lease_expired')),
				       ($1::uuid, NULL, NULL, 'workflow.failed', jsonb_build_object('failed_step_id', $2::text, 'failure_type', 'lease_expired'))`,
				lease.workflowID, lease.stepID, lease.attemptID); err != nil {
				return 0, fmt.Errorf("record exhausted lease: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit lease reclamation: %w", err)
	}
	return processed, nil
}

func (s *Store) CancelWorkflow(ctx context.Context, id string) (domain.Workflow, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Workflow{}, fmt.Errorf("begin cancellation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM workflows WHERE id = $1::uuid FOR UPDATE`, id).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Workflow{}, ErrNotFound
		}
		return domain.Workflow{}, fmt.Errorf("lock workflow for cancellation: %w", err)
	}
	if status == domain.WorkflowCancelled {
		if err := tx.Commit(ctx); err != nil {
			return domain.Workflow{}, fmt.Errorf("commit repeated cancellation: %w", err)
		}
		return s.GetWorkflow(ctx, id)
	}
	if status == domain.WorkflowSucceeded || status == domain.WorkflowFailed {
		return domain.Workflow{}, ErrInvalidTransition
	}
	if err := cancelActiveWork(ctx, tx, id, "", "workflow cancelled"); err != nil {
		return domain.Workflow{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflows SET status = 'cancelled', completed_at = now(), updated_at = now()
		WHERE id = $1::uuid`, id); err != nil {
		return domain.Workflow{}, fmt.Errorf("cancel workflow: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, event_type, payload)
		VALUES ($1::uuid, 'workflow.cancelled', '{}'::jsonb)`, id); err != nil {
		return domain.Workflow{}, fmt.Errorf("record cancellation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Workflow{}, fmt.Errorf("commit cancellation: %w", err)
	}
	return s.GetWorkflow(ctx, id)
}

func (s *Store) ListEvents(ctx context.Context, workflowID string, afterID int64, limit int) ([]domain.Event, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM workflows WHERE id = $1::uuid)`, workflowID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check workflow for events: %w", err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT id, workflow_id::text, step_id::text, attempt_id::text,
		event_type, payload, created_at FROM workflow_events
		WHERE workflow_id = $1::uuid AND id > $2 ORDER BY id LIMIT $3`, workflowID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list workflow events: %w", err)
	}
	defer rows.Close()
	var events []domain.Event
	for rows.Next() {
		var event domain.Event
		var payload []byte
		if err := rows.Scan(&event.ID, &event.WorkflowID, &event.StepID, &event.AttemptID, &event.Type, &payload, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan workflow event: %w", err)
		}
		event.Payload = json.RawMessage(payload)
		events = append(events, event)
	}
	return events, rows.Err()
}

func cancelActiveWork(ctx context.Context, tx pgx.Tx, workflowID, exceptStepID, reason string) error {
	var except any
	if exceptStepID != "" {
		except = exceptStepID
	}
	if _, err := tx.Exec(ctx, `UPDATE attempts SET status = 'cancelled', finished_at = now(), error = 'workflow stopped'
		WHERE status = 'running' AND step_id IN (
			SELECT id FROM steps WHERE workflow_id = $1::uuid AND ($2::uuid IS NULL OR id <> $2::uuid)
		)`, workflowID, except); err != nil {
		return fmt.Errorf("cancel active attempts: %w", err)
	}
	rows, err := tx.Query(ctx, `UPDATE steps SET status = 'cancelled', worker_id = NULL, lease_token = NULL,
		lease_expires_at = NULL, completed_at = now(), updated_at = now()
		WHERE workflow_id = $1::uuid AND status IN ('blocked', 'ready', 'running')
		  AND ($2::uuid IS NULL OR id <> $2::uuid)
		RETURNING id::text, step_key`, workflowID, except)
	if err != nil {
		return fmt.Errorf("cancel remaining steps: %w", err)
	}
	type cancelledStep struct{ id, key string }
	var cancelled []cancelledStep
	for rows.Next() {
		var step cancelledStep
		if err := rows.Scan(&step.id, &step.key); err != nil {
			rows.Close()
			return fmt.Errorf("scan cancelled step: %w", err)
		}
		cancelled = append(cancelled, step)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate cancelled steps: %w", err)
	}
	rows.Close()
	for _, step := range cancelled {
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_events (workflow_id, step_id, event_type, payload)
			VALUES ($1::uuid, $2::uuid, 'step.cancelled', jsonb_build_object('step_key', $3::text, 'reason', $4::text))`,
			workflowID, step.id, step.key, reason); err != nil {
			return fmt.Errorf("record step cancellation: %w", err)
		}
	}
	return nil
}

func hashDefinition(request domain.SubmitWorkflowRequest) (string, error) {
	type canonicalStep struct {
		Key            string          `json:"key"`
		Kind           string          `json:"kind"`
		Input          json.RawMessage `json:"input"`
		DependsOn      []string        `json:"depends_on,omitempty"`
		MaxAttempts    int             `json:"max_attempts"`
		RetryBackoffMS int64           `json:"retry_backoff_ms"`
		TimeoutMS      int64           `json:"timeout_ms"`
	}
	type canonicalWorkflow struct {
		Name  string          `json:"name"`
		Steps []canonicalStep `json:"steps"`
	}
	canonical := canonicalWorkflow{Name: request.Name, Steps: make([]canonicalStep, 0, len(request.Steps))}
	for _, step := range request.Steps {
		input := step.Input
		if len(input) == 0 {
			input = json.RawMessage(`{}`)
		}
		var decoded any
		if err := json.Unmarshal(input, &decoded); err != nil {
			return "", err
		}
		input, err := json.Marshal(decoded)
		if err != nil {
			return "", err
		}
		dependencies := append([]string(nil), step.DependsOn...)
		sort.Strings(dependencies)
		canonical.Steps = append(canonical.Steps, canonicalStep{
			Key: step.Key, Kind: step.Kind, Input: input, DependsOn: dependencies,
			MaxAttempts: step.ResolvedMaxAttempts(), RetryBackoffMS: step.ResolvedRetryBackoff().Milliseconds(),
			TimeoutMS: step.ResolvedTimeout().Milliseconds(),
		})
	}
	sort.Slice(canonical.Steps, func(i, j int) bool { return canonical.Steps[i].Key < canonical.Steps[j].Key })
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", sum), nil
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
