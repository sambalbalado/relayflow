package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

var (
	idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)
	stepKeyPattern        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

const (
	WorkflowPending   = "pending"
	WorkflowRunning   = "running"
	WorkflowSucceeded = "succeeded"
	WorkflowFailed    = "failed"
	WorkflowCancelled = "cancelled"

	StepBlocked   = "blocked"
	StepReady     = "ready"
	StepRunning   = "running"
	StepSucceeded = "succeeded"
	StepFailed    = "failed"
	StepCancelled = "cancelled"
)

type SubmitWorkflowRequest struct {
	IdempotencyKey string        `json:"idempotency_key"`
	Name           string        `json:"name"`
	Steps          []StepRequest `json:"steps"`
}

type StepRequest struct {
	Key       string          `json:"key"`
	Kind      string          `json:"kind"`
	Input     json.RawMessage `json:"input"`
	DependsOn []string        `json:"depends_on,omitempty"`
	Retry     RetryPolicy     `json:"retry,omitempty"`
	Timeout   string          `json:"timeout,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts    int    `json:"max_attempts,omitempty"`
	InitialBackoff string `json:"initial_backoff,omitempty"`
}

type Workflow struct {
	ID             string     `json:"id"`
	IdempotencyKey string     `json:"idempotency_key"`
	Name           string     `json:"name"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Steps          []Step     `json:"steps"`
}

type Step struct {
	ID             string          `json:"id"`
	WorkflowID     string          `json:"workflow_id"`
	OperationKey   string          `json:"operation_key"`
	Key            string          `json:"key"`
	Kind           string          `json:"kind"`
	Input          json.RawMessage `json:"input"`
	Output         json.RawMessage `json:"output,omitempty"`
	Status         string          `json:"status"`
	AttemptCount   int             `json:"attempt_count"`
	DependsOn      []string        `json:"depends_on,omitempty"`
	MaxAttempts    int             `json:"max_attempts"`
	RetryBackoffMS int64           `json:"retry_backoff_ms"`
	TimeoutMS      int64           `json:"timeout_ms"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	Error          *string         `json:"error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type ClaimedStep struct {
	Step
	AttemptID    string
	AttemptNo    int
	LeaseToken   string
	RetryBackoff time.Duration
	Timeout      time.Duration
}

type Event struct {
	ID         int64           `json:"id"`
	WorkflowID string          `json:"workflow_id"`
	StepID     *string         `json:"step_id,omitempty"`
	AttemptID  *string         `json:"attempt_id,omitempty"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
}

type FailureType string

const (
	FailureRetryable FailureType = "retryable"
	FailurePermanent FailureType = "permanent"
	FailureTimeout   FailureType = "timeout"
)

type StepFailure struct {
	Type    FailureType
	Message string
}

func (f StepFailure) Error() string { return f.Message }

func (r SubmitWorkflowRequest) Validate() error {
	if strings.TrimSpace(r.IdempotencyKey) == "" {
		return errors.New("idempotency_key is required")
	}
	if len(r.IdempotencyKey) > 200 {
		return errors.New("idempotency_key must be at most 200 characters")
	}
	if !idempotencyKeyPattern.MatchString(r.IdempotencyKey) {
		return errors.New("idempotency_key must start with an alphanumeric character and contain only letters, numbers, '.', '_', ':', '/', or '-'")
	}
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("name is required")
	}
	if len(r.Name) > 200 {
		return errors.New("name must be at most 200 characters")
	}
	if len(r.Steps) == 0 || len(r.Steps) > 100 {
		return errors.New("steps must contain between 1 and 100 entries")
	}
	keys := make(map[string]struct{}, len(r.Steps))
	for _, step := range r.Steps {
		if strings.TrimSpace(step.Key) == "" || len(step.Key) > 100 || !stepKeyPattern.MatchString(step.Key) {
			return errors.New("step key must start with an alphanumeric character, contain only letters, numbers, '.', '_', or '-', and be at most 100 characters")
		}
		if _, exists := keys[step.Key]; exists {
			return fmt.Errorf("duplicate step key %q", step.Key)
		}
		keys[step.Key] = struct{}{}
	}
	graph := make(map[string][]string, len(r.Steps))
	for _, step := range r.Steps {
		if !supportedKind(step.Kind) {
			return fmt.Errorf("unsupported step kind %q", step.Kind)
		}
		if len(step.Input) > 0 && !json.Valid(step.Input) {
			return fmt.Errorf("step %q input must be valid JSON", step.Key)
		}
		if err := validateHandlerInput(step); err != nil {
			return fmt.Errorf("step %q input: %w", step.Key, err)
		}
		if step.Retry.MaxAttempts < 0 || step.Retry.MaxAttempts > 10 {
			return fmt.Errorf("step %q max_attempts must be between 1 and 10 when set", step.Key)
		}
		if step.Retry.InitialBackoff != "" {
			backoff, err := time.ParseDuration(step.Retry.InitialBackoff)
			if err != nil || backoff < time.Millisecond || backoff > time.Hour {
				return fmt.Errorf("step %q initial_backoff must be between 1ms and 1h", step.Key)
			}
		}
		if step.Timeout != "" {
			timeout, err := time.ParseDuration(step.Timeout)
			if err != nil || timeout < time.Millisecond || timeout > 5*time.Minute {
				return fmt.Errorf("step %q timeout must be between 1ms and 5m", step.Key)
			}
		}
		dependencies := make(map[string]struct{}, len(step.DependsOn))
		for _, dependency := range step.DependsOn {
			if dependency == step.Key {
				return fmt.Errorf("step %q cannot depend on itself", step.Key)
			}
			if _, exists := keys[dependency]; !exists {
				return fmt.Errorf("step %q depends on unknown step %q", step.Key, dependency)
			}
			if _, exists := dependencies[dependency]; exists {
				return fmt.Errorf("step %q repeats dependency %q", step.Key, dependency)
			}
			dependencies[dependency] = struct{}{}
			graph[step.Key] = append(graph[step.Key], dependency)
		}
	}
	if hasCycle(graph, keys) {
		return errors.New("workflow graph contains a cycle")
	}
	return nil
}

func validateHandlerInput(step StepRequest) error {
	switch step.Kind {
	case "sleep":
		var input struct {
			Duration string `json:"duration"`
		}
		if err := decodeObject(step.Input, &input); err != nil {
			return err
		}
		duration, err := time.ParseDuration(input.Duration)
		if err != nil || duration <= 0 || duration > 5*time.Minute {
			return errors.New("sleep duration must be greater than zero and at most 5m")
		}
	case "flaky":
		var input struct {
			FailAttempts int `json:"fail_attempts"`
		}
		if err := decodeObject(step.Input, &input); err != nil {
			return err
		}
		if input.FailAttempts < 0 || input.FailAttempts > 9 {
			return errors.New("fail_attempts must be between 0 and 9")
		}
	case "noop", "fail":
		var input struct{}
		if err := decodeObject(step.Input, &input); err != nil {
			return err
		}
	}
	return nil
}

func decodeObject(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("must be a valid object: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("must contain exactly one JSON value")
	}
	return nil
}

func (s StepRequest) ResolvedMaxAttempts() int {
	if s.Retry.MaxAttempts == 0 {
		return 3
	}
	return s.Retry.MaxAttempts
}

func (s StepRequest) ResolvedRetryBackoff() time.Duration {
	if s.Retry.InitialBackoff == "" {
		return 250 * time.Millisecond
	}
	duration, _ := time.ParseDuration(s.Retry.InitialBackoff)
	return duration
}

func (s StepRequest) ResolvedTimeout() time.Duration {
	if s.Timeout == "" {
		return 30 * time.Second
	}
	duration, _ := time.ParseDuration(s.Timeout)
	return duration
}

func supportedKind(kind string) bool {
	switch kind {
	case "echo", "noop", "sleep", "flaky", "fail":
		return true
	default:
		return false
	}
}

func hasCycle(graph map[string][]string, keys map[string]struct{}) bool {
	const (
		visiting = 1
		visited  = 2
	)
	state := make(map[string]int, len(keys))
	var visit func(string) bool
	visit = func(key string) bool {
		if state[key] == visiting {
			return true
		}
		if state[key] == visited {
			return false
		}
		state[key] = visiting
		for _, dependency := range graph[key] {
			if visit(dependency) {
				return true
			}
		}
		state[key] = visited
		return false
	}
	for key := range keys {
		if visit(key) {
			return true
		}
	}
	return false
}
