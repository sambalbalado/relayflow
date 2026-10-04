package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSubmitWorkflowRequestValidate(t *testing.T) {
	valid := SubmitWorkflowRequest{
		IdempotencyKey: "demo-1",
		Name:           "demo",
		Steps:          []StepRequest{{Key: "hello", Kind: "echo", Input: json.RawMessage(`{"message":"hello"}`)}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*SubmitWorkflowRequest)
		want   string
	}{
		{"missing idempotency key", func(r *SubmitWorkflowRequest) { r.IdempotencyKey = "" }, "idempotency_key"},
		{"missing name", func(r *SubmitWorkflowRequest) { r.Name = "" }, "name is required"},
		{"zero steps", func(r *SubmitWorkflowRequest) { r.Steps = nil }, "between 1 and 100"},
		{"unsupported kind", func(r *SubmitWorkflowRequest) { r.Steps[0].Kind = "shell" }, "unsupported"},
		{"invalid json", func(r *SubmitWorkflowRequest) { r.Steps[0].Input = json.RawMessage(`{`) }, "valid JSON"},
		{"unsafe idempotency key", func(r *SubmitWorkflowRequest) { r.IdempotencyKey = "bad key" }, "contain only"},
		{"unsafe step key", func(r *SubmitWorkflowRequest) { r.Steps[0].Key = "bad key" }, "contain only"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := valid
			candidate.Steps = append([]StepRequest(nil), valid.Steps...)
			tt.mutate(&candidate)
			err := candidate.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestSubmitWorkflowRequestValidatesDAG(t *testing.T) {
	valid := SubmitWorkflowRequest{IdempotencyKey: "dag-1", Name: "dag", Steps: []StepRequest{
		{Key: "root", Kind: "noop"},
		{Key: "left", Kind: "noop", DependsOn: []string{"root"}},
		{Key: "right", Kind: "noop", DependsOn: []string{"root"}},
		{Key: "join", Kind: "echo", DependsOn: []string{"left", "right"}},
	}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid DAG rejected: %v", err)
	}

	tests := []struct {
		name  string
		steps []StepRequest
		want  string
	}{
		{"cycle", []StepRequest{{Key: "a", Kind: "noop", DependsOn: []string{"b"}}, {Key: "b", Kind: "noop", DependsOn: []string{"a"}}}, "cycle"},
		{"unknown dependency", []StepRequest{{Key: "a", Kind: "noop", DependsOn: []string{"missing"}}}, "unknown"},
		{"self dependency", []StepRequest{{Key: "a", Kind: "noop", DependsOn: []string{"a"}}}, "itself"},
		{"duplicate dependency", []StepRequest{{Key: "a", Kind: "noop"}, {Key: "b", Kind: "noop", DependsOn: []string{"a", "a"}}}, "repeats"},
		{"duplicate key", []StepRequest{{Key: "a", Kind: "noop"}, {Key: "a", Kind: "noop"}}, "duplicate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := SubmitWorkflowRequest{IdempotencyKey: "invalid", Name: "invalid", Steps: tt.steps}
			if err := request.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestStepPolicyValidation(t *testing.T) {
	tests := []struct {
		name string
		step StepRequest
		want string
	}{
		{"too many attempts", StepRequest{Key: "a", Kind: "noop", Retry: RetryPolicy{MaxAttempts: 11}}, "max_attempts"},
		{"invalid backoff", StepRequest{Key: "a", Kind: "noop", Retry: RetryPolicy{InitialBackoff: "instant"}}, "initial_backoff"},
		{"invalid timeout", StepRequest{Key: "a", Kind: "noop", Timeout: "10m"}, "timeout"},
		{"invalid sleep input", StepRequest{Key: "a", Kind: "sleep", Input: json.RawMessage(`{"duration":"6m"}`)}, "sleep duration"},
		{"unknown sleep field", StepRequest{Key: "a", Kind: "sleep", Input: json.RawMessage(`{"duration":"1s","command":"oops"}`)}, "unknown field"},
		{"invalid flaky input", StepRequest{Key: "a", Kind: "flaky", Input: json.RawMessage(`{"fail_attempts":10}`)}, "fail_attempts"},
		{"non-object noop input", StepRequest{Key: "a", Kind: "noop", Input: json.RawMessage(`null`)}, "JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := SubmitWorkflowRequest{IdempotencyKey: "policy", Name: "policy", Steps: []StepRequest{tt.step}}
			if err := request.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestSubmitWorkflowRequestAllowsSleep(t *testing.T) {
	request := SubmitWorkflowRequest{
		IdempotencyKey: "sleep-1", Name: "sleep",
		Steps: []StepRequest{{Key: "wait", Kind: "sleep", Input: json.RawMessage(`{"duration":"1s"}`)}},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("sleep request rejected: %v", err)
	}
}
