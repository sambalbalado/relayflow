package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sambalbalado/relayflow/internal/domain"
	"github.com/sambalbalado/relayflow/internal/observability"
	"github.com/sambalbalado/relayflow/internal/store"
)

type fakeStore struct {
	workflow domain.Workflow
	events   []domain.Event
	created  bool
	err      error
}

func (f fakeStore) Ping(context.Context) error { return f.err }
func (f fakeStore) CreateWorkflow(context.Context, domain.SubmitWorkflowRequest) (domain.Workflow, bool, error) {
	return f.workflow, f.created, f.err
}
func (f fakeStore) GetWorkflow(context.Context, string) (domain.Workflow, error) {
	return f.workflow, f.err
}
func (f fakeStore) CancelWorkflow(context.Context, string) (domain.Workflow, error) {
	return f.workflow, f.err
}
func (f fakeStore) ListEvents(context.Context, string, int64, int) ([]domain.Event, error) {
	return f.events, f.err
}
func (f fakeStore) MetricsSnapshot(context.Context) (observability.Snapshot, error) {
	return observability.Snapshot{Steps: map[string]int64{"ready": 2}, Workflows: map[string]int64{}}, f.err
}

func newTestHandler(workflowStore WorkflowStore) http.Handler {
	return NewHandler(workflowStore, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSubmitWorkflow(t *testing.T) {
	handler := newTestHandler(fakeStore{workflow: domain.Workflow{ID: "workflow-1"}, created: true})
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{
		"idempotency_key":"demo-1","name":"demo","steps":[{"key":"hello","kind":"echo","input":{"message":"hello"}}]
	}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Location"); got != "/v1/workflows/workflow-1" {
		t.Fatalf("Location = %q", got)
	}
}

func TestSubmitWorkflowRejectsUnknownFields(t *testing.T) {
	handler := newTestHandler(fakeStore{})
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{"unknown":true}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSubmitWorkflowRejectsIdempotencyConflict(t *testing.T) {
	handler := newTestHandler(fakeStore{err: store.ErrIdempotencyConflict})
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{
		"idempotency_key":"reused","name":"demo","steps":[{"key":"hello","kind":"echo","input":{}}]
	}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestGetWorkflowNotFound(t *testing.T) {
	handler := newTestHandler(fakeStore{err: store.ErrNotFound})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/workflows/00000000-0000-0000-0000-000000000001", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestReadinessFailure(t *testing.T) {
	handler := newTestHandler(fakeStore{err: errors.New("down")})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestCancelTerminalWorkflowConflicts(t *testing.T) {
	handler := newTestHandler(fakeStore{err: store.ErrInvalidTransition})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflows/00000000-0000-0000-0000-000000000001/cancel", nil))
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestListEventsValidatesPagination(t *testing.T) {
	handler := newTestHandler(fakeStore{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/workflows/00000000-0000-0000-0000-000000000001/events?limit=201", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSubmitWorkflowRequiresJSONContentType(t *testing.T) {
	handler := newTestHandler(fakeStore{})
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSubmitWorkflowRejectsTrailingJSON(t *testing.T) {
	handler := newTestHandler(fakeStore{})
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{} {}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestWorkflowRoutesRejectMalformedUUID(t *testing.T) {
	handler := newTestHandler(fakeStore{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/workflows/not-a-uuid", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestMetricsUsesPrometheusFormat(t *testing.T) {
	handler := newTestHandler(fakeStore{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.Contains(contentType, "version=0.0.4") {
		t.Fatalf("Content-Type = %q", contentType)
	}
	if !strings.Contains(response.Body.String(), `relayflow_steps{status="ready"} 2`) {
		t.Fatalf("body = %s", response.Body.String())
	}
}

func TestResponsesIncludeRequestID(t *testing.T) {
	handler := newTestHandler(fakeStore{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing X-Request-ID")
	}
}
