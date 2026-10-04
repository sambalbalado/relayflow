package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sambalbalado/relayflow/internal/domain"
	"github.com/sambalbalado/relayflow/internal/observability"
	"github.com/sambalbalado/relayflow/internal/store"
)

type WorkflowStore interface {
	Ping(context.Context) error
	CreateWorkflow(context.Context, domain.SubmitWorkflowRequest) (domain.Workflow, bool, error)
	GetWorkflow(context.Context, string) (domain.Workflow, error)
	CancelWorkflow(context.Context, string) (domain.Workflow, error)
	ListEvents(context.Context, string, int64, int) ([]domain.Event, error)
	MetricsSnapshot(context.Context) (observability.Snapshot, error)
}

type Handler struct {
	store  WorkflowStore
	logger *slog.Logger
	mux    *http.ServeMux
}

func NewHandler(workflowStore WorkflowStore, logger *slog.Logger) *Handler {
	handler := &Handler{store: workflowStore, logger: logger, mux: http.NewServeMux()}
	handler.mux.HandleFunc("GET /healthz", handler.health)
	handler.mux.HandleFunc("GET /readyz", handler.ready)
	handler.mux.HandleFunc("GET /metrics", handler.metrics)
	handler.mux.HandleFunc("POST /v1/workflows", handler.submitWorkflow)
	handler.mux.HandleFunc("GET /v1/workflows/{id}", handler.getWorkflow)
	handler.mux.HandleFunc("POST /v1/workflows/{id}/cancel", handler.cancelWorkflow)
	handler.mux.HandleFunc("GET /v1/workflows/{id}/events", handler.listEvents)
	return handler
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	started := time.Now()
	requestID := fmt.Sprintf("%x-%x", started.UnixNano(), requestSequence.Add(1))
	response.Header().Set("X-Request-ID", requestID)
	response.Header().Set("X-Content-Type-Options", "nosniff")
	recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
	h.mux.ServeHTTP(recorder, request)
	pattern := request.Pattern
	if pattern == "" {
		pattern = "unmatched"
	}
	h.logger.Info("http request completed", "request_id", requestID, "method", request.Method,
		"route", pattern, "status", recorder.status, "duration_ms", time.Since(started).Milliseconds())
}

var requestSequence atomic.Uint64

var uuidPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (h *Handler) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ready(response http.ResponseWriter, request *http.Request) {
	if err := h.store.Ping(request.Context()); err != nil {
		writeError(response, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "ready", "checks": map[string]string{"database": "ready"}})
}

func (h *Handler) metrics(response http.ResponseWriter, request *http.Request) {
	snapshot, err := h.store.MetricsSnapshot(request.Context())
	if err != nil {
		h.logger.Error("collect metrics", "error", err)
		writeError(response, http.StatusServiceUnavailable, "metrics unavailable")
		return
	}
	response.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	response.WriteHeader(http.StatusOK)
	if err := observability.WritePrometheus(response, snapshot); err != nil {
		h.logger.Error("write metrics", "error", err)
	}
}

func (h *Handler) submitWorkflow(response http.ResponseWriter, request *http.Request) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(response, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input domain.SubmitWorkflowRequest
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(response, http.StatusRequestEntityTooLarge, "request body exceeds 1 MiB")
			return
		}
		writeError(response, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(response, http.StatusBadRequest, "request body must contain exactly one JSON value")
		return
	}
	if err := input.Validate(); err != nil {
		writeError(response, http.StatusUnprocessableEntity, err.Error())
		return
	}
	workflow, created, err := h.store.CreateWorkflow(request.Context(), input)
	if errors.Is(err, store.ErrIdempotencyConflict) {
		writeError(response, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		h.logger.Error("submit workflow", "error", err)
		writeError(response, http.StatusInternalServerError, "could not persist workflow")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	response.Header().Set("Location", "/v1/workflows/"+workflow.ID)
	writeJSON(response, status, workflow)
}

func (h *Handler) getWorkflow(response http.ResponseWriter, request *http.Request) {
	id, ok := workflowID(response, request)
	if !ok {
		return
	}
	workflow, err := h.store.GetWorkflow(request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(response, http.StatusNotFound, "workflow not found")
		return
	}
	if err != nil {
		h.logger.Error("get workflow", "workflow_id", id, "error", err)
		writeError(response, http.StatusInternalServerError, "could not load workflow")
		return
	}
	writeJSON(response, http.StatusOK, workflow)
}

func (h *Handler) cancelWorkflow(response http.ResponseWriter, request *http.Request) {
	id, ok := workflowID(response, request)
	if !ok {
		return
	}
	workflow, err := h.store.CancelWorkflow(request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(response, http.StatusNotFound, "workflow not found")
		return
	}
	if errors.Is(err, store.ErrInvalidTransition) {
		writeError(response, http.StatusConflict, "terminal workflow cannot be cancelled")
		return
	}
	if err != nil {
		h.logger.Error("cancel workflow", "workflow_id", id, "error", err)
		writeError(response, http.StatusInternalServerError, "could not cancel workflow")
		return
	}
	writeJSON(response, http.StatusOK, workflow)
}

func (h *Handler) listEvents(response http.ResponseWriter, request *http.Request) {
	id, ok := workflowID(response, request)
	if !ok {
		return
	}
	afterID, err := parseInt64Query(request, "after", 0)
	if err != nil || afterID < 0 {
		writeError(response, http.StatusBadRequest, "after must be a non-negative integer")
		return
	}
	limit64, err := parseInt64Query(request, "limit", 50)
	if err != nil || limit64 < 1 || limit64 > 200 {
		writeError(response, http.StatusBadRequest, "limit must be between 1 and 200")
		return
	}
	events, err := h.store.ListEvents(request.Context(), id, afterID, int(limit64))
	if errors.Is(err, store.ErrNotFound) {
		writeError(response, http.StatusNotFound, "workflow not found")
		return
	}
	if err != nil {
		h.logger.Error("list workflow events", "workflow_id", id, "error", err)
		writeError(response, http.StatusInternalServerError, "could not load workflow events")
		return
	}
	if events == nil {
		events = []domain.Event{}
	}
	writeJSON(response, http.StatusOK, map[string]any{"events": events})
}

func workflowID(response http.ResponseWriter, request *http.Request) (string, bool) {
	id := strings.TrimSpace(request.PathValue("id"))
	if !uuidPattern.MatchString(id) {
		writeError(response, http.StatusBadRequest, "workflow id must be a UUID")
		return "", false
	}
	return id, true
}

func parseInt64Query(request *http.Request, name string, fallback int64) (int64, error) {
	value := request.URL.Query().Get(name)
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}
