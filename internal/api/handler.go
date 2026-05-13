package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/manohar6317/task-queue-engine/internal/models"
	"github.com/manohar6317/task-queue-engine/internal/queue"
	"github.com/manohar6317/task-queue-engine/internal/store"
	"github.com/manohar6317/task-queue-engine/internal/worker"
)

// Handler holds all dependencies needed by the HTTP layer
type Handler struct {
	queue      *queue.SQSQueue
	store      *store.DynamoStore
	workerPool *worker.Pool
}

func NewHandler(q *queue.SQSQueue, s *store.DynamoStore, wp *worker.Pool) *Handler {
	return &Handler{queue: q, store: s, workerPool: wp}
}

// SubmitTask handles POST /tasks
// Validates input, persists to DynamoDB, enqueues to SQS
func (h *Handler) SubmitTask(w http.ResponseWriter, r *http.Request) {
	var req models.SubmitTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Type == "" || req.Payload == "" {
		writeError(w, "fields 'type' and 'payload' are required", http.StatusBadRequest)
		return
	}

	task := &models.Task{
		ID:        uuid.New().String(),
		Type:      req.Type,
		Payload:   req.Payload,
		Status:    models.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	// Persist first — so the task is visible even before the worker picks it up
	if err := h.store.Save(r.Context(), task); err != nil {
		log.Printf("[API] Failed to save task: %v", err)
		writeError(w, "failed to save task", http.StatusInternalServerError)
		return
	}

	// Enqueue for async processing
	if err := h.queue.Enqueue(r.Context(), task); err != nil {
		log.Printf("[API] Failed to enqueue task: %v", err)
		writeError(w, "failed to enqueue task", http.StatusInternalServerError)
		return
	}

	log.Printf("[API] Task submitted: %s (type=%s)", task.ID, task.Type)
	writeJSON(w, task, http.StatusCreated)
}

// GetTask handles GET /tasks/{id}
// Returns current task status from DynamoDB
func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if taskID == "" {
		writeError(w, "task ID is required", http.StatusBadRequest)
		return
	}

	task, err := h.store.Get(r.Context(), taskID)
	if err != nil {
		writeError(w, "task not found", http.StatusNotFound)
		return
	}

	writeJSON(w, task, http.StatusOK)
}

// GetMetrics handles GET /metrics
// Returns real-time operational statistics for observability
func (h *Handler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	pending, _ := h.store.CountByStatus(ctx, models.StatusPending)
	completed, _ := h.store.CountByStatus(ctx, models.StatusCompleted)
	failed, _ := h.store.CountByStatus(ctx, models.StatusFailed)
	processing, _ := h.store.CountByStatus(ctx, models.StatusProcessing)
	queueDepth, _ := h.queue.GetQueueDepth(ctx)

	metrics := models.MetricsResponse{
		TotalTasks:     pending + completed + failed + processing,
		PendingTasks:   pending,
		CompletedTasks: completed,
		FailedTasks:    failed,
		ActiveWorkers:  int(h.workerPool.ActiveWorkers()),
		QueueDepth:     queueDepth,
	}

	writeJSON(w, metrics, http.StatusOK)
}

// HealthCheck handles GET /health — used by load balancers and monitoring tools
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}, http.StatusOK)
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, data any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, msg string, status int) {
	writeJSON(w, map[string]string{"error": msg}, status)
}
