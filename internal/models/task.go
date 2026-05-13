package models

import "time"

// TaskStatus represents the lifecycle state of a task
type TaskStatus string

const (
	StatusPending    TaskStatus = "PENDING"
	StatusProcessing TaskStatus = "PROCESSING"
	StatusCompleted  TaskStatus = "COMPLETED"
	StatusFailed     TaskStatus = "FAILED"
)

// Task is the core domain model representing a unit of async work
type Task struct {
	ID         string     `json:"id"          dynamodbav:"id"`
	Type       string     `json:"type"        dynamodbav:"type"`
	Payload    string     `json:"payload"     dynamodbav:"payload"`
	Status     TaskStatus `json:"status"      dynamodbav:"status"`
	RetryCount int        `json:"retry_count" dynamodbav:"retry_count"`
	CreatedAt  time.Time  `json:"created_at"  dynamodbav:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"  dynamodbav:"updated_at"`
	Error      string     `json:"error,omitempty" dynamodbav:"error,omitempty"`
}

// SubmitTaskRequest is the payload for the POST /tasks endpoint
type SubmitTaskRequest struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

// MetricsResponse holds real-time operational statistics
type MetricsResponse struct {
	TotalTasks     int64 `json:"total_tasks"`
	PendingTasks   int64 `json:"pending_tasks"`
	CompletedTasks int64 `json:"completed_tasks"`
	FailedTasks    int64 `json:"failed_tasks"`
	ActiveWorkers  int   `json:"active_workers"`
	QueueDepth     int64 `json:"queue_depth"`
}
