package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manohar6317/task-queue-engine/internal/models"
	"github.com/manohar6317/task-queue-engine/internal/queue"
	"github.com/manohar6317/task-queue-engine/internal/store"
)

const maxRetries = 3

// Pool manages a fixed set of concurrent workers that process tasks from the queue.
// Uses goroutines (Go's lightweight threads) for true concurrency.
type Pool struct {
	queue         *queue.SQSQueue
	store         *store.DynamoStore
	workerCount   int
	activeWorkers int64 // atomic counter — safe for concurrent reads/writes
	wg            sync.WaitGroup
}

func NewPool(q *queue.SQSQueue, s *store.DynamoStore, workerCount int) *Pool {
	return &Pool{
		queue:       q,
		store:       s,
		workerCount: workerCount,
	}
}

// Start spawns N goroutines, each polling the queue independently
func (p *Pool) Start(ctx context.Context) {
	log.Printf("[WorkerPool] Starting %d workers", p.workerCount)
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i) // goroutine — this is what makes it concurrent
	}
}

// Wait blocks until all workers have stopped (used for graceful shutdown)
func (p *Pool) Wait() {
	p.wg.Wait()
}

// ActiveWorkers returns a snapshot of how many workers are currently processing tasks
func (p *Pool) ActiveWorkers() int64 {
	return atomic.LoadInt64(&p.activeWorkers)
}

// runWorker is the main loop for each worker goroutine
func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	log.Printf("[Worker-%d] Started", id)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled — graceful shutdown
			log.Printf("[Worker-%d] Shutting down", id)
			return
		default:
			p.poll(ctx, id)
		}
	}
}

// poll fetches a batch of messages from SQS and processes each one
func (p *Pool) poll(ctx context.Context, workerID int) {
	tasks, handles, err := p.queue.Dequeue(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // Context cancelled, not an error
		}
		log.Printf("[Worker-%d] Dequeue error: %v — retrying in 5s", workerID, err)
		time.Sleep(5 * time.Second)
		return
	}

	for i, task := range tasks {
		atomic.AddInt64(&p.activeWorkers, 1)
		p.processTask(ctx, task, handles[i], workerID)
		atomic.AddInt64(&p.activeWorkers, -1)
	}
}

// processTask handles the full lifecycle of a single task:
// mark processing → execute → mark complete/failed → delete from queue
func (p *Pool) processTask(ctx context.Context, task *models.Task, receiptHandle string, workerID int) {
	log.Printf("[Worker-%d] Processing task %s (type=%s, retry=%d)", workerID, task.ID, task.Type, task.RetryCount)

	// Mark task as in-progress in DynamoDB
	if err := p.store.UpdateStatus(ctx, task.ID, models.StatusProcessing, ""); err != nil {
		log.Printf("[Worker-%d] Failed to update status to PROCESSING: %v", workerID, err)
	}

	// Execute the actual task logic
	err := executeTask(task)

	if err != nil {
		task.RetryCount++
		if task.RetryCount >= maxRetries {
			// Exhausted retries — mark as permanently failed and delete from queue
			// SQS DLQ redrive policy will have already moved it, but we clean up explicitly
			log.Printf("[Worker-%d] Task %s FAILED permanently after %d retries: %v", workerID, task.ID, maxRetries, err)
			p.store.UpdateStatus(ctx, task.ID, models.StatusFailed, err.Error())
			p.queue.Delete(ctx, receiptHandle)
		} else {
			// Transient failure — don't delete the message.
			// SQS visibility timeout will expire and re-deliver it for retry.
			log.Printf("[Worker-%d] Task %s failed (attempt %d/%d) — will retry", workerID, task.ID, task.RetryCount, maxRetries)
			p.store.UpdateStatus(ctx, task.ID, models.StatusPending, err.Error())
		}
		return
	}

	// Success — update status and remove from queue
	p.store.UpdateStatus(ctx, task.ID, models.StatusCompleted, "")
	p.queue.Delete(ctx, receiptHandle)
	log.Printf("[Worker-%d] Task %s COMPLETED", workerID, task.ID)
}

// executeTask routes task execution based on type.
// In a real system, each handler would call downstream services, APIs, or databases.
func executeTask(task *models.Task) error {
	switch task.Type {
	case "email":
		return processEmailTask(task)
	case "image-resize":
		return processImageResizeTask(task)
	case "data-export":
		return processDataExportTask(task)
	default:
		return fmt.Errorf("unknown task type: %s", task.Type)
	}
}

func processEmailTask(task *models.Task) error {
	log.Printf("[TaskExec] Sending email — payload: %s", task.Payload)
	time.Sleep(100 * time.Millisecond) // Simulated I/O latency
	return nil
}

func processImageResizeTask(task *models.Task) error {
	log.Printf("[TaskExec] Resizing image — payload: %s", task.Payload)
	time.Sleep(200 * time.Millisecond)
	return nil
}

func processDataExportTask(task *models.Task) error {
	log.Printf("[TaskExec] Exporting data — payload: %s", task.Payload)
	time.Sleep(500 * time.Millisecond)
	return nil
}
