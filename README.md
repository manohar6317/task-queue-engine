# Task Queue Engine

A production-grade distributed task queue system built with **Go** and **AWS**, designed for reliable asynchronous job processing at scale.

[![CI/CD](https://github.com/manohar6317/task-queue-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/manohar6317/task-queue-engine/actions)
![Go Version](https://img.shields.io/badge/go-1.22-blue)
![AWS](https://img.shields.io/badge/AWS-SQS%20%7C%20DynamoDB-orange)

---

## Architecture

```
                          ┌─────────────────────────────────────────┐
                          │           Task Queue Engine              │
                          │                                          │
  Client                  │  ┌──────────────┐                       │
    │                     │  │  HTTP API    │                       │
    │  POST /tasks        │  │  (Go server) │                       │
    ├────────────────────►│  └──────┬───────┘                       │
    │                     │         │ 1. Save task                  │
    │  GET /tasks/{id}    │         ▼                               │
    ├────────────────────►│  ┌──────────────┐   2. Enqueue          │
    │                     │  │  DynamoDB    │◄──────────────────┐  │
    │  GET /metrics       │  │  (task store)│                   │  │
    └────────────────────►│  └──────────────┘                   │  │
                          │                          ┌───────────┴──┴────┐
                          │                          │    AWS SQS        │
                          │                          │  ┌─────────────┐  │
                          │                          │  │ Main Queue  │  │
                          │                          │  └──────┬──────┘  │
                          │                          │         │         │
                          │                          │  (after 3 fails)  │
                          │                          │  ┌──────▼──────┐  │
                          │                          │  │  Dead Letter│  │
                          │                          │  │   Queue     │  │
                          │                          │  └─────────────┘  │
                          │                          └────────┬──────────┘
                          │                                   │ 3. Poll (long-polling)
                          │  ┌──────────────────────────────────────────┐
                          │  │           Worker Pool (5 goroutines)      │
                          │  │  ┌────────┐ ┌────────┐ ┌────────┐       │
                          │  │  │Worker 0│ │Worker 1│ │Worker N│  ...  │
                          │  │  └────────┘ └────────┘ └────────┘       │
                          │  │      4. Process task concurrently        │
                          │  │      5. Update status in DynamoDB        │
                          │  │      6. Delete message from SQS          │
                          │  └──────────────────────────────────────────┘
                          └─────────────────────────────────────────────┘
```

---

## Features

- **Concurrent Workers** — configurable pool of goroutines, each polling SQS independently
- **Reliable Delivery** — SQS visibility timeout prevents duplicate processing during failures
- **Auto Retry** — failed tasks are retried up to 3 times before moving to the Dead Letter Queue
- **Dead Letter Queue** — unprocessable messages are isolated for investigation without blocking the main queue
- **Task Persistence** — every task's full lifecycle is tracked in DynamoDB with timestamps
- **Real-time Metrics** — `/metrics` endpoint exposes queue depth, active workers, and task counts
- **Graceful Shutdown** — handles SIGINT/SIGTERM by draining in-flight tasks before exiting
- **Dockerized** — multi-stage build produces a ~15MB production image
- **CI/CD** — GitHub Actions pipeline runs lint, tests (with race detector), and Docker build on every push

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22 |
| Message Queue | AWS SQS |
| Database | AWS DynamoDB |
| Containerization | Docker (multi-stage) |
| CI/CD | GitHub Actions |
| Cloud | AWS (free tier compatible) |

---

## Project Structure

```
task-queue-engine/
├── cmd/
│   └── api/
│       └── main.go              # Entry point — wires up all components
├── internal/
│   ├── api/
│   │   └── handler.go           # HTTP handlers (submit, status, metrics, health)
│   ├── models/
│   │   └── task.go              # Task domain model and request/response types
│   ├── queue/
│   │   └── sqs.go               # SQS enqueue / dequeue / delete operations
│   ├── store/
│   │   └── dynamodb.go          # DynamoDB CRUD and status queries
│   └── worker/
│       └── worker.go            # Concurrent worker pool with retry logic
├── config/
│   └── config.go                # Environment-based configuration loader
├── .github/
│   └── workflows/
│       └── ci.yml               # CI/CD pipeline (lint → test → docker build)
├── setup-aws.sh                 # One-time AWS resource provisioning script
├── Dockerfile                   # Multi-stage Docker build
├── docker-compose.yml           # Local development environment
└── go.mod
```

---

## Getting Started

### Prerequisites

- [Go 1.22+](https://go.dev/dl/)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) configured (`aws configure`)
- [Docker](https://docs.docker.com/get-docker/) (optional)
- AWS account (free tier works)

### 1. Clone the repository

```bash
git clone https://github.com/manohar6317/task-queue-engine.git
cd task-queue-engine
```

### 2. Provision AWS resources

```bash
chmod +x setup-aws.sh
./setup-aws.sh
```

This creates:
- SQS main queue with a 30s visibility timeout
- SQS Dead Letter Queue with a 3-retry redrive policy
- DynamoDB `tasks` table with a GSI on `status`

Copy the output values into a `.env` file.

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with the values from setup-aws.sh output
```

```env
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/task-queue
SQS_DLQ_URL=https://sqs.us-east-1.amazonaws.com/123456789/task-queue-dlq
DYNAMODB_TABLE=tasks
WORKER_COUNT=5
PORT=8080
```

### 4. Run locally

```bash
# Option A: directly with Go
source .env
go run ./cmd/api

# Option B: with Docker
docker-compose up --build
```

---

## API Reference

### Submit a Task
```http
POST /tasks
Content-Type: application/json

{
  "type": "email",
  "payload": "user@example.com"
}
```

**Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "email",
  "payload": "user@example.com",
  "status": "PENDING",
  "retry_count": 0,
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

### Check Task Status
```http
GET /tasks/{id}
```

**Possible status values:** `PENDING` → `PROCESSING` → `COMPLETED` or `FAILED`

### Get System Metrics
```http
GET /metrics
```

```json
{
  "total_tasks": 1500,
  "pending_tasks": 23,
  "completed_tasks": 1460,
  "failed_tasks": 17,
  "active_workers": 4,
  "queue_depth": 23
}
```

### Health Check
```http
GET /health
```

---

## Supported Task Types

| Type | Description |
|---|---|
| `email` | Sends an email notification |
| `image-resize` | Resizes an uploaded image |
| `data-export` | Exports a data set to CSV/JSON |

To add a new task type, add a case to `executeTask()` in `internal/worker/worker.go`.

---

## Key Design Decisions

**Why SQS over a database queue?**  
SQS provides at-least-once delivery guarantees, built-in visibility timeouts, and native dead-letter queue support — without managing a separate queue infrastructure.

**Why DynamoDB?**  
Serverless, pay-per-request pricing, and predictable single-digit millisecond latency make it ideal for task status lookups. The GSI on `status` enables O(1) metric aggregation.

**Why goroutines over threads?**  
Go goroutines are extremely lightweight (~2KB stack vs ~1MB per OS thread). Running 5–50 concurrent workers is trivial in Go.

**Why long polling?**  
SQS long polling (20s) reduces empty receive calls by ~99% compared to short polling, lowering both latency and AWS costs.

---

## License

MIT
