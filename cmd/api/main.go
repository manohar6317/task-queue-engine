package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/manohar6317/task-queue-engine/internal/api"
	internalqueue "github.com/manohar6317/task-queue-engine/internal/queue"
	"github.com/manohar6317/task-queue-engine/internal/store"
	"github.com/manohar6317/task-queue-engine/internal/worker"
	appconfig "github.com/manohar6317/task-queue-engine/config"
)

func main() {
	// Load configuration from environment variables
	cfg := appconfig.Load()

	// Initialize AWS SDK with region from config
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.AWSRegion),
	)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	// Wire up AWS clients and app components
	sqsClient := sqs.NewFromConfig(awsCfg)
	dynamoClient := dynamodb.NewFromConfig(awsCfg)

	q := internalqueue.NewSQSQueue(sqsClient, cfg.QueueURL, cfg.DLQUrl)
	s := store.NewDynamoStore(dynamoClient, cfg.TableName)
	wp := worker.NewPool(q, s, cfg.WorkerCount)
	h := api.NewHandler(q, s, wp)

	// Ensure DynamoDB table exists before serving traffic
	if err := s.EnsureTable(context.Background()); err != nil {
		log.Printf("Warning: could not ensure table exists: %v", err)
	}

	// Start worker goroutines — they begin polling SQS immediately
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)

	// Register HTTP routes using Go 1.22's enhanced pattern matching
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", h.SubmitTask)
	mux.HandleFunc("GET /tasks/{id}", h.GetTask)
	mux.HandleFunc("GET /metrics", h.GetMetrics)
	mux.HandleFunc("GET /health", h.HealthCheck)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown — waits for in-flight requests and workers to finish
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("Received signal %s — initiating graceful shutdown", sig)

		cancel() // Stop workers

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
	}()

	log.Printf("Task Queue Engine running on port %s | Workers: %d", cfg.Port, cfg.WorkerCount)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}

	wp.Wait() // Block until all workers drain
	log.Println("Shutdown complete")
}
