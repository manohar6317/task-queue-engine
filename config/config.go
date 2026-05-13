package config

import (
	"log"
	"os"
	"strconv"
)

// Config holds all runtime configuration loaded from environment variables
type Config struct {
	AWSRegion   string
	QueueURL    string
	DLQUrl      string
	TableName   string
	WorkerCount int
	Port        string
}

// Load reads configuration from environment variables with sensible defaults
func Load() *Config {
	workerCount, err := strconv.Atoi(getEnv("WORKER_COUNT", "5"))
	if err != nil {
		log.Fatal("WORKER_COUNT must be a valid integer")
	}

	return &Config{
		AWSRegion:   getEnv("AWS_REGION", "us-east-1"),
		QueueURL:    mustGetEnv("SQS_QUEUE_URL"),
		DLQUrl:      mustGetEnv("SQS_DLQ_URL"),
		TableName:   getEnv("DYNAMODB_TABLE", "tasks"),
		WorkerCount: workerCount,
		Port:        getEnv("PORT", "8080"),
	}
}

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func mustGetEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Required environment variable %s is not set", key)
	}
	return val
}
