#!/bin/bash
# setup-aws.sh — Creates all required AWS resources for the task queue engine
# Run this once before starting the application.
# Prerequisites: AWS CLI installed and configured (aws configure)

set -e

REGION=${AWS_REGION:-us-east-1}
QUEUE_NAME="task-queue"
DLQ_NAME="task-queue-dlq"
TABLE_NAME="tasks"
MAX_RETRIES=3

echo "==> Setting up AWS resources in region: $REGION"

# ---- 1. Create Dead Letter Queue (must exist before main queue) ----
echo "Creating Dead Letter Queue: $DLQ_NAME"
DLQ_URL=$(aws sqs create-queue \
  --queue-name "$DLQ_NAME" \
  --region "$REGION" \
  --query 'QueueUrl' \
  --output text)

DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --region "$REGION" \
  --query 'Attributes.QueueArn' \
  --output text)

echo "DLQ ARN: $DLQ_ARN"

# ---- 2. Create Main Task Queue with DLQ redrive policy ----
echo "Creating Main Queue: $QUEUE_NAME"
QUEUE_URL=$(aws sqs create-queue \
  --queue-name "$QUEUE_NAME" \
  --region "$REGION" \
  --attributes "{
    \"VisibilityTimeout\": \"30\",
    \"MessageRetentionPeriod\": \"86400\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"$MAX_RETRIES\\\"}\"
  }" \
  --query 'QueueUrl' \
  --output text)

echo "Queue URL: $QUEUE_URL"

# ---- 3. Create DynamoDB table ----
echo "Creating DynamoDB table: $TABLE_NAME"
aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
    AttributeName=id,AttributeType=S \
    AttributeName=status,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --global-secondary-indexes "[
    {
      \"IndexName\": \"status-index\",
      \"KeySchema\": [{\"AttributeName\":\"status\",\"KeyType\":\"HASH\"}],
      \"Projection\": {\"ProjectionType\":\"ALL\"}
    }
  ]" \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" 2>/dev/null || echo "Table already exists — skipping"

echo ""
echo "==> Setup complete! Add these to your .env file:"
echo ""
echo "AWS_REGION=$REGION"
echo "SQS_QUEUE_URL=$QUEUE_URL"
echo "SQS_DLQ_URL=$DLQ_URL"
echo "DYNAMODB_TABLE=$TABLE_NAME"
echo "WORKER_COUNT=5"
echo "PORT=8080"
