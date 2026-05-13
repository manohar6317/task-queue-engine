package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/manohar6317/task-queue-engine/internal/models"
)

// SQSQueue wraps AWS SQS operations for task enqueueing and dequeueing
type SQSQueue struct {
	client   *sqs.Client
	queueURL string
	dlqURL   string
}

func NewSQSQueue(client *sqs.Client, queueURL, dlqURL string) *SQSQueue {
	return &SQSQueue{
		client:   client,
		queueURL: queueURL,
		dlqURL:   dlqURL,
	}
}

// Enqueue serializes a task and sends it to SQS
func (q *SQSQueue) Enqueue(ctx context.Context, task *models.Task) error {
	body, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	_, err = q.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.queueURL),
		MessageBody: aws.String(string(body)),
		// Message attributes allow filtering without deserializing the body
		MessageAttributes: map[string]types.MessageAttributeValue{
			"TaskType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(task.Type),
			},
			"RetryCount": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.Itoa(task.RetryCount)),
			},
		},
	})

	return err
}

// Dequeue uses long-polling (WaitTimeSeconds=20) to efficiently receive messages.
// Long polling reduces empty receives and lowers AWS costs vs short polling.
func (q *SQSQueue) Dequeue(ctx context.Context) ([]*models.Task, []string, error) {
	result, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20, // Long polling — waits up to 20s for messages
		VisibilityTimeout:   30, // Hide message from other workers for 30s
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to receive messages: %w", err)
	}

	var tasks []*models.Task
	var receiptHandles []string

	for _, msg := range result.Messages {
		var task models.Task
		if err := json.Unmarshal([]byte(*msg.Body), &task); err != nil {
			// Skip malformed messages — they'll expire via DLQ redrive policy
			continue
		}
		tasks = append(tasks, &task)
		receiptHandles = append(receiptHandles, *msg.ReceiptHandle)
	}

	return tasks, receiptHandles, nil
}

// Delete removes a successfully processed message from the queue
func (q *SQSQueue) Delete(ctx context.Context, receiptHandle string) error {
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

// GetQueueDepth returns the approximate number of messages waiting to be processed
func (q *SQSQueue) GetQueueDepth(ctx context.Context) (int64, error) {
	result, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		return 0, err
	}

	val := result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	depth, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, nil
	}
	return depth, nil
}
