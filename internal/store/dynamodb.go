package store

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/manohar6317/task-queue-engine/internal/models"
)

// DynamoStore handles all task persistence operations against DynamoDB
type DynamoStore struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoStore(client *dynamodb.Client, tableName string) *DynamoStore {
	return &DynamoStore{client: client, tableName: tableName}
}

// EnsureTable creates the DynamoDB table if it doesn't already exist.
// Uses a Global Secondary Index on `status` to enable efficient status queries.
func (s *DynamoStore) EnsureTable(ctx context.Context) error {
	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	})
	if err == nil {
		return nil // Table already exists
	}

	_, err = s.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(s.tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("status-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest, // No capacity planning needed
	})
	return err
}

// Save persists a new task to DynamoDB
func (s *DynamoStore) Save(ctx context.Context, task *models.Task) error {
	task.UpdatedAt = time.Now()
	item, err := attributevalue.MarshalMap(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      item,
	})
	return err
}

// Get retrieves a single task by its ID
func (s *DynamoStore) Get(ctx context.Context, taskID string) (*models.Task, error) {
	result, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: taskID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}
	if result.Item == nil {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	var task models.Task
	if err := attributevalue.UnmarshalMap(result.Item, &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}
	return &task, nil
}

// UpdateStatus updates a task's status and optional error message atomically
func (s *DynamoStore) UpdateStatus(ctx context.Context, taskID string, status models.TaskStatus, errMsg string) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: taskID},
		},
		UpdateExpression: aws.String("SET #s = :status, updated_at = :now, #e = :err"),
		ExpressionAttributeNames: map[string]string{
			"#s": "status",
			"#e": "error",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: string(status)},
			":now":    &types.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339)},
			":err":    &types.AttributeValueMemberS{Value: errMsg},
		},
	})
	return err
}

// CountByStatus queries the GSI to count tasks in a given status
func (s *DynamoStore) CountByStatus(ctx context.Context, status models.TaskStatus) (int64, error) {
	result, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.tableName),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("#s = :status"),
		ExpressionAttributeNames: map[string]string{
			"#s": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: string(status)},
		},
		Select: types.SelectCount,
	})
	if err != nil {
		return 0, err
	}
	return int64(result.Count), nil
}
