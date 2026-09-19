package trace

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// EventType classifies a trace_event, matching the pipeline stage that emitted it.
type EventType string

const (
	Auth           EventType = "auth"
	RateLimit      EventType = "ratelimit"
	BudgetReserve  EventType = "budget_reserve"
	Route          EventType = "route"
	BackendAttempt EventType = "backend_attempt"
	BackendResult  EventType = "backend_result"
	BudgetSettle   EventType = "budget_settle"
	UsagePublish   EventType = "usage_publish"
	Error          EventType = "error"
)

// Store persists request metadata and the trace_events sequence for a request.
type Store interface {
	RecordRequest(ctx context.Context, requestID, tenantID, alias string) error
	RecordEvent(ctx context.Context, requestID string, seq int, eventType EventType, node string, payload map[string]any) error
}

type dynamoStore struct {
	client        *dynamodb.Client
	requestsTable string
	eventsTable   string
}

// NewDynamoStore builds a Store backed by the requests and trace_events DynamoDB tables.
func NewDynamoStore(client *dynamodb.Client, requestsTable, eventsTable string) Store {
	return &dynamoStore{client: client, requestsTable: requestsTable, eventsTable: eventsTable}
}

type requestItem struct {
	RequestID string `dynamodbav:"request_id"`
	TenantID  string `dynamodbav:"tenant_id"`
	Alias     string `dynamodbav:"alias"`
	CreatedAt string `dynamodbav:"created_at"`
}

func (s *dynamoStore) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error {
	item, err := attributevalue.MarshalMap(requestItem{
		RequestID: requestID,
		TenantID:  tenantID,
		Alias:     alias,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("trace: marshaling request item: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.requestsTable, Item: item})
	if err != nil {
		return fmt.Errorf("trace: writing request item: %w", err)
	}
	return nil
}

type eventItem struct {
	RequestID string `dynamodbav:"request_id"`
	Seq       int    `dynamodbav:"seq"`
	Type      string `dynamodbav:"type"`
	Node      string `dynamodbav:"node"`
	Payload   string `dynamodbav:"payload_json"`
}

func (s *dynamoStore) RecordEvent(ctx context.Context, requestID string, seq int, eventType EventType, node string, payload map[string]any) error {
	for _, forbidden := range ForbiddenKeys {
		if _, ok := payload[forbidden]; ok {
			return fmt.Errorf("trace: payload contains forbidden key %q", forbidden)
		}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("trace: marshaling event payload: %w", err)
	}
	item, err := attributevalue.MarshalMap(eventItem{
		RequestID: requestID,
		Seq:       seq,
		Type:      string(eventType),
		Node:      node,
		Payload:   string(payloadJSON),
	})
	if err != nil {
		return fmt.Errorf("trace: marshaling event item: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.eventsTable, Item: item})
	if err != nil {
		return fmt.Errorf("trace: writing event item: %w", err)
	}
	return nil
}
