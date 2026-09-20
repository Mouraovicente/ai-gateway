package trace

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

// MaxPayloadBytes bounds the JSON-marshalled event payload written to
// trace_events. A payload above this size is replaced with a small
// truncation marker instead of failing the request.
const MaxPayloadBytes = 64 << 10

// putItemAPI is the subset of *dynamodb.Client this package needs, so tests
// can substitute a fake without a real AWS endpoint. *dynamodb.Client
// satisfies it.
type putItemAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

type dynamoStore struct {
	client        putItemAPI
	requestsTable string
	eventsTable   string
}

// NewDynamoStore builds a Store backed by the requests and trace_events DynamoDB tables.
func NewDynamoStore(client putItemAPI, requestsTable, eventsTable string) Store {
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

// eventItem validates and marshals one trace_event into its DynamoDB item:
// forbidden keys are rejected outright, and an oversized payload is
// replaced with a truncation marker rather than failing the write.
func (s *dynamoStore) eventItem(requestID string, seq int, eventType EventType, node string, payload map[string]any) (map[string]types.AttributeValue, error) {
	for key := range payload {
		if isForbidden(key) {
			return nil, fmt.Errorf("trace: payload contains forbidden key %q", key)
		}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("trace: marshaling event payload: %w", err)
	}
	if len(payloadJSON) > MaxPayloadBytes {
		payloadJSON, err = json.Marshal(map[string]any{
			"truncated":      true,
			"original_bytes": len(payloadJSON),
		})
		if err != nil {
			return nil, fmt.Errorf("trace: marshaling truncated payload marker: %w", err)
		}
	}
	item, err := attributevalue.MarshalMap(eventItem{
		RequestID: requestID,
		Seq:       seq,
		Type:      string(eventType),
		Node:      node,
		Payload:   string(payloadJSON),
	})
	if err != nil {
		return nil, fmt.Errorf("trace: marshaling event item: %w", err)
	}
	return item, nil
}

func (s *dynamoStore) RecordEvent(ctx context.Context, requestID string, seq int, eventType EventType, node string, payload map[string]any) error {
	item, err := s.eventItem(requestID, seq, eventType, node, payload)
	if err != nil {
		return err
	}
	if _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.eventsTable, Item: item}); err != nil {
		return fmt.Errorf("trace: writing event item: %w", err)
	}
	return nil
}
