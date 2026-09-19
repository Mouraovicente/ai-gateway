package budget

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// ErrBudgetExceeded is returned by Reserve when the tenant's monthly budget
// would be exceeded by the estimated tokens for this request.
var ErrBudgetExceeded = errors.New("budget: monthly token budget exceeded")

// EstimateTokens implements spec assumption S1: a pessimistic pre-call
// estimate of tokens this request will consume, used by Reserve before the
// backend call happens.
func EstimateTokens(promptBytes int, maxTokens int) int {
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	return promptBytes/4 + maxTokens
}

// Store reserves estimated tokens atomically before a backend call, and
// settles the reservation with the real usage afterward.
type Store interface {
	Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error)
	Settle(ctx context.Context, reservation core.Reservation, realTokens int) error
}

// budgetAPI is the subset of *dynamodb.Client this package needs, so tests
// can substitute a fake without a real AWS endpoint. *dynamodb.Client
// satisfies it.
type budgetAPI interface {
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type dynamoStore struct {
	client            budgetAPI
	budgetsTable      string
	reservationsTable string
}

// NewDynamoStore builds a Store backed by the budgets table (pk tenant_id, sk
// period) and a reservations table used to track orphaned reservations (TTL 15m).
func NewDynamoStore(client *dynamodb.Client, budgetsTable, reservationsTable string) Store {
	return &dynamoStore{client: client, budgetsTable: budgetsTable, reservationsTable: reservationsTable}
}

func (s *dynamoStore) Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"tenant_id": tenantID, "period": period})
	if err != nil {
		return core.Reservation{}, fmt.Errorf("budget: marshaling key: %w", err)
	}
	// DynamoDB condition expressions don't support arithmetic (e.g. "used +
	// :est <= :limit" is a ValidationException), so the comparison threshold
	// is precomputed in Go: used <= limit - est.
	exprValues, err := attributevalue.MarshalMap(map[string]any{
		":est":        estimatedTokens,
		":maxAllowed": monthlyLimit - estimatedTokens,
	})
	if err != nil {
		return core.Reservation{}, fmt.Errorf("budget: marshaling expression values: %w", err)
	}

	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.budgetsTable,
		Key:                       key,
		UpdateExpression:          strPtr("ADD used :est"),
		ConditionExpression:       strPtr("attribute_not_exists(used) OR used <= :maxAllowed"),
		ExpressionAttributeValues: exprValues,
	})
	if err != nil {
		var condFailed *types.ConditionalCheckFailedException
		if errors.As(err, &condFailed) {
			return core.Reservation{}, ErrBudgetExceeded
		}
		return core.Reservation{}, fmt.Errorf("budget: reserving tokens: %w", err)
	}

	reservationID := uuid.NewString()
	ttl := time.Now().Add(15 * time.Minute).Unix()
	reservationItem, err := attributevalue.MarshalMap(map[string]any{
		"reservation_id":   reservationID,
		"tenant_id":        tenantID,
		"period":           period,
		"estimated_tokens": estimatedTokens,
		"ttl":              ttl,
	})
	if err != nil {
		return core.Reservation{}, fmt.Errorf("budget: marshaling reservation item: %w", err)
	}
	if _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.reservationsTable, Item: reservationItem}); err != nil {
		return core.Reservation{}, fmt.Errorf("budget: recording reservation: %w", err)
	}

	return core.Reservation{ID: reservationID, TenantID: tenantID, Period: period, EstimatedTokens: estimatedTokens}, nil
}

func (s *dynamoStore) Settle(ctx context.Context, reservation core.Reservation, realTokens int) error {
	delta := realTokens - reservation.EstimatedTokens

	key, err := attributevalue.MarshalMap(map[string]string{"tenant_id": reservation.TenantID, "period": reservation.Period})
	if err != nil {
		return fmt.Errorf("budget: marshaling settle key: %w", err)
	}
	exprValues, err := attributevalue.MarshalMap(map[string]any{":delta": delta})
	if err != nil {
		return fmt.Errorf("budget: marshaling settle expression values: %w", err)
	}
	if _, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.budgetsTable,
		Key:                       key,
		UpdateExpression:          strPtr("ADD used :delta"),
		ExpressionAttributeValues: exprValues,
	}); err != nil {
		return fmt.Errorf("budget: settling tokens: %w", err)
	}

	reservationKey, err := attributevalue.MarshalMap(map[string]string{"reservation_id": reservation.ID})
	if err != nil {
		return fmt.Errorf("budget: marshaling reservation key: %w", err)
	}
	if _, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: &s.reservationsTable, Key: reservationKey}); err != nil {
		return fmt.Errorf("budget: deleting reservation: %w", err)
	}
	return nil
}

func strPtr(s string) *string { return &s }
