package budget

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

type fakeBudgetAPI struct {
	updateErr    error
	putErr       error
	deleteErr    error
	updateInputs []*dynamodb.UpdateItemInput
}

func (f *fakeBudgetAPI) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.updateInputs = append(f.updateInputs, params)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func (f *fakeBudgetAPI) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeBudgetAPI) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

func TestReserve_MapsConditionalCheckFailedToErrBudgetExceeded(t *testing.T) {
	fake := &fakeBudgetAPI{updateErr: &types.ConditionalCheckFailedException{}}
	store := &dynamoStore{client: fake, budgetsTable: "budgets", reservationsTable: "reservations"}

	_, err := store.Reserve(context.Background(), "tenant-1", "2026-09", 100, 1000)
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestReserve_WrapsOtherClientErrors(t *testing.T) {
	fake := &fakeBudgetAPI{updateErr: errors.New("boom")}
	store := &dynamoStore{client: fake, budgetsTable: "budgets", reservationsTable: "reservations"}

	_, err := store.Reserve(context.Background(), "tenant-1", "2026-09", 100, 1000)
	if err == nil || errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected wrapped non-budget error, got %v", err)
	}
	if !strings.HasPrefix(err.Error(), "budget:") {
		t.Fatalf("expected error wrapped with 'budget:' prefix, got: %v", err)
	}
	if !errors.Is(err, fake.updateErr) {
		t.Fatalf("expected wrapped error to unwrap to client error, got: %v", err)
	}
}

func TestSettle_DeltaIsRealMinusEstimated(t *testing.T) {
	fake := &fakeBudgetAPI{}
	store := &dynamoStore{client: fake, budgetsTable: "budgets", reservationsTable: "reservations"}
	reservation := core.Reservation{ID: "res-1", TenantID: "tenant-1", Period: "2026-09", EstimatedTokens: 1000}

	if err := store.Settle(context.Background(), reservation, 800); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(fake.updateInputs) != 1 {
		t.Fatalf("expected exactly one UpdateItem call, got %d", len(fake.updateInputs))
	}
	deltaAttr, ok := fake.updateInputs[0].ExpressionAttributeValues[":delta"].(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("expected :delta to be a numeric attribute, got %#v", fake.updateInputs[0].ExpressionAttributeValues[":delta"])
	}
	if deltaAttr.Value != "-200" {
		t.Fatalf("expected delta -200 (real 800 - estimated 1000), got %s", deltaAttr.Value)
	}
}

func TestSettle_WrapsClientErrors(t *testing.T) {
	fake := &fakeBudgetAPI{updateErr: errors.New("boom")}
	store := &dynamoStore{client: fake, budgetsTable: "budgets", reservationsTable: "reservations"}
	reservation := core.Reservation{ID: "res-1", TenantID: "tenant-1", Period: "2026-09", EstimatedTokens: 1000}

	err := store.Settle(context.Background(), reservation, 1200)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.HasPrefix(err.Error(), "budget:") {
		t.Fatalf("expected error wrapped with 'budget:' prefix, got: %v", err)
	}
}
