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

func TestReserve_NewTenantOverLimitIsRejected(t *testing.T) {
	// Regression for the missing-row condition bug: attribute_not_exists(used)
	// alone always passes, letting a brand-new tenant blow past the limit on
	// their very first request. DynamoDB enforces the condition server-side,
	// so simulate that here by returning ConditionalCheckFailedException,
	// which is what a correct condition expression produces for this case.
	fake := &fakeBudgetAPI{updateErr: &types.ConditionalCheckFailedException{}}
	store := &dynamoStore{client: fake, budgetsTable: "budgets", reservationsTable: "reservations"}

	_, err := store.Reserve(context.Background(), "new-tenant", "2026-09", 2000, 1000)
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded for new tenant over limit, got %v", err)
	}
	if len(fake.updateInputs) != 1 {
		t.Fatalf("expected exactly one UpdateItem call, got %d", len(fake.updateInputs))
	}
	cond := *fake.updateInputs[0].ConditionExpression
	if !strings.Contains(cond, ":est <= :limit") {
		t.Fatalf("expected condition to bound the missing-row case by :limit, got: %s", cond)
	}
	if _, ok := fake.updateInputs[0].ExpressionAttributeValues[":limit"]; !ok {
		t.Fatalf("expected :limit expression attribute value to be set")
	}
}

func TestReserve_CompensatesBudgetWhenPutItemFails(t *testing.T) {
	fake := &fakeBudgetAPI{putErr: errors.New("dynamo unavailable")}
	store := &dynamoStore{client: fake, budgetsTable: "budgets", reservationsTable: "reservations"}

	_, err := store.Reserve(context.Background(), "tenant-1", "2026-09", 500, 1000)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "recording reservation") {
		t.Fatalf("expected wrapped PutItem error, got: %v", err)
	}
	if len(fake.updateInputs) != 2 {
		t.Fatalf("expected reserve UpdateItem + compensating UpdateItem, got %d calls", len(fake.updateInputs))
	}
	negAttr, ok := fake.updateInputs[1].ExpressionAttributeValues[":negEst"].(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("expected compensating call to use :negEst, got %#v", fake.updateInputs[1].ExpressionAttributeValues)
	}
	if negAttr.Value != "-500" {
		t.Fatalf("expected compensating delta -500, got %s", negAttr.Value)
	}
	if fake.updateInputs[1].ConditionExpression != nil {
		t.Fatalf("expected compensating UpdateItem to be unconditional, got condition %q", *fake.updateInputs[1].ConditionExpression)
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
