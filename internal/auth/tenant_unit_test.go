package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type fakeGetItemAPI struct {
	item map[string]any
	err  error
}

func (f *fakeGetItemAPI) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.item == nil {
		return &dynamodb.GetItemOutput{}, nil
	}
	item, err := attributevalue.MarshalMap(f.item)
	if err != nil {
		return nil, err
	}
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func TestHashAPIKey_IsDeterministicSHA256Hex(t *testing.T) {
	got := HashAPIKey("abc")
	want := "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"[:64]
	if got != want {
		t.Fatalf("HashAPIKey(%q) = %q, want %q", "abc", got, want)
	}
	if HashAPIKey("abc") != HashAPIKey("abc") {
		t.Fatalf("HashAPIKey is not deterministic")
	}
}

func TestResolveAPIKey_WithFakeClient_FindsTenant(t *testing.T) {
	fake := &fakeGetItemAPI{item: map[string]any{
		"api_key_hash":         HashAPIKey("key-1"),
		"tenant_id":            "tenant-x",
		"tier":                 "standard",
		"rpm_limit":            30,
		"monthly_token_budget": 500000,
	}}
	store := NewDynamoStore(fake, "tenants")

	tenant, err := store.ResolveAPIKey(context.Background(), "key-1")
	if err != nil {
		t.Fatalf("ResolveAPIKey: %v", err)
	}
	if tenant.ID != "tenant-x" || tenant.Tier != "standard" || tenant.RPMLimit != 30 || tenant.MonthlyTokenBudget != 500000 {
		t.Fatalf("unexpected tenant: %+v", tenant)
	}
}

func TestResolveAPIKey_WithFakeClient_UnknownKeyReturnsSentinel(t *testing.T) {
	fake := &fakeGetItemAPI{}
	store := NewDynamoStore(fake, "tenants")

	_, err := store.ResolveAPIKey(context.Background(), "missing")
	if !errors.Is(err, ErrUnknownAPIKey) {
		t.Fatalf("expected ErrUnknownAPIKey, got %v", err)
	}
}

func TestResolveAPIKey_WithFakeClient_PropagatesClientError(t *testing.T) {
	fake := &fakeGetItemAPI{err: errors.New("boom")}
	store := NewDynamoStore(fake, "tenants")

	_, err := store.ResolveAPIKey(context.Background(), "key-1")
	if err == nil {
		t.Fatalf("expected error")
	}
}

// TestResolveAPIKey_RejectsIncompleteTenantRecord: a row missing rpm_limit
// or monthly_token_budget must fail loudly instead of becoming a permanent
// 429 (rate.NewLimiter(0,0) never allows) or a permanent, silent 402.
func TestResolveAPIKey_RejectsIncompleteTenantRecord(t *testing.T) {
	cases := map[string]map[string]any{
		"missing rpm": {
			"api_key_hash":         HashAPIKey("key-1"),
			"tenant_id":            "tenant-x",
			"tier":                 "free",
			"monthly_token_budget": 1000,
		},
		"missing budget": {
			"api_key_hash": HashAPIKey("key-1"),
			"tenant_id":    "tenant-x",
			"tier":         "free",
			"rpm_limit":    10,
		},
		"missing tenant id": {
			"api_key_hash":         HashAPIKey("key-1"),
			"tier":                 "free",
			"rpm_limit":            10,
			"monthly_token_budget": 1000,
		},
	}
	for name, item := range cases {
		store := NewDynamoStore(&fakeGetItemAPI{item: item}, "tenants")
		if _, err := store.ResolveAPIKey(context.Background(), "key-1"); !errors.Is(err, ErrInvalidTenantConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidTenantConfig", name, err)
		}
	}
}
