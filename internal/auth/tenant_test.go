//go:build integration

package auth

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func newTestDynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint })
}

func TestResolveAPIKey_FindsSeededTenant(t *testing.T) {
	client := newTestDynamoClient(t)
	ctx := context.Background()

	hash := HashAPIKey("test-key-123")
	item, _ := attributevalue.MarshalMap(map[string]any{
		"api_key_hash":         hash,
		"tenant_id":            "tenant-abc",
		"tier":                 "free",
		"rpm_limit":            10,
		"monthly_token_budget": 100000,
	})
	if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: strPtr("tenants"), Item: item}); err != nil {
		t.Fatalf("seeding tenant: %v", err)
	}

	store := NewDynamoStore(client, "tenants")
	tenant, err := store.ResolveAPIKey(ctx, "test-key-123")
	if err != nil {
		t.Fatalf("ResolveAPIKey: %v", err)
	}
	if tenant.ID != "tenant-abc" || tenant.Tier != "free" || tenant.RPMLimit != 10 {
		t.Fatalf("unexpected tenant: %+v", tenant)
	}
}

func TestResolveAPIKey_UnknownKeyReturnsError(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "tenants")
	if _, err := store.ResolveAPIKey(context.Background(), "does-not-exist"); err == nil {
		t.Fatalf("expected error for unknown API key")
	}
}

func strPtr(s string) *string { return &s }
