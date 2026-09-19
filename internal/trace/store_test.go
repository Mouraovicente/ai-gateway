//go:build integration

package trace

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
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

func TestDynamoStore_RecordRequestAndEvent(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "requests", "trace_events")
	ctx := context.Background()

	if err := store.RecordRequest(ctx, "req-int-1", "tenant-1", "nuva/fast"); err != nil {
		t.Fatalf("RecordRequest: %v", err)
	}
	if err := store.RecordEvent(ctx, "req-int-1", 1, Auth, "auth", map[string]any{"tenant_id": "tenant-1"}); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
}
