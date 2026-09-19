//go:build ignore

package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/Mouraovicente/ai-gateway/internal/auth"
)

// Run with: go run scripts/seed_tenants.go
// Seeds one tenant per tier for local dev/testing against LocalStack.
// These are fake example keys for local dev only — never a real API key.
func main() {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("loading AWS config: %v", err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint })

	tenants := []struct {
		apiKey             string
		tenantID           string
		tier               string
		rpmLimit           int
		monthlyTokenBudget int
	}{
		{"dev-free-key", "tenant-free", "free", 10, 50000},
		{"dev-standard-key", "tenant-standard", "standard", 30, 500000},
		{"dev-premium-key", "tenant-premium", "premium", 60, 2000000},
	}

	for _, t := range tenants {
		item, err := attributevalue.MarshalMap(map[string]any{
			"api_key_hash":         auth.HashAPIKey(t.apiKey),
			"tenant_id":            t.tenantID,
			"tier":                 t.tier,
			"rpm_limit":            t.rpmLimit,
			"monthly_token_budget": t.monthlyTokenBudget,
		})
		if err != nil {
			log.Fatalf("marshaling tenant %s: %v", t.tenantID, err)
		}
		if _, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: strPtr("tenants"), Item: item}); err != nil {
			log.Fatalf("seeding tenant %s: %v", t.tenantID, err)
		}
		log.Printf("seeded tenant %s (api key: %s)", t.tenantID, t.apiKey)
	}
}

func strPtr(s string) *string { return &s }
