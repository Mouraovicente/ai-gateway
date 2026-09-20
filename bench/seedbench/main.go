//go:build ignore

package main

// Seeds a throwaway high-limit tenant for local load testing only.
// bench-key is an obviously fake dev key, never a real credential.
// Run: go run bench/seedbench/main.go
import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/Mouraovicente/ai-gateway/internal/auth"
)

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

	item, err := attributevalue.MarshalMap(map[string]any{
		"api_key_hash":         auth.HashAPIKey("bench-key"),
		"tenant_id":            "tenant-bench",
		"tier":                 "free",
		"rpm_limit":            100000,
		"monthly_token_budget": 1000000000,
	})
	if err != nil {
		log.Fatalf("marshaling bench tenant: %v", err)
	}
	name := "tenants"
	if _, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: &name, Item: item}); err != nil {
		log.Fatalf("seeding bench tenant: %v", err)
	}
	log.Println("seeded tenant-bench (api key: bench-key, rpm 100000, budget 1e9) — dev/load-test only")
}
