package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// ErrUnknownAPIKey is returned when no tenant matches the given API key.
// The API layer maps this to a 401.
var ErrUnknownAPIKey = errors.New("auth: unknown api key")

// ErrInvalidTenantConfig is returned when a tenant row exists but is not
// usable: a missing or non-positive rpm_limit would otherwise become
// rate.NewLimiter(0,0) (permanent 429 with a 292-year Retry-After) and a
// missing monthly_token_budget would become a permanent, silent 402. A
// seeding mistake must fail loudly, not look like a throttled tenant.
var ErrInvalidTenantConfig = errors.New("auth: tenant record is incomplete")

// HashAPIKey returns the SHA-256 hex digest of an API key. Keys are never
// stored or logged in plaintext; only this hash is persisted, as the pk of
// the tenants table.
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Store resolves an API key to its Tenant.
type Store interface {
	ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error)
}

// getItemAPI is the subset of *dynamodb.Client this package needs, so tests
// can substitute a fake without a real AWS endpoint. *dynamodb.Client
// satisfies it.
type getItemAPI interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

type dynamoStore struct {
	client    getItemAPI
	tableName string
}

// NewDynamoStore builds a Store backed by the tenants DynamoDB table (pk api_key_hash).
func NewDynamoStore(client getItemAPI, tableName string) Store {
	return &dynamoStore{client: client, tableName: tableName}
}

type tenantItem struct {
	APIKeyHash         string `dynamodbav:"api_key_hash"`
	TenantID           string `dynamodbav:"tenant_id"`
	Tier               string `dynamodbav:"tier"`
	RPMLimit           int    `dynamodbav:"rpm_limit"`
	MonthlyTokenBudget int    `dynamodbav:"monthly_token_budget"`
}

func (s *dynamoStore) ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error) {
	hash := HashAPIKey(apiKey)
	key, err := attributevalue.MarshalMap(map[string]string{"api_key_hash": hash})
	if err != nil {
		return core.Tenant{}, fmt.Errorf("auth: marshaling key: %w", err)
	}
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.tableName, Key: key})
	if err != nil {
		return core.Tenant{}, fmt.Errorf("auth: querying tenant: %w", err)
	}
	if out.Item == nil {
		return core.Tenant{}, ErrUnknownAPIKey
	}
	var item tenantItem
	if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
		return core.Tenant{}, fmt.Errorf("auth: unmarshaling tenant: %w", err)
	}
	if item.TenantID == "" || item.Tier == "" || item.RPMLimit <= 0 || item.MonthlyTokenBudget <= 0 {
		return core.Tenant{}, fmt.Errorf("%w (tenant_id=%q)", ErrInvalidTenantConfig, item.TenantID)
	}
	return core.Tenant{
		ID:                 item.TenantID,
		APIKeyHash:         item.APIKeyHash,
		Tier:               item.Tier,
		RPMLimit:           item.RPMLimit,
		MonthlyTokenBudget: item.MonthlyTokenBudget,
	}, nil
}
