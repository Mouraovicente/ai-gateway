//go:build integration

package budget

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// uniqueTenantID returns a tenant id unique to this test run, so repeated
// runs against a reused LocalStack instance never see a budgets/reservations
// row left over from a previous run (tenant ids used to be fixed literals,
// which made these tests pass only once per fresh LocalStack).
func uniqueTenantID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

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

func TestReserveThenSettle_AdjustsUsedByDelta(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "budgets", "reservations")
	ctx := context.Background()
	tenantID := uniqueTenantID("tenant-budget-test-1")

	reservation, err := store.Reserve(ctx, tenantID, "2026-09", 1000, 100000)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if reservation.EstimatedTokens != 1000 {
		t.Fatalf("unexpected reservation: %+v", reservation)
	}

	if err := store.Settle(ctx, reservation, 800); err != nil {
		t.Fatalf("Settle: %v", err)
	}
}

func TestReserve_RejectsWhenOverBudget(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "budgets", "reservations")
	ctx := context.Background()
	tenantID := uniqueTenantID("tenant-budget-test-2")

	if _, err := store.Reserve(ctx, tenantID, "2026-09", 900, 1000); err != nil {
		t.Fatalf("first Reserve should succeed: %v", err)
	}
	if _, err := store.Reserve(ctx, tenantID, "2026-09", 200, 1000); err != ErrBudgetExceeded {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestReserve_ConcurrentRequestsNeverExceedLimit(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "budgets", "reservations")
	ctx := context.Background()
	tenantID := uniqueTenantID("tenant-budget-race")
	const limit = 1000
	const perRequest = 30
	const goroutines = 50 // 50 * 30 = 1500 > limit: some must be rejected

	var accepted int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Reserve(ctx, tenantID, "2026-09", perRequest, limit); err == nil {
				atomic.AddInt64(&accepted, 1)
			}
		}()
	}
	wg.Wait()

	if accepted*perRequest > limit {
		t.Fatalf("accepted %d reservations of %d tokens each (%d total), exceeds limit %d", accepted, perRequest, accepted*perRequest, limit)
	}
	t.Logf("accepted=%d perRequest=%d total=%d limit=%d", accepted, perRequest, accepted*perRequest, limit)
	if accepted == 0 {
		t.Fatalf("expected at least one reservation to be accepted")
	}
}

var _ core.Reservation // keep core imported for the Reservation type used above
