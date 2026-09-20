package memstore

import (
	"context"
	"errors"
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/auth"
	"github.com/Mouraovicente/ai-gateway/internal/budget"
	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
)

func TestAuthStore_ResolvesByHashAndRejectsUnknown(t *testing.T) {
	s := NewAuthStore(map[string]core.Tenant{
		"dev-free-key": {ID: "tenant-free", Tier: "free", RPMLimit: 10, MonthlyTokenBudget: 50000},
	})
	got, err := s.ResolveAPIKey(context.Background(), "dev-free-key")
	if err != nil {
		t.Fatalf("ResolveAPIKey: %v", err)
	}
	if got.ID != "tenant-free" || got.APIKeyHash != auth.HashAPIKey("dev-free-key") {
		t.Fatalf("unexpected tenant: %+v", got)
	}
	if _, err := s.ResolveAPIKey(context.Background(), "nope"); !errors.Is(err, auth.ErrUnknownAPIKey) {
		t.Fatalf("unknown key err = %v, want ErrUnknownAPIKey", err)
	}
}

func TestBudgetStore_ReserveIsAtomicAndSettleAppliesDelta(t *testing.T) {
	s := NewBudgetStore()
	ctx := context.Background()

	// Missing row: the estimate alone must fit under the limit.
	if _, err := s.Reserve(ctx, "t1", "2026-09", 101, 100); !errors.Is(err, budget.ErrBudgetExceeded) {
		t.Fatalf("over-limit first reserve err = %v, want ErrBudgetExceeded", err)
	}
	r, err := s.Reserve(ctx, "t1", "2026-09", 60, 100)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if s.Used("t1", "2026-09") != 60 {
		t.Fatalf("used = %d, want 60", s.Used("t1", "2026-09"))
	}
	// Existing row already at 60: a 50-token reserve exceeds 100.
	if _, err := s.Reserve(ctx, "t1", "2026-09", 50, 100); !errors.Is(err, budget.ErrBudgetExceeded) {
		t.Fatalf("second reserve err = %v, want ErrBudgetExceeded", err)
	}
	if err := s.Settle(ctx, r, 10); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if got := s.Used("t1", "2026-09"); got != 10 {
		t.Fatalf("after settle used = %d, want 10", got)
	}
}

func TestTraceStore_RejectsForbiddenPayloadKey(t *testing.T) {
	s := NewTraceStore()
	err := s.RecordEvent(context.Background(), "req-1", 1, trace.Auth, "auth", map[string]any{"Prompt": "leaked"})
	if err == nil {
		t.Fatal("expected forbidden key to be rejected")
	}
	if len(s.Events()) != 0 {
		t.Fatal("forbidden payload must not be stored")
	}
	if err := s.RecordEvent(context.Background(), "req-1", 1, trace.Auth, "auth", map[string]any{"tenant_id": "t1"}); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	if len(s.Events()) != 1 {
		t.Fatalf("expected one stored event, got %d", len(s.Events()))
	}
}
