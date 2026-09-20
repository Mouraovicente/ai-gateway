// Package memstore holds in-memory implementations of the gateway's four
// storage ports (auth, budget, trace, usage). It exists so the process can
// run with STORE_BACKEND=memory for local development and for benchmarks
// that need to measure the gateway's own overhead instead of LocalStack's
// throughput. It is not a production backend: nothing here survives a
// restart and nothing is shared between replicas.
package memstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/Mouraovicente/ai-gateway/internal/auth"
	"github.com/Mouraovicente/ai-gateway/internal/budget"
	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
	"github.com/Mouraovicente/ai-gateway/internal/usage"
)

// AuthStore resolves API keys from a fixed, preloaded tenant table keyed by
// the same SHA-256 hash the DynamoDB store uses, so a key that works
// against one backend works against the other.
type AuthStore struct {
	byHash map[string]core.Tenant
}

// NewAuthStore indexes tenants by hash of their (plaintext) API key.
func NewAuthStore(tenants map[string]core.Tenant) *AuthStore {
	byHash := make(map[string]core.Tenant, len(tenants))
	for apiKey, t := range tenants {
		t.APIKeyHash = auth.HashAPIKey(apiKey)
		byHash[t.APIKeyHash] = t
	}
	return &AuthStore{byHash: byHash}
}

func (s *AuthStore) ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error) {
	t, ok := s.byHash[auth.HashAPIKey(apiKey)]
	if !ok {
		return core.Tenant{}, auth.ErrUnknownAPIKey
	}
	return t, nil
}

// BudgetStore mirrors the DynamoDB budget semantics: Reserve is an atomic
// check-and-add against the tenant/period row (a missing row counts as
// used=0, matching the attribute_not_exists branch of the real condition
// expression) and Settle applies the delta between real and estimated.
type BudgetStore struct {
	mu   sync.Mutex
	used map[string]int // "tenant|period" -> tokens
}

func NewBudgetStore() *BudgetStore {
	return &BudgetStore{used: make(map[string]int)}
}

func budgetKey(tenantID, period string) string { return tenantID + "|" + period }

func (s *BudgetStore) Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := budgetKey(tenantID, period)
	used, exists := s.used[k]
	// Same rule as the DynamoDB condition: no row yet means the estimate
	// alone must fit; an existing row must already be at or below
	// limit - estimate.
	if (!exists && estimatedTokens > monthlyLimit) || (exists && used > monthlyLimit-estimatedTokens) {
		return core.Reservation{}, budget.ErrBudgetExceeded
	}
	s.used[k] = used + estimatedTokens
	return core.Reservation{ID: uuid.NewString(), TenantID: tenantID, Period: period, EstimatedTokens: estimatedTokens}, nil
}

func (s *BudgetStore) Settle(ctx context.Context, reservation core.Reservation, realTokens int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := budgetKey(reservation.TenantID, reservation.Period)
	s.used[k] += realTokens - reservation.EstimatedTokens
	return nil
}

// Used reports the tokens currently charged to a tenant/period, for tests.
func (s *BudgetStore) Used(tenantID, period string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used[budgetKey(tenantID, period)]
}

// TraceStore keeps the last recorded requests/events in memory. It enforces
// the same forbidden-key rule as the DynamoDB store: a payload that could
// carry prompt or completion content is rejected, not stored.
type TraceStore struct {
	mu       sync.Mutex
	requests []string
	events   []trace.EventRecord
	// maxEvents bounds memory in a long benchmark run; the oldest events
	// are dropped once it is reached.
	maxEvents int
}

func NewTraceStore() *TraceStore { return &TraceStore{maxEvents: 10_000} }

func (s *TraceStore) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, requestID)
	if len(s.requests) > s.maxEvents {
		s.requests = s.requests[len(s.requests)-s.maxEvents:]
	}
	return nil
}

func (s *TraceStore) RecordEvent(ctx context.Context, requestID string, seq int, eventType trace.EventType, node string, payload map[string]any) error {
	for key := range payload {
		if trace.IsForbiddenKey(key) {
			return fmt.Errorf("memstore: payload contains forbidden key %q", key)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, trace.EventRecord{RequestID: requestID, Seq: seq, Type: eventType, Node: node, Payload: payload})
	if len(s.events) > s.maxEvents {
		s.events = s.events[len(s.events)-s.maxEvents:]
	}
	return nil
}

// Events returns a copy of the recorded events, for tests.
func (s *TraceStore) Events() []trace.EventRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]trace.EventRecord, len(s.events))
	copy(out, s.events)
	return out
}

// UsagePublisher collects usage events instead of sending them to SQS.
type UsagePublisher struct {
	mu     sync.Mutex
	events []usage.Event
	max    int
}

func NewUsagePublisher() *UsagePublisher { return &UsagePublisher{max: 10_000} }

func (p *UsagePublisher) Publish(ctx context.Context, event usage.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	if len(p.events) > p.max {
		p.events = p.events[len(p.events)-p.max:]
	}
	return nil
}

// Events returns a copy of the published events, for tests.
func (p *UsagePublisher) Events() []usage.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]usage.Event, len(p.events))
	copy(out, p.events)
	return out
}
