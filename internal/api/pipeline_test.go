package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/budget"
	"github.com/Mouraovicente/ai-gateway/internal/config"
	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
)

type fakeAuth struct{ tenant core.Tenant }

func (f *fakeAuth) ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error) {
	if apiKey != "valid-key" {
		return core.Tenant{}, context.DeadlineExceeded // any non-nil error signals "not found" for this fake
	}
	return f.tenant, nil
}

type fakeLimiter struct{ allow bool }

func (f *fakeLimiter) Allow(tenantID string, rpm int) (bool, int) {
	if f.allow {
		return true, 0
	}
	return false, 5
}

// settleCall records one budget.Settle invocation, so tests can assert the
// settle invariant: exactly one Settle per request, with the expected token
// count, on every exit path (success, error, cancel).
type settleCall struct {
	reservationID string
	realTokens    int
}

type fakeBudget struct {
	reserveErr error

	mu      sync.Mutex
	settles []settleCall
}

func (f *fakeBudget) Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error) {
	if f.reserveErr != nil {
		return core.Reservation{}, f.reserveErr
	}
	return core.Reservation{ID: "r1", TenantID: tenantID, Period: period, EstimatedTokens: estimatedTokens}, nil
}
func (f *fakeBudget) Settle(ctx context.Context, reservation core.Reservation, realTokens int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settles = append(f.settles, settleCall{reservationID: reservation.ID, realTokens: realTokens})
	return nil
}

func (f *fakeBudget) settleCalls() []settleCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]settleCall, len(f.settles))
	copy(out, f.settles)
	return out
}

type fakeTrace struct{}

func (f *fakeTrace) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error {
	return nil
}
func (f *fakeTrace) RecordEvent(ctx context.Context, requestID string, seq int, eventType trace.EventType, node string, payload map[string]any) error {
	return nil
}

// recordingTrace is a trace.Store that records the ordered sequence of
// event types it was asked to record, so tests can assert the exact
// trace_event sequence a request produces.
type recordingTrace struct {
	mu     sync.Mutex
	events []trace.EventType
}

func (r *recordingTrace) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error {
	return nil
}
func (r *recordingTrace) RecordEvent(ctx context.Context, requestID string, seq int, eventType trace.EventType, node string, payload map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, eventType)
	return nil
}
func (r *recordingTrace) types() []trace.EventType {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]trace.EventType, len(r.events))
	copy(out, r.events)
	return out
}

type fakeChatBackend struct{ content string }

func (f *fakeChatBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	return core.ChatResponse{Message: core.Message{Role: "assistant", Content: f.content}, Usage: core.Usage{PromptTokens: 5, CompletionTokens: 3}, FinishReason: "stop"}, nil
}

func (f *fakeChatBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)
	close(chunks)
	close(errs)
	return chunks, errs
}

// fakeStreamBackend is a resilience.FullBackend whose ChatStream plays back
// a fixed sequence of chunks and then a fixed terminal error (nil for a
// clean end of stream). Chat is not exercised by the streaming tests.
type fakeStreamBackend struct {
	streamChunks []core.ChatChunk
	streamErr    error
}

func (f *fakeStreamBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	return core.ChatResponse{}, errors.New("fakeStreamBackend.Chat is not used by streaming tests")
}

func (f *fakeStreamBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk, len(f.streamChunks))
	errs := make(chan error, 1)
	for _, c := range f.streamChunks {
		chunks <- c
	}
	close(chunks)
	errs <- f.streamErr
	close(errs)
	return chunks, errs
}

// testProviders registers every provider name these fixtures reference, so
// router.Resolve's provider-known check (internal/router/router.go) doesn't
// reject an otherwise-valid alias cascade as an unknown provider.
func testProviders() map[string]config.Provider {
	return map[string]config.Provider{
		"ollama":     {},
		"openrouter": {},
	}
}

func testRouting() *config.Routing {
	return &config.Routing{
		Aliases: map[string]config.AliasTiers{
			"nuva/fast": {Tiers: map[string][]config.BackendTargetConfig{"free": {{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}}}},
		},
		Providers: testProviders(),
	}
}

// testRoutingWithFallback gives tier "standard" a two-target cascade, used
// by the streaming fallback tests below.
func testRoutingWithFallback() *config.Routing {
	return &config.Routing{
		Aliases: map[string]config.AliasTiers{
			"nuva/fast": {Tiers: map[string][]config.BackendTargetConfig{
				"standard": {{Provider: "ollama", Model: "m1"}, {Provider: "openrouter", Model: "m2"}},
			}},
		},
		Providers: testProviders(),
	}
}

func TestPipeline_HappyPath_ReturnsCompletionAndSettlesBudget(t *testing.T) {
	fb := &fakeBudget{}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free", RPMLimit: 10, MonthlyTokenBudget: 100000}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    fb,
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{"ollama": &fakeChatBackend{content: "ok"}},
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	reqBody := `{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	// fakeChatBackend reports PromptTokens: 5, CompletionTokens: 3 -> settle must carry 8, not 0 or 3.
	if calls := fb.settleCalls(); len(calls) != 1 || calls[0].realTokens != 8 {
		t.Fatalf("expected exactly one settle with realTokens=8, got %+v", calls)
	}
}

func TestPipeline_MissingAuth_Returns401(t *testing.T) {
	p := &Pipeline{Auth: &fakeAuth{}, RateLimit: &fakeLimiter{allow: true}, Budget: &fakeBudget{}, Routing: testRouting(), Trace: &fakeTrace{}}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[]}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestPipeline_RateLimited_Returns429WithRetryAfter(t *testing.T) {
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: false},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "5" {
		t.Fatalf("expected Retry-After: 5, got %q", rec.Header().Get("Retry-After"))
	}
}

func TestPipeline_BudgetExceeded_Returns402(t *testing.T) {
	fb := &fakeBudget{reserveErr: budget.ErrBudgetExceeded}
	tr := &recordingTrace{}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    fb,
		Routing:   testRouting(),
		Trace:     tr,
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("expected 402, got %d", rec.Code)
	}
	// Reserve failed before the settle defer is ever registered: no Settle call at all.
	if calls := fb.settleCalls(); len(calls) != 0 {
		t.Fatalf("expected zero settle calls when Reserve fails, got %+v", calls)
	}
	want := []trace.EventType{trace.Auth, trace.RateLimit, trace.BudgetReserve}
	if got := tr.types(); !reflect.DeepEqual(got, want) {
		t.Fatalf("event sequence = %v, want %v", got, want)
	}
}

func TestPipeline_UnknownAlias_Returns400(t *testing.T) {
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/does-not-exist","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	var body map[string]map[string]string
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["error"]["type"] != "unknown_model" {
		t.Fatalf("unexpected error type: %+v", body)
	}
}

func TestPipeline_AllBackendsFailed_Returns502WithAttempts(t *testing.T) {
	fb := &fakeBudget{}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    fb,
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{}, // no backend registered for "ollama"
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d: %s", rec.Code, rec.Body.String())
	}
	if calls := fb.settleCalls(); len(calls) != 1 || calls[0].realTokens != 0 {
		t.Fatalf("expected exactly one settle with realTokens=0, got %+v", calls)
	}
}

func TestPipeline_Streaming_FallsBackBeforeFirstByte(t *testing.T) {
	transientErr := &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("unavailable")}
	first := &fakeStreamBackend{streamErr: transientErr}
	second := &fakeStreamBackend{streamChunks: []core.ChatChunk{
		{Delta: "ok"},
		{Usage: &core.Usage{PromptTokens: 4, CompletionTokens: 2}},
	}}

	fb := &fakeBudget{}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "standard"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    fb,
		Routing:   testRoutingWithFallback(),
		Backends:  map[string]resilience.FullBackend{"ollama": first, "openrouter": second},
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"content":"ok"`) {
		t.Fatalf("expected the fallback backend's delta in the stream, got: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("expected the DONE sentinel, got: %s", body)
	}
	if strings.Contains(body, "backend_stream_failed") {
		t.Fatalf("did not expect a stream error event when the fallback target succeeds, got: %s", body)
	}
	// second's terminal usage chunk reports 4+2=6.
	if calls := fb.settleCalls(); len(calls) != 1 || calls[0].realTokens != 6 {
		t.Fatalf("expected exactly one settle with realTokens=6, got %+v", calls)
	}
}

func TestPipeline_Streaming_StopsWithoutFallbackAfterFirstByte(t *testing.T) {
	failsAfterTwoChunks := &fakeStreamBackend{
		streamChunks: []core.ChatChunk{{Delta: "he"}, {Delta: "llo"}},
		streamErr:    &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("connection dropped")},
	}
	neverCalled := &fakeStreamBackend{streamChunks: []core.ChatChunk{{Delta: "should not appear"}}}

	fb := &fakeBudget{}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "standard"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    fb,
		Routing:   testRoutingWithFallback(),
		Backends:  map[string]resilience.FullBackend{"ollama": failsAfterTwoChunks, "openrouter": neverCalled},
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `"content":"he"`) || !strings.Contains(body, `"content":"llo"`) {
		t.Fatalf("expected both deltas from the failing backend before the error, got: %s", body)
	}
	if !strings.Contains(body, "backend_stream_failed") {
		t.Fatalf("expected a backend_stream_failed error event, got: %s", body)
	}
	if strings.Contains(body, "should not appear") {
		t.Fatalf("expected no fallback to the second backend after the first byte was sent, got: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("expected the DONE sentinel even after the stream error, got: %s", body)
	}
	// failsAfterTwoChunks never sent a Usage chunk: real usage is unknown, settle releases the estimate with 0.
	if calls := fb.settleCalls(); len(calls) != 1 || calls[0].realTokens != 0 {
		t.Fatalf("expected exactly one settle with realTokens=0, got %+v", calls)
	}
}

func TestPipeline_Streaming_HappyPath_EventSequenceAndSingleSettle(t *testing.T) {
	backend := &fakeStreamBackend{streamChunks: []core.ChatChunk{
		{Delta: "ok"},
		{Usage: &core.Usage{PromptTokens: 4, CompletionTokens: 2}},
	}}
	fb := &fakeBudget{}
	tr := &recordingTrace{}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    fb,
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{"ollama": backend},
		Trace:     tr,
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	want := []trace.EventType{trace.Auth, trace.RateLimit, trace.BudgetReserve, trace.Route, trace.BackendAttempt, trace.BackendResult, trace.BudgetSettle}
	if got := tr.types(); !reflect.DeepEqual(got, want) {
		t.Fatalf("event sequence = %v, want %v", got, want)
	}
	if calls := fb.settleCalls(); len(calls) != 1 || calls[0].realTokens != 6 {
		t.Fatalf("expected exactly one settle with realTokens=6, got %+v", calls)
	}
}

func TestPipeline_Streaming_ClientCancel_SettlesOnceQuietly(t *testing.T) {
	backend := &fakeStreamBackend{streamChunks: []core.ChatChunk{{Delta: "hi"}}}
	fb := &fakeBudget{}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    fb,
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{"ollama": backend},
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // simulate a client that is already gone by the time onChunk runs
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if strings.Contains(rec.Body.String(), "backend_stream_failed") {
		t.Fatalf("expected no in-band error event on client cancel, got: %s", rec.Body.String())
	}
	if calls := fb.settleCalls(); len(calls) != 1 {
		t.Fatalf("expected exactly one settle call on client cancel, got %+v", calls)
	}
}
