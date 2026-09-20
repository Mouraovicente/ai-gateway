package api

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mouraovicente/ai-gateway/internal/auth"
	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/ratelimit"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
)

func TestParseBearer(t *testing.T) {
	cases := []struct {
		header  string
		wantKey string
		wantOK  bool
	}{
		{"Bearer dev-free-key", "dev-free-key", true},
		{"bearer dev-free-key", "dev-free-key", true},
		{"BEARER dev-free-key", "dev-free-key", true},
		{"dev-free-key", "", false},                                  // no scheme is not a key
		{"Bearer  dev-free-key", "", false},                          // double space
		{"Bearer ", "", false},                                       // empty key
		{"Bearer short", "", false},                                  // below minAPIKeyLen
		{"Basic dev-free-key", "", false},                            // wrong scheme
		{"", "", false},                                              // absent header
		{"Bearer " + strings.Repeat("k", maxAPIKeyLen+1), "", false}, // above maxAPIKeyLen
	}
	for _, c := range cases {
		key, ok := ParseBearer(c.header)
		if ok != c.wantOK || key != c.wantKey {
			t.Errorf("ParseBearer(%q) = %q,%v; want %q,%v", c.header, key, ok, c.wantKey, c.wantOK)
		}
	}
}

// countingAuth counts every store lookup, so the pre-auth limiter test can
// assert the store is not touched once an IP is over its failure budget.
type countingAuth struct {
	calls  atomic.Int64
	tenant core.Tenant
}

func (c *countingAuth) ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error) {
	c.calls.Add(1)
	if apiKey != "valid-key-abcdef" {
		return core.Tenant{}, auth.ErrUnknownAPIKey
	}
	return c.tenant, nil
}

func hygienePipeline(a auth.Store) *Pipeline {
	return &Pipeline{
		Auth:      a,
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{"ollama": &fakeChatBackend{content: "hi"}},
		Trace:     &fakeTrace{},
		IPLimit:   ratelimit.NewIPLimiter(60),
	}
}

func chatBody() string {
	return `{"model":"nuva/fast","messages":[{"role":"user","content":"hi"}]}`
}

func TestPipeline_PreAuthIPLimiter_Blocks429WithoutStoreCalls(t *testing.T) {
	store := &countingAuth{tenant: core.Tenant{ID: "t1", Tier: "free", RPMLimit: 100, MonthlyTokenBudget: 1000}}
	h := NewPipelineChatHandler(hygienePipeline(store))

	var last int
	for i := 0; i < 61; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatBody()))
		req.Header.Set("Authorization", "Bearer wrong-key-000")
		req.RemoteAddr = "203.0.113.9:4444"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		last = rec.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("61st bad key from one IP: got %d, want 429", last)
	}
	if got := store.calls.Load(); got > 60 {
		t.Fatalf("store was called %d times; the over-limit request must not reach it", got)
	}

	// A different IP, and a good key, are both unaffected.
	before := store.calls.Load()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatBody()))
	req.Header.Set("Authorization", "Bearer valid-key-abcdef")
	req.RemoteAddr = "203.0.113.10:4444"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("good key from a clean IP: got %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if store.calls.Load() != before+1 {
		t.Fatalf("good key should reach the store exactly once")
	}
}

// storeErrAuth simulates a DynamoDB/network failure (a wrapped SDK error),
// which must map to 503, never 401.
type storeErrAuth struct{}

func (storeErrAuth) ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error) {
	return core.Tenant{}, fmt.Errorf("auth: querying tenant: %w", context.DeadlineExceeded)
}

func TestPipeline_AuthStoreError_Returns503WithRetryAfter(t *testing.T) {
	h := NewPipelineChatHandler(hygienePipeline(storeErrAuth{}))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatBody()))
	req.Header.Set("Authorization", "Bearer some-key-123456")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", rec.Header().Get("Retry-After"))
	}
	if !strings.Contains(rec.Body.String(), "store_unavailable") {
		t.Fatalf("body = %s, want store_unavailable", rec.Body.String())
	}
}

func TestPipeline_BodyLimits_Return413(t *testing.T) {
	store := &countingAuth{tenant: core.Tenant{ID: "t1", Tier: "free", RPMLimit: 100, MonthlyTokenBudget: 1_000_000}}
	h := NewPipelineChatHandler(hygienePipeline(store))

	var many bytes.Buffer
	many.WriteString(`{"model":"nuva/fast","messages":[`)
	for i := 0; i < maxMessages+1; i++ {
		if i > 0 {
			many.WriteString(",")
		}
		many.WriteString(`{"role":"user","content":"x"}`)
	}
	many.WriteString(`]}`)

	big := fmt.Sprintf(`{"model":"nuva/fast","messages":[{"role":"user","content":%q}]}`, strings.Repeat("x", maxPromptBytes+1))

	for name, body := range map[string]string{"too many messages": many.String(), "prompt too large": big} {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-key-abcdef")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s: got %d, want 413 (%s)", name, rec.Code, rec.Body.String())
		}
	}
}

// slowStreamBackend emits chunks forever until its context is done, so the
// only thing that can stop the handler is the SSE write deadline.
type slowStreamBackend struct{}

func (slowStreamBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	return core.ChatResponse{}, context.Canceled
}

func (slowStreamBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)
	go func() {
		defer close(chunks)
		defer close(errs)
		payload := strings.Repeat("y", 32<<10)
		for {
			select {
			case chunks <- core.ChatChunk{Delta: payload}:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
	}()
	return chunks, errs
}

// TestPipeline_Streaming_SlowClientIsCutByWriteDeadline opens a stream over
// a real socket and then stops reading. Without a per-write deadline the
// handler blocks forever on a full socket buffer (net/http only cancels the
// request context when the connection *closes*, which a silent client never
// does). With StreamWriteTimeout set, the handler must return on its own.
func TestPipeline_Streaming_SlowClientIsCutByWriteDeadline(t *testing.T) {
	store := &countingAuth{tenant: core.Tenant{ID: "t1", Tier: "free", RPMLimit: 1000, MonthlyTokenBudget: 1_000_000}}
	p := hygienePipeline(store)
	p.Backends = map[string]resilience.FullBackend{"ollama": slowStreamBackend{}}
	p.StreamWriteTimeout = 200 * time.Millisecond

	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		NewPipelineChatHandler(p).ServeHTTP(w, r)
	}))
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	body := `{"model":"nuva/fast","messages":[{"role":"user","content":"hi"}],"stream":true}`
	_, _ = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer valid-key-abcdef\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)

	// Read nothing at all from here on: the socket buffer fills and every
	// further write blocks past the deadline.
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("handler did not return: the slow client was never cut")
	}

}
