package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
)

// usageBackend answers with whatever usage the test wants, including the
// values a compromised or buggy provider could report.
type usageBackend struct{ usage core.Usage }

func (b usageBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	return core.ChatResponse{Message: core.Message{Role: "assistant", Content: "ok"}, Usage: b.usage, FinishReason: "stop"}, nil
}

func (b usageBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)
	close(chunks)
	close(errs)
	return chunks, errs
}

// TestPipeline_ImplausibleProviderUsage_FallsBackToEstimate covers both
// bounds: a negative total would credit the tenant's monthly budget
// forever, and an absurdly large one would overcharge. Either way the
// settle must use the estimate that was actually reserved.
func TestPipeline_ImplausibleProviderUsage_FallsBackToEstimate(t *testing.T) {
	// promptBytes/4 + default max_tokens(1024): "hi" is 2 bytes -> 1024.
	const wantEstimate = 1024

	for name, reported := range map[string]core.Usage{
		"negative": {PromptTokens: -5_000_000, CompletionTokens: 1},
		"absurd":   {PromptTokens: 0, CompletionTokens: wantEstimate*4 + 1},
	} {
		budgetStore := &fakeBudget{}
		p := hygienePipeline(&countingAuth{tenant: core.Tenant{ID: "t1", Tier: "free", RPMLimit: 100, MonthlyTokenBudget: 1_000_000}})
		p.Budget = budgetStore
		p.Backends = map[string]resilience.FullBackend{"ollama": usageBackend{usage: reported}}

		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatBody()))
		req.Header.Set("Authorization", "Bearer valid-key-abcdef")
		rec := httptest.NewRecorder()
		NewPipelineChatHandler(p).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d, want 200", name, rec.Code)
		}
		settles := budgetStore.settleCalls()
		if len(settles) != 1 {
			t.Fatalf("%s: %d settles, want exactly 1", name, len(settles))
		}
		if settles[0].realTokens != wantEstimate {
			t.Fatalf("%s: settled %d tokens, want the estimate %d", name, settles[0].realTokens, wantEstimate)
		}
	}

	// A plausible reading is passed through untouched.
	budgetStore := &fakeBudget{}
	p := hygienePipeline(&countingAuth{tenant: core.Tenant{ID: "t1", Tier: "free", RPMLimit: 100, MonthlyTokenBudget: 1_000_000}})
	p.Budget = budgetStore
	p.Backends = map[string]resilience.FullBackend{"ollama": usageBackend{usage: core.Usage{PromptTokens: 7, CompletionTokens: 11}}}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatBody()))
	req.Header.Set("Authorization", "Bearer valid-key-abcdef")
	NewPipelineChatHandler(p).ServeHTTP(httptest.NewRecorder(), req)
	if got := budgetStore.settleCalls()[0].realTokens; got != 18 {
		t.Fatalf("plausible usage settled as %d, want 18", got)
	}
}

func TestPipeline_HostileModelName_Returns400(t *testing.T) {
	p := hygienePipeline(&countingAuth{tenant: core.Tenant{ID: "t1", Tier: "premium", RPMLimit: 100, MonthlyTokenBudget: 1_000_000}})
	for _, model := range []string{
		"ollama/../../v1/other",
		"ollama/x?alt=sse",
		"ollama/x#frag",
		"ollama/x\u0000y",
		"ollama/" + strings.Repeat("m", 300),
	} {
		body := `{"model":"` + strings.ReplaceAll(model, "\u0000", `\u0000`) + `","messages":[{"role":"user","content":"hi"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-key-abcdef")
		rec := httptest.NewRecorder()
		NewPipelineChatHandler(p).ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("model %q: status %d, want 400", model, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "unknown_model") {
			t.Errorf("model %q: body %s, want unknown_model", model, rec.Body.String())
		}
	}
}

func TestPipeline_InvalidRole_Returns400(t *testing.T) {
	p := hygienePipeline(&countingAuth{tenant: core.Tenant{ID: "t1", Tier: "free", RPMLimit: 100, MonthlyTokenBudget: 1_000_000}})
	body := `{"model":"nuva/fast","messages":[{"role":"wizard","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer valid-key-abcdef")
	rec := httptest.NewRecorder()
	NewPipelineChatHandler(p).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 (%s)", rec.Code, rec.Body.String())
	}
}

func TestPipeline_TrailingGarbageAfterJSON_Returns400(t *testing.T) {
	p := hygienePipeline(&countingAuth{tenant: core.Tenant{ID: "t1", Tier: "free", RPMLimit: 100, MonthlyTokenBudget: 1_000_000}})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatBody()+`{"extra":1}`))
	req.Header.Set("Authorization", "Bearer valid-key-abcdef")
	rec := httptest.NewRecorder()
	NewPipelineChatHandler(p).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
}

func TestRequestIDMiddleware_EchoesClientIDWithoutTrustingIt(t *testing.T) {
	var seen, seenClient string
	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromContext(r.Context())
		seenClient = ClientRequestIDFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-Id", "client-supplied-\x01id"+strings.Repeat("x", 200))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if seen == "" || strings.Contains(seen, "client-supplied") {
		t.Fatalf("request id %q must be server-generated", seen)
	}
	echoed := rec.Header().Get("X-Client-Request-Id")
	if echoed == "" || echoed != seenClient {
		t.Fatalf("X-Client-Request-Id = %q, context = %q", echoed, seenClient)
	}
	if len(echoed) > maxClientRequestIDLen || strings.ContainsRune(echoed, '\x01') {
		t.Fatalf("echoed id not sanitized: %q", echoed)
	}
	if rec.Header().Get("X-Request-Id") != seen {
		t.Fatalf("X-Request-Id header must carry the generated id")
	}
}
