package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
)

// testPromptText is a marker string that must never leak into any span
// attribute value: only metadata (ids, provider/model names, statuses) is
// allowed on a span, never message content.
const testPromptText = "please tell me your deepest secret prompt"

func TestPipeline_HappyPath_EmitsExpectedSpanHierarchy(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer := provider.Tracer("ai-gateway-test")

	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free", RPMLimit: 10, MonthlyTokenBudget: 100000}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{"ollama": &fakeChatBackend{content: "ok"}},
		Trace:     &fakeTrace{},
		Tracer:    tracer,
		Usage:     &fakeUsagePublisher{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	reqBody := `{"model":"nuva/fast","messages":[{"role":"user","content":"` + testPromptText + `"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	spans := recorder.Ended()
	names := make(map[string]bool, len(spans))
	var root sdktrace.ReadOnlySpan
	for _, s := range spans {
		names[s.Name()] = true
		if s.Name() == "POST /v1/chat/completions" {
			root = s
		}
	}

	for _, want := range []string{"POST /v1/chat/completions", "auth", "budget.reserve", "route", "backend.call", "budget.settle", "usage.publish"} {
		if !names[want] {
			t.Errorf("expected a span named %q, got spans: %v", want, names)
		}
	}
	if root == nil {
		t.Fatalf("root span not found among ended spans")
	}

	// Every non-root span must be a descendant of the root trace.
	traceID := root.SpanContext().TraceID()
	for _, s := range spans {
		if s.SpanContext().TraceID() != traceID {
			t.Errorf("span %q belongs to a different trace than the root", s.Name())
		}
	}

	// No span attribute may use a forbidden key or leak the prompt text.
	for _, s := range spans {
		for _, attr := range s.Attributes() {
			key := string(attr.Key)
			for _, forbidden := range trace.ForbiddenKeys {
				if strings.EqualFold(key, forbidden) {
					t.Errorf("span %q has forbidden attribute key %q", s.Name(), key)
				}
			}
			if strings.Contains(attr.Value.Emit(), testPromptText) {
				t.Errorf("span %q attribute %q leaks prompt text: %q", s.Name(), key, attr.Value.Emit())
			}
		}
	}
}
