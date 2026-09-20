package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

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

	// Every non-root span must be a descendant of the root trace, and every
	// child span (everything but the root) must be a direct child of the
	// root span specifically — not just somewhere in the same trace.
	traceID := root.SpanContext().TraceID()
	rootSpanID := root.SpanContext().SpanID()
	for _, s := range spans {
		if s.SpanContext().TraceID() != traceID {
			t.Errorf("span %q belongs to a different trace than the root", s.Name())
		}
		if s.Name() == "POST /v1/chat/completions" {
			continue
		}
		if s.Parent().SpanID() != rootSpanID {
			t.Errorf("span %q has parent %s, want root span %s", s.Name(), s.Parent().SpanID(), rootSpanID)
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

// findSum returns the first int64 Sum-metric datapoints for the given
// instrument name found in rm, or nil if it isn't present.
func findSum(rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				return sum.DataPoints
			}
		}
	}
	return nil
}

// findHistogram returns the first float64 histogram datapoints for the
// given instrument name found in rm, or nil if it isn't present.
func findHistogram(rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[float64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
				return hist.DataPoints
			}
		}
	}
	return nil
}

func TestPipeline_Streaming_HappyPath_EmitsSpanHierarchyAndTTFTMetric(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	tracer := tracerProvider.Tracer("ai-gateway-test")

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, err := trace.NewMetrics(meterProvider.Meter("ai-gateway-test"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	backend := &fakeStreamBackend{streamChunks: []core.ChatChunk{
		{Delta: "ok"},
		{Usage: &core.Usage{PromptTokens: 4, CompletionTokens: 2}},
	}}
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free", RPMLimit: 10, MonthlyTokenBudget: 100000}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{"ollama": backend},
		Trace:     &fakeTrace{},
		Tracer:    tracer,
		Metrics:   metrics,
		Usage:     &fakeUsagePublisher{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Span hierarchy: same shape as the non-streaming happy path, everything
	// a direct child of the root.
	spans := spanRecorder.Ended()
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
	rootSpanID := root.SpanContext().SpanID()
	for _, s := range spans {
		if s.Name() == "POST /v1/chat/completions" {
			continue
		}
		if s.Parent().SpanID() != rootSpanID {
			t.Errorf("span %q has parent %s, want root span %s", s.Name(), s.Parent().SpanID(), rootSpanID)
		}
	}

	// Metrics: RequestsTotal and TTFTMs must both have been recorded — this
	// is the exact gap fix round 1 targets ("gateway_ttft_ms never appears
	// even after a streaming request").
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if dp := findSum(rm, "gateway_requests_total"); len(dp) == 0 {
		t.Errorf("expected gateway_requests_total to have at least one datapoint, got none")
	} else if dp[0].Value != 1 {
		t.Errorf("gateway_requests_total = %d, want 1", dp[0].Value)
	}
	if dp := findHistogram(rm, "gateway_ttft_ms"); len(dp) == 0 {
		t.Errorf("expected gateway_ttft_ms to have at least one datapoint, got none")
	} else if dp[0].Count == 0 {
		t.Errorf("gateway_ttft_ms datapoint has zero count")
	}
	if dp := findHistogram(rm, "gateway_latency_ms"); len(dp) == 0 {
		t.Errorf("expected gateway_latency_ms to have at least one datapoint, got none")
	}
	if dp := findSum(rm, "gateway_tokens_total"); len(dp) < 2 {
		t.Errorf("expected gateway_tokens_total to have prompt+completion datapoints, got %d", len(dp))
	}
}
