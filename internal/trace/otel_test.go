package trace

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
)

func TestNewMetrics_RegistersAllFourInstruments(t *testing.T) {
	provider := metric.NewMeterProvider()
	defer provider.Shutdown(context.Background())
	meter := provider.Meter("ai-gateway-test")

	m, err := NewMetrics(meter)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	if m.RequestsTotal == nil || m.LatencyMs == nil || m.TokensTotal == nil || m.TTFTMs == nil {
		t.Fatalf("expected all four instruments to be non-nil: %+v", m)
	}

	// Smoke-test that recording doesn't panic.
	m.RequestsTotal.Add(context.Background(), 1)
	m.LatencyMs.Record(context.Background(), 42.0)
	m.TokensTotal.Add(context.Background(), 10)
	m.TTFTMs.Record(context.Background(), 5.0)
}
