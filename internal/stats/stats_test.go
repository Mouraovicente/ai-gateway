package stats

import "testing"

func TestRecorder_ComputesPercentilesPerRoute(t *testing.T) {
	r := NewInMemoryRecorder(0) // window=0 means "keep everything", for deterministic tests
	for _, latency := range []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100} {
		r.Record("nuva/fast", "qwen2.5-coder:1.5b", "tenant-1", latency, 50)
	}

	report := r.Snapshot()
	route, ok := report.Routes["nuva/fast"]
	if !ok {
		t.Fatalf("expected route nuva/fast in report, got %+v", report)
	}
	if route.Count != 10 {
		t.Fatalf("expected 10 samples, got %d", route.Count)
	}
	if route.P50LatencyMs < 40 || route.P50LatencyMs > 60 {
		t.Fatalf("unexpected p50: %d", route.P50LatencyMs)
	}
	if route.P99LatencyMs < 90 {
		t.Fatalf("unexpected p99: %d", route.P99LatencyMs)
	}
}

func TestRecorder_EmptyReportHasNoRoutes(t *testing.T) {
	r := NewInMemoryRecorder(0)
	report := r.Snapshot()
	if len(report.Routes) != 0 {
		t.Fatalf("expected no routes recorded yet, got %+v", report.Routes)
	}
}
