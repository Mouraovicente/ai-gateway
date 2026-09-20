package stats

import (
	"testing"
	"time"
)

func TestRecorder_ComputesPercentilesPerRoute(t *testing.T) {
	r := NewInMemoryRecorder(0) // window=0 means "keep everything", for deterministic tests
	for _, latency := range []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100} {
		r.Record("nuva/fast", "qwen2.5-coder:1.5b", "tenant-1", latency, 50)
	}

	report := r.Snapshot("tenant-1")
	route, ok := report.Routes["nuva/fast/qwen2.5-coder:1.5b"]
	if !ok {
		t.Fatalf("expected route nuva/fast/qwen2.5-coder:1.5b in report, got %+v", report)
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
	if report.Total.Count != 10 {
		t.Fatalf("expected total count 10, got %d", report.Total.Count)
	}
}

func TestRecorder_EmptyReportHasNoRoutes(t *testing.T) {
	r := NewInMemoryRecorder(0)
	report := r.Snapshot("tenant-1")
	if len(report.Routes) != 0 {
		t.Fatalf("expected no routes recorded yet, got %+v", report.Routes)
	}
	if report.Total.Count != 0 {
		t.Fatalf("expected total count 0, got %d", report.Total.Count)
	}
}

func TestRecorder_TenantCannotSeeAnotherTenantsRoutes(t *testing.T) {
	r := NewInMemoryRecorder(0)
	r.Record("nuva/fast", "m1", "tenant-a", 10, 5)
	r.Record("nuva/fast", "m1", "tenant-b", 999, 999)

	reportA := r.Snapshot("tenant-a")
	if len(reportA.Routes) != 1 {
		t.Fatalf("expected tenant-a to see exactly one route, got %+v", reportA.Routes)
	}
	route, ok := reportA.Routes["nuva/fast/m1"]
	if !ok {
		t.Fatalf("expected tenant-a's own route, got %+v", reportA.Routes)
	}
	if route.Count != 1 || route.P50LatencyMs != 10 {
		t.Fatalf("expected tenant-a's route to reflect only its own sample, got %+v", route)
	}
	// Total is a global rollup: it must include tenant-b's sample too, but
	// with no tenant id anywhere in the response.
	if reportA.Total.Count != 2 {
		t.Fatalf("expected global total across both tenants, got %+v", reportA.Total)
	}

	reportB := r.Snapshot("tenant-b")
	if len(reportB.Routes) != 1 {
		t.Fatalf("expected tenant-b to see exactly its own one route, got %+v", reportB.Routes)
	}
	if reportB.Routes["nuva/fast/m1"].P50LatencyMs != 999 {
		t.Fatalf("expected tenant-b's own route, got %+v", reportB.Routes)
	}
}

func TestRecorder_EvictsSamplesOlderThanWindow(t *testing.T) {
	r := NewInMemoryRecorder(50 * time.Millisecond)
	r.Record("nuva/fast", "m1", "tenant-1", 10, 5)
	time.Sleep(80 * time.Millisecond)
	r.Record("nuva/fast", "m1", "tenant-1", 20, 5)

	report := r.Snapshot("tenant-1")
	route := report.Routes["nuva/fast/m1"]
	if route.Count != 1 {
		t.Fatalf("expected only the fresh sample to survive eviction, got count %d", route.Count)
	}
	if route.P50LatencyMs != 20 {
		t.Fatalf("expected the surviving sample's latency 20, got %d", route.P50LatencyMs)
	}
}

func TestRecorder_EvictsKeyEntirelyWhenAllSamplesExpire(t *testing.T) {
	r := NewInMemoryRecorder(10 * time.Millisecond)
	ir := r.(*inMemoryRecorder)
	r.Record("nuva/fast", "m1", "tenant-1", 10, 5)
	time.Sleep(20 * time.Millisecond)

	// Snapshot triggers eviction of every key, including ones with no new
	// samples recorded since expiring.
	report := r.Snapshot("tenant-1")
	if _, ok := report.Routes["nuva/fast/m1"]; ok {
		t.Fatalf("expected expired route to disappear from snapshot, got %+v", report.Routes)
	}

	ir.mu.Lock()
	_, exists := ir.samples[key{route: "nuva/fast", model: "m1", tenant: "tenant-1"}]
	ir.mu.Unlock()
	if exists {
		t.Fatalf("expected map entry for expired key to be deleted, not just emptied")
	}
}

func TestRecorder_CapsSamplesPerKey(t *testing.T) {
	r := NewInMemoryRecorder(0)
	ir := r.(*inMemoryRecorder)
	for i := 0; i < maxSamplesPerKey+50; i++ {
		r.Record("nuva/fast", "m1", "tenant-1", i, 1)
	}
	ir.mu.Lock()
	got := len(ir.samples[key{route: "nuva/fast", model: "m1", tenant: "tenant-1"}])
	ir.mu.Unlock()
	if got != maxSamplesPerKey {
		t.Fatalf("expected samples capped at %d, got %d", maxSamplesPerKey, got)
	}
}
