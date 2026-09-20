package stats

import (
	"sort"
	"sync"
	"time"
)

// RouteStats is the computed percentile summary for one route (alias/model pair).
type RouteStats struct {
	Count        int
	P50LatencyMs int
	P90LatencyMs int
	P99LatencyMs int
	P50Tokens    int
	P90Tokens    int
	P99Tokens    int
}

// Report is the full /stats response body.
type Report struct {
	Routes map[string]RouteStats
}

type sample struct {
	latencyMs int
	tokens    int
	at        time.Time
}

// Recorder accumulates latency/token samples per route/model/tenant and
// computes percentiles over a sliding time window (window=0 keeps all
// samples, useful for deterministic tests).
type Recorder interface {
	Record(route, model, tenantID string, latencyMs, tokens int)
	Snapshot() Report
}

type inMemoryRecorder struct {
	mu      sync.Mutex
	window  time.Duration
	samples map[string][]sample // keyed by route
}

// NewInMemoryRecorder builds an in-memory Recorder keeping samples for the
// given sliding window (0 means "keep everything").
func NewInMemoryRecorder(window time.Duration) Recorder {
	return &inMemoryRecorder{window: window, samples: make(map[string][]sample)}
}

func (r *inMemoryRecorder) Record(route, model, tenantID string, latencyMs, tokens int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples[route] = append(r.samples[route], sample{latencyMs: latencyMs, tokens: tokens, at: time.Now()})
}

func (r *inMemoryRecorder) Snapshot() Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	report := Report{Routes: make(map[string]RouteStats)}
	var cutoff time.Time
	if r.window > 0 {
		cutoff = time.Now().Add(-r.window)
	}

	for route, samples := range r.samples {
		var latencies, tokens []int
		for _, s := range samples {
			if r.window > 0 && s.at.Before(cutoff) {
				continue
			}
			latencies = append(latencies, s.latencyMs)
			tokens = append(tokens, s.tokens)
		}
		if len(latencies) == 0 {
			continue
		}
		sort.Ints(latencies)
		sort.Ints(tokens)
		report.Routes[route] = RouteStats{
			Count:        len(latencies),
			P50LatencyMs: percentile(latencies, 50),
			P90LatencyMs: percentile(latencies, 90),
			P99LatencyMs: percentile(latencies, 99),
			P50Tokens:    percentile(tokens, 50),
			P90Tokens:    percentile(tokens, 90),
			P99Tokens:    percentile(tokens, 99),
		}
	}
	return report
}

func percentile(sorted []int, p int) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
