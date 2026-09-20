package stats

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// maxSamplesPerKey bounds memory per (route, model, tenant) key: once full,
// the oldest sample is dropped for each new one (ring buffer), so a single
// hot route/tenant combination can never grow the recorder unbounded.
const maxSamplesPerKey = 10_000

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

// Report is the /stats response body for one caller: Routes holds only that
// caller's own (route, model) entries (never another tenant's), and Total is
// the global rollup across every tenant with no tenant id attached, so a
// caller can see aggregate health without seeing anyone else's identity or
// per-key breakdown.
type Report struct {
	Routes map[string]RouteStats
	Total  RouteStats
}

type sample struct {
	latencyMs int
	tokens    int
	at        time.Time
}

// key identifies one (route, model, tenant) bucket. route/model form the
// caller-visible label; tenant scopes visibility in Snapshot.
type key struct {
	route  string
	model  string
	tenant string
}

func (k key) label() string {
	return k.route + "/" + k.model
}

// Recorder accumulates latency/token samples per (route, model, tenant) and
// computes percentiles over a sliding time window (window=0 keeps all
// samples, useful for deterministic tests).
type Recorder interface {
	Record(route, model, tenantID string, latencyMs, tokens int)
	// Snapshot returns tenantID's own per-route entries plus the global
	// total (no tenant ids in the total). tenantID == "" returns every
	// route with no tenant scoping (used for internal/ops access only —
	// never wire this to an unauthenticated or cross-tenant HTTP path).
	Snapshot(tenantID string) Report
}

type inMemoryRecorder struct {
	mu      sync.Mutex
	window  time.Duration
	samples map[key][]sample
}

// NewInMemoryRecorder builds an in-memory Recorder keeping samples for the
// given sliding window (0 means "keep everything").
func NewInMemoryRecorder(window time.Duration) Recorder {
	return &inMemoryRecorder{window: window, samples: make(map[key][]sample)}
}

func (r *inMemoryRecorder) Record(route, model, tenantID string, latencyMs, tokens int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key{route: route, model: model, tenant: tenantID}
	r.evictLocked(k)
	s := append(r.samples[k], sample{latencyMs: latencyMs, tokens: tokens, at: time.Now()})
	if len(s) > maxSamplesPerKey {
		s = s[len(s)-maxSamplesPerKey:]
	}
	r.samples[k] = s
}

// evictLocked drops samples older than the window for k. Caller holds r.mu.
func (r *inMemoryRecorder) evictLocked(k key) {
	if r.window <= 0 {
		return
	}
	cutoff := time.Now().Add(-r.window)
	s := r.samples[k]
	i := 0
	for i < len(s) && s[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		r.samples[k] = append([]sample(nil), s[i:]...)
	}
}

func (r *inMemoryRecorder) Snapshot(tenantID string) Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	report := Report{Routes: make(map[string]RouteStats)}
	var allLatencies, allTokens []int
	var tenantLatencies, tenantTokens map[string][]int
	if tenantID != "" {
		tenantLatencies = make(map[string][]int)
		tenantTokens = make(map[string][]int)
	}

	for k := range r.samples {
		r.evictLocked(k)
	}
	for k, samples := range r.samples {
		for _, s := range samples {
			allLatencies = append(allLatencies, s.latencyMs)
			allTokens = append(allTokens, s.tokens)
			if tenantID != "" && k.tenant == tenantID {
				label := k.label()
				tenantLatencies[label] = append(tenantLatencies[label], s.latencyMs)
				tenantTokens[label] = append(tenantTokens[label], s.tokens)
			}
		}
		if tenantID == "" {
			label := strings.Join([]string{k.route, k.model, k.tenant}, "/")
			report.Routes[label] = routeStatsFromSamples(sampleValues(samples))
		}
	}
	for label, latencies := range tenantLatencies {
		report.Routes[label] = routeStats(latencies, tenantTokens[label])
	}
	report.Total = routeStats(allLatencies, allTokens)
	return report
}

func sampleValues(samples []sample) ([]int, []int) {
	latencies := make([]int, len(samples))
	tokens := make([]int, len(samples))
	for i, s := range samples {
		latencies[i] = s.latencyMs
		tokens[i] = s.tokens
	}
	return latencies, tokens
}

func routeStatsFromSamples(latencies, tokens []int) RouteStats {
	return routeStats(latencies, tokens)
}

func routeStats(latencies, tokens []int) RouteStats {
	if len(latencies) == 0 {
		return RouteStats{}
	}
	sortedLatencies := append([]int(nil), latencies...)
	sortedTokens := append([]int(nil), tokens...)
	sort.Ints(sortedLatencies)
	sort.Ints(sortedTokens)
	return RouteStats{
		Count:        len(sortedLatencies),
		P50LatencyMs: percentile(sortedLatencies, 50),
		P90LatencyMs: percentile(sortedLatencies, 90),
		P99LatencyMs: percentile(sortedLatencies, 99),
		P50Tokens:    percentile(sortedTokens, 50),
		P90Tokens:    percentile(sortedTokens, 90),
		P99Tokens:    percentile(sortedTokens, 99),
	}
}

// percentile uses the nearest-rank method (index = p*n/100 into the sorted
// slice), not linear interpolation between the two closest ranks: simpler,
// and the value returned is always an actual observed sample, never an
// average of two samples.
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
