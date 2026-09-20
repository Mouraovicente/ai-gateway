package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// DefaultIPFailuresPerMinute is how many failed authentications a single
// source IP may burn per minute before the gateway stops touching the
// tenant store for it at all.
const DefaultIPFailuresPerMinute = 60

// ipEntryTTL is how long an IP's bucket survives without a new failure.
const ipEntryTTL = 10 * time.Minute

// IPLimiter is the pre-auth guard: a token bucket per source IP that only
// authentication *failures* consume. A caller with a valid key is never
// slowed by it; a caller brute-forcing keys runs out of tokens and is
// rejected with 429 before ResolveAPIKey (and therefore before any
// DynamoDB GetItem) is ever reached.
//
// It is keyed by unverified client input on purpose — that is the whole
// point — so the map is bounded by TTL eviction instead of growing with
// every spoofed source.
type IPLimiter struct {
	perMinute int
	entries   sync.Map // ip string -> *ipEntry
	lastSweep atomic.Int64
}

type ipEntry struct {
	mu       sync.Mutex
	lim      *rate.Limiter
	lastSeen atomic.Int64
}

// NewIPLimiter builds a limiter allowing perMinute failed auths per IP
// (<= 0 falls back to DefaultIPFailuresPerMinute).
func NewIPLimiter(perMinute int) *IPLimiter {
	if perMinute <= 0 {
		perMinute = DefaultIPFailuresPerMinute
	}
	l := &IPLimiter{perMinute: perMinute}
	l.lastSweep.Store(time.Now().UnixNano())
	return l
}

func (l *IPLimiter) entry(ip string) *ipEntry {
	if v, ok := l.entries.Load(ip); ok {
		return v.(*ipEntry)
	}
	e := &ipEntry{lim: rate.NewLimiter(rate.Limit(float64(l.perMinute)/60.0), l.perMinute)}
	e.lastSeen.Store(time.Now().UnixNano())
	actual, _ := l.entries.LoadOrStore(ip, e)
	return actual.(*ipEntry)
}

// Allowed reports whether ip still has failure budget left. It never
// consumes a token: an IP that has not failed recently is always allowed,
// so the happy path costs one map lookup.
func (l *IPLimiter) Allowed(ip string) bool {
	v, ok := l.entries.Load(ip)
	if !ok {
		return true
	}
	e := v.(*ipEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lim.Tokens() >= 1
}

// RecordFailure consumes one token for ip, called on every 401.
func (l *IPLimiter) RecordFailure(ip string) {
	e := l.entry(ip)
	now := time.Now()
	e.lastSeen.Store(now.UnixNano())
	e.mu.Lock()
	e.lim.AllowN(now, 1)
	e.mu.Unlock()
	l.maybeSweep(now)
}

// maybeSweep drops entries idle for longer than ipEntryTTL, at most once a
// minute, on the failure path only (a healthy gateway never sweeps).
func (l *IPLimiter) maybeSweep(now time.Time) {
	last := l.lastSweep.Load()
	if now.Sub(time.Unix(0, last)) < time.Minute {
		return
	}
	if !l.lastSweep.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	cutoff := now.Add(-ipEntryTTL).UnixNano()
	l.entries.Range(func(k, v any) bool {
		if v.(*ipEntry).lastSeen.Load() < cutoff {
			l.entries.Delete(k)
		}
		return true
	})
}
