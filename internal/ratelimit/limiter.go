package ratelimit

import (
	"math"
	"sync"

	"golang.org/x/time/rate"
)

// Limiter decides whether a tenant may make a request right now, given its
// requests-per-minute limit.
//
// Allow must only be called with an authenticated tenant ID, i.e. after
// auth.Store.ResolveAPIKey has succeeded — never before. Calling it earlier
// (e.g. keyed by raw, unverified input) would let an unauthenticated caller
// create arbitrary entries in the tenant map below.
type Limiter interface {
	// Allow reports whether the request is permitted. When false, retryAfterSeconds
	// is the integer number of seconds the caller should wait before retrying.
	Allow(tenantID string, rpm int) (allowed bool, retryAfterSeconds int)
}

// maxRetryAfterSeconds caps the Retry-After the gateway ever advertises.
const maxRetryAfterSeconds = 300

// tenantLimiter pairs a rate.Limiter with the rpm it was last configured
// for, so a tier change (different rpm for the same tenant) can be detected
// and applied instead of silently reusing the original limit forever.
type tenantLimiter struct {
	mu  sync.Mutex
	lim *rate.Limiter
	rpm int
}

func (t *tenantLimiter) allow(rpm int) (bool, int) {
	t.mu.Lock()
	if rpm != t.rpm {
		// Replace the limiter outright rather than SetLimit/SetBurst: those
		// keep the current (possibly near-empty) token count, so a tier
		// bump wouldn't unblock the tenant until tokens refill. A fresh
		// limiter starts full, so a higher rpm takes effect immediately.
		t.rpm = rpm
		t.lim = rate.NewLimiter(rate.Limit(float64(rpm)/60.0), rpm)
	}
	lim := t.lim
	t.mu.Unlock()

	if lim.Allow() {
		return true, 0
	}
	reservation := lim.Reserve()
	delay := reservation.Delay()
	reservation.Cancel()
	retryAfter := int(math.Ceil(delay.Seconds()))
	if retryAfter < 1 {
		retryAfter = 1
	}
	// Defense in depth: rate.InfDuration (an rpm that made the limiter
	// unusable) would otherwise be handed to the client as a Retry-After of
	// ~292 years, which a well-behaved client may honour.
	if retryAfter > maxRetryAfterSeconds {
		retryAfter = maxRetryAfterSeconds
	}
	return false, retryAfter
}

type inMemoryLimiter struct {
	// limiters: tenantID -> *tenantLimiter.
	//
	// ponytail: no eviction — an entry is created and kept forever for every
	// distinct tenant ID seen. Memory grows with the number of distinct
	// tenants, ceiling is roughly thousands of tenants per instance before
	// it matters. Upgrade path: TTL sweep (drop entries idle > N minutes) or
	// an LRU cap, once tenant churn (short-lived/rotating tenant IDs) shows
	// up in practice.
	limiters sync.Map
}

// NewInMemoryLimiter returns a per-instance token-bucket limiter keyed by
// tenant id. This is deliberately per-instance: a second gateway replica
// multiplies the effective limit.
func NewInMemoryLimiter() Limiter {
	return &inMemoryLimiter{}
}

func (l *inMemoryLimiter) Allow(tenantID string, rpm int) (bool, int) {
	tlAny, _ := l.limiters.LoadOrStore(tenantID, &tenantLimiter{
		lim: rate.NewLimiter(rate.Limit(float64(rpm)/60.0), rpm),
		rpm: rpm,
	})
	tl := tlAny.(*tenantLimiter)
	return tl.allow(rpm)
}
