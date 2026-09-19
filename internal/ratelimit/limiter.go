package ratelimit

import (
	"math"
	"sync"

	"golang.org/x/time/rate"
)

// Limiter decides whether a tenant may make a request right now, given its
// requests-per-minute limit.
type Limiter interface {
	// Allow reports whether the request is permitted. When false, retryAfterSeconds
	// is the integer number of seconds the caller should wait before retrying.
	Allow(tenantID string, rpm int) (allowed bool, retryAfterSeconds int)
}

type inMemoryLimiter struct {
	limiters sync.Map // tenantID -> *rate.Limiter
}

// NewInMemoryLimiter returns a per-instance token-bucket limiter keyed by
// tenant id. This is deliberately per-instance: a second gateway replica
// multiplies the effective limit.
func NewInMemoryLimiter() Limiter {
	return &inMemoryLimiter{}
}

func (l *inMemoryLimiter) Allow(tenantID string, rpm int) (bool, int) {
	limiterAny, _ := l.limiters.LoadOrStore(tenantID, rate.NewLimiter(rate.Limit(float64(rpm)/60.0), rpm))
	limiter := limiterAny.(*rate.Limiter)

	if limiter.Allow() {
		return true, 0
	}
	reservation := limiter.Reserve()
	delay := reservation.Delay()
	reservation.Cancel()
	retryAfter := int(math.Ceil(delay.Seconds()))
	if retryAfter < 1 {
		retryAfter = 1
	}
	return false, retryAfter
}
