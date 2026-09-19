package ratelimit

import "testing"

func TestInMemoryLimiter_AllowsUpToRPMThenBlocks(t *testing.T) {
	l := NewInMemoryLimiter()

	allowed, _ := l.Allow("tenant-1", 1)
	if !allowed {
		t.Fatalf("expected first request to be allowed")
	}

	allowed, retryAfter := l.Allow("tenant-1", 1)
	if allowed {
		t.Fatalf("expected second immediate request to be blocked with rpm=1")
	}
	if retryAfter <= 0 {
		t.Fatalf("expected a positive Retry-After, got %d", retryAfter)
	}
}

func TestInMemoryLimiter_RpmChangeTakesEffectImmediately(t *testing.T) {
	l := NewInMemoryLimiter()

	if allowed, _ := l.Allow("tenant-1", 1); !allowed {
		t.Fatalf("expected first request to be allowed")
	}
	if allowed, _ := l.Allow("tenant-1", 1); allowed {
		t.Fatalf("expected second immediate request to be blocked with rpm=1")
	}

	allowed, _ := l.Allow("tenant-1", 1000)
	if !allowed {
		t.Fatalf("expected request to be allowed immediately after rpm increased to 1000")
	}
}

func TestInMemoryLimiter_TracksTenantsIndependently(t *testing.T) {
	l := NewInMemoryLimiter()

	if allowed, _ := l.Allow("tenant-a", 1); !allowed {
		t.Fatalf("expected tenant-a first request allowed")
	}
	if allowed, _ := l.Allow("tenant-b", 1); !allowed {
		t.Fatalf("expected tenant-b first request allowed independently of tenant-a")
	}
}
