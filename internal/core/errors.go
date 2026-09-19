package core

import "fmt"

// ErrorClass tells resilience whether a BackendError is worth retrying.
type ErrorClass int

const (
	// Transient errors (timeout, 5xx, provider 429, connection refused) may
	// succeed on retry or on the next backend in the cascade.
	Transient ErrorClass = iota
	// Permanent errors (4xx validation, unknown model, invalid key) never
	// succeed on retry; resilience must move straight to the next backend
	// without retrying the same one.
	Permanent
)

func (c ErrorClass) String() string {
	if c == Transient {
		return "transient"
	}
	return "permanent"
}

// BackendError is the typed error every backend adapter must return so that
// resilience can decide whether to retry, fall back, or give up.
type BackendError struct {
	Class  ErrorClass
	Status int
	Err    error
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("backend error (%s): %v", e.Class, e.Err)
}

func (e *BackendError) Unwrap() error {
	return e.Err
}

// IsTransient reports whether this error should trigger a retry/fallback.
func (e *BackendError) IsTransient() bool {
	return e.Class == Transient
}
