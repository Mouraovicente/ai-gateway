package resilience

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// ErrAllBackendsFailed is returned when every target in the cascade failed.
var ErrAllBackendsFailed = errors.New("resilience: all backends failed")

// ErrStreamFailedAfterFirstByte is returned by CallStream when a backend
// fails after at least one chunk with a non-empty Delta was already
// delivered to the caller's onChunk. Past that point, no retry or fallback
// is attempted: the client has already received partial output, so mixing
// in a second backend's output would be incoherent.
var ErrStreamFailedAfterFirstByte = errors.New("resilience: backend stream failed after first byte was sent")

// maxAttemptsPerBackend caps retries on the same backend before moving to
// the next target in the cascade (spec: teto de 2 tentativas por backend).
const maxAttemptsPerBackend = 2

// Backend is the common interface satisfied by ollama.Client and openrouter.Client
// (and gemini.Client from Task 10) for the purposes of resilience.Call.
type Backend interface {
	Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error)
}

// FullBackend is satisfied by every backend adapter (ollama.Client,
// openrouter.Client, gemini.Client): it can be called both non-streaming
// (Backend) and streaming (core.StreamBackend). Pipeline.Backends (Task 11)
// is keyed by provider name and holds values of this type.
type FullBackend interface {
	Backend
	core.StreamBackend
}

// Attempt records one call made while resolving a request, for the
// all_backends_failed error body, usage_event.attempts and the OTel
// backend.call span (StartedAt/LatencyMs give it a real, non-zero-duration
// timestamp instead of being created after the fact with no duration).
type Attempt struct {
	Provider  string
	Model     string
	Status    int
	Err       error
	StartedAt time.Time
	LatencyMs int
}

// Call tries each target in order. For a given target, it retries up to
// maxAttemptsPerBackend times with exponential backoff + jitter, but only
// when the error is Transient; a Permanent error moves immediately to the
// next target. If every target fails, it returns ErrAllBackendsFailed
// alongside every Attempt made, for the 502 response body.
func Call(ctx context.Context, backends map[string]Backend, targets []core.BackendTarget, req core.ChatRequest) (core.ChatResponse, core.BackendTarget, []Attempt, error) {
	var attempts []Attempt

	for _, target := range targets {
		backend, ok := backends[target.Provider]
		if !ok {
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Err: errors.New("resilience: no backend registered for provider")})
			continue
		}

		for attempt := 0; attempt < maxAttemptsPerBackend; attempt++ {
			startedAt := time.Now()
			resp, err := backend.Chat(ctx, target.Model, req)
			latencyMs := int(time.Since(startedAt).Milliseconds())
			if err == nil {
				attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: 200, StartedAt: startedAt, LatencyMs: latencyMs})
				return resp, target, attempts, nil
			}

			var be *core.BackendError
			status := 0
			if errors.As(err, &be) {
				status = be.Status
			}
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: status, Err: err, StartedAt: startedAt, LatencyMs: latencyMs})

			if !errors.As(err, &be) || !be.IsTransient() {
				break // permanent error: stop retrying this backend, move to next target
			}
			if attempt == maxAttemptsPerBackend-1 {
				break // exhausted retries on this backend, move to next target
			}
			if err := backoff(ctx, attempt); err != nil {
				return core.ChatResponse{}, core.BackendTarget{}, attempts, fmt.Errorf("resilience: cancelled during backoff: %w", err)
			}
		}
	}

	return core.ChatResponse{}, core.BackendTarget{}, attempts, ErrAllBackendsFailed
}

// backoff sleeps for 200ms*2^attempt plus jitter, or returns ctx.Err() if
// the context is cancelled first, so a client disconnect during a retry
// wait doesn't leave the call blocked for the full backoff window.
func backoff(ctx context.Context, attempt int) error {
	base := 200 * time.Millisecond * time.Duration(1<<attempt)
	jitter := time.Duration(rand.Int63n(int64(base / 2)))
	select {
	case <-time.After(base + jitter):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CallStream tries each target's ChatStream in order. Before the first
// chunk with a non-empty Delta is delivered to onChunk, it follows the same
// retry/fallback policy as Call: up to maxAttemptsPerBackend retries on
// Transient errors, then falls back to the next target; a Permanent error
// moves straight to the next target. Once the first byte has been sent to
// onChunk, no more retry or fallback happens — any later error stops the
// stream immediately with ErrStreamFailedAfterFirstByte, wrapping the
// underlying error, alongside whatever core.Usage the stream reported
// before failing (zero value if it failed before any usage chunk arrived).
func CallStream(ctx context.Context, targets []core.BackendTarget, resolve func(core.BackendTarget) (core.StreamBackend, error), req core.ChatRequest, onChunk func(core.ChatChunk) error) (core.Usage, []Attempt, error) {
	var attempts []Attempt

	for _, target := range targets {
		backend, err := resolve(target)
		if err != nil {
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Err: err})
			continue
		}

		for attempt := 0; attempt < maxAttemptsPerBackend; attempt++ {
			startedAt := time.Now()
			usage, firstByteSent, streamErr := runStream(ctx, backend, target.Model, req, onChunk)
			latencyMs := int(time.Since(startedAt).Milliseconds())

			if streamErr == nil {
				attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: 200, StartedAt: startedAt, LatencyMs: latencyMs})
				return usage, attempts, nil
			}

			var be *core.BackendError
			status := 0
			if errors.As(streamErr, &be) {
				status = be.Status
			}
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: status, Err: streamErr, StartedAt: startedAt, LatencyMs: latencyMs})

			if firstByteSent {
				return usage, attempts, fmt.Errorf("%w: %v", ErrStreamFailedAfterFirstByte, streamErr)
			}

			if !errors.As(streamErr, &be) || !be.IsTransient() {
				break // permanent error before first byte: move to next target
			}
			if attempt == maxAttemptsPerBackend-1 {
				break // exhausted retries on this backend before first byte: move to next target
			}
			if err := backoff(ctx, attempt); err != nil {
				return core.Usage{}, attempts, fmt.Errorf("resilience: cancelled during backoff: %w", err)
			}
		}
	}

	return core.Usage{}, attempts, ErrAllBackendsFailed
}

// runStream drains one ChatStream call, forwarding every chunk with a
// non-empty Delta (and every chunk carrying Usage, even delta-less ones) to
// onChunk. It reports whether at least one Delta chunk was sent before any
// error, and the Usage from the last chunk that carried one (Gemini bundles
// Delta and Usage in the same final chunk; Ollama/OpenRouter send a
// separate delta-less usage chunk — both shapes are handled the same way
// here, since the Usage capture is unconditional and independent of the
// Delta-forwarding branch).
func runStream(ctx context.Context, backend core.StreamBackend, model string, req core.ChatRequest, onChunk func(core.ChatChunk) error) (core.Usage, bool, error) {
	chunks, errs := backend.ChatStream(ctx, model, req)
	var usage core.Usage
	firstByteSent := false

	for chunk := range chunks {
		if chunk.Delta != "" {
			if err := onChunk(chunk); err != nil {
				return usage, firstByteSent, err
			}
			firstByteSent = true
		} else if chunk.Usage != nil {
			if err := onChunk(chunk); err != nil {
				return usage, firstByteSent, err
			}
		}
		if chunk.Usage != nil {
			usage = *chunk.Usage
		}
	}
	if err := <-errs; err != nil {
		return usage, firstByteSent, err
	}
	return usage, firstByteSent, nil
}
