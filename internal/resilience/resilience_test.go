package resilience

import (
	"context"
	"errors"
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

type fakeBackend struct {
	calls   int
	results []struct {
		resp core.ChatResponse
		err  error
	}
}

func (f *fakeBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	i := f.calls
	f.calls++
	if i >= len(f.results) {
		i = len(f.results) - 1
	}
	return f.results[i].resp, f.results[i].err
}

func TestCall_SucceedsOnFirstBackend(t *testing.T) {
	ollama := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{resp: core.ChatResponse{Message: core.Message{Content: "ok"}}}}}

	backends := map[string]Backend{"ollama": ollama}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}}

	resp, target, attempts, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if resp.Message.Content != "ok" || target.Provider != "ollama" {
		t.Fatalf("unexpected result: resp=%+v target=%+v", resp, target)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(attempts))
	}
	if ollama.calls != 1 {
		t.Fatalf("expected exactly 1 call on success, got %d", ollama.calls)
	}
}

func TestCall_RetriesTransientThenFallsBackToNextTarget(t *testing.T) {
	transientErr := &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("unavailable")}
	ollama := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{err: transientErr}, {err: transientErr}, {err: transientErr}}}
	openrouter := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{resp: core.ChatResponse{Message: core.Message{Content: "fallback ok"}}}}}

	backends := map[string]Backend{"ollama": ollama, "openrouter": openrouter}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}, {Provider: "openrouter", Model: "m2"}}

	resp, target, attempts, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if resp.Message.Content != "fallback ok" || target.Provider != "openrouter" {
		t.Fatalf("unexpected result: resp=%+v target=%+v", resp, target)
	}
	if ollama.calls != 2 {
		t.Fatalf("expected exactly 2 attempts on ollama (cap of 2), got %d", ollama.calls)
	}
	if len(attempts) != 3 {
		t.Fatalf("expected 3 attempts recorded (2 ollama + 1 openrouter), got %d: %+v", len(attempts), attempts)
	}
}

func TestCall_PermanentErrorSkipsRetryGoesToNextTarget(t *testing.T) {
	permanentErr := &core.BackendError{Class: core.Permanent, Status: 400, Err: errors.New("bad request")}
	ollama := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{err: permanentErr}}}
	openrouter := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{resp: core.ChatResponse{Message: core.Message{Content: "fallback ok"}}}}}

	backends := map[string]Backend{"ollama": ollama, "openrouter": openrouter}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}, {Provider: "openrouter", Model: "m2"}}

	_, target, _, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if target.Provider != "openrouter" {
		t.Fatalf("expected fallback to openrouter, got %+v", target)
	}
	if ollama.calls != 1 {
		t.Fatalf("expected exactly 1 attempt on permanent error (no retry), got %d", ollama.calls)
	}
}

func TestCall_AllBackendsFail_ReturnsErrAllBackendsFailed(t *testing.T) {
	transientErr := &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("unavailable")}
	failing := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{err: transientErr}, {err: transientErr}}}

	backends := map[string]Backend{"ollama": failing}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}}

	_, _, attempts, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if !errors.Is(err, ErrAllBackendsFailed) {
		t.Fatalf("expected ErrAllBackendsFailed, got %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("expected 2 attempts recorded, got %d", len(attempts))
	}
}

type fakeStreamBackend struct {
	chunks []core.ChatChunk
	err    error
}

func (f *fakeStreamBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk, len(f.chunks))
	errs := make(chan error, 1)
	for _, c := range f.chunks {
		chunks <- c
	}
	close(chunks)
	errs <- f.err
	close(errs)
	return chunks, errs
}

func TestCallStream_SucceedsOnFirstTarget(t *testing.T) {
	backend := &fakeStreamBackend{chunks: []core.ChatChunk{
		{Delta: "ol"},
		{Delta: "a", Usage: &core.Usage{PromptTokens: 5, CompletionTokens: 2}},
	}}
	resolve := func(target core.BackendTarget) (core.StreamBackend, error) { return backend, nil }
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}}

	var received []string
	onChunk := func(c core.ChatChunk) error {
		received = append(received, c.Delta)
		return nil
	}

	usage, attempts, err := CallStream(context.Background(), targets, resolve, core.ChatRequest{}, onChunk)
	if err != nil {
		t.Fatalf("CallStream returned error: %v", err)
	}
	if len(received) != 2 || received[0] != "ol" || received[1] != "a" {
		t.Fatalf("unexpected received deltas: %+v", received)
	}
	if usage.PromptTokens != 5 || usage.CompletionTokens != 2 {
		t.Fatalf("unexpected usage: %+v", usage)
	}
	if len(attempts) != 1 || attempts[0].Status != 200 {
		t.Fatalf("unexpected attempts: %+v", attempts)
	}
}
