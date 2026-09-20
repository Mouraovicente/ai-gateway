package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Mouraovicente/ai-gateway/internal/auth"
	"github.com/Mouraovicente/ai-gateway/internal/budget"
	"github.com/Mouraovicente/ai-gateway/internal/config"
	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/ratelimit"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/router"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
)

// UsagePublisher is Task 12's hook for publishing a usage event once a
// request settles. Pipeline.Usage may be left nil (the default before Task
// 12 wires a real implementation); NewPipelineChatHandler is nil-safe and
// simply skips the usage_publish trace event in that case.
type UsagePublisher interface {
	Publish(ctx context.Context, requestID string, usage core.Usage) error
}

// Pipeline wires every module the real /v1/chat/completions handler needs.
// This replaces the Task 5 NewChatHandler, which only called a fixed Ollama model.
type Pipeline struct {
	Auth      auth.Store
	RateLimit ratelimit.Limiter
	Budget    budget.Store
	Routing   *config.Routing
	Backends  map[string]resilience.FullBackend
	Trace     trace.Store
	Usage     UsagePublisher
}

// NewPipelineChatHandler is the real handler for POST /v1/chat/completions:
// auth -> ratelimit -> budget.Reserve -> router.Resolve -> resilience.Call/CallStream -> budget.Settle,
// recording a trace_event at each stage. Both the non-streaming and the
// streaming path get the full retry/fallback cascade; see
// resilience.CallStream for the streaming-specific rule about stopping
// fallback once the first byte has reached the client.
func NewPipelineChatHandler(p *Pipeline) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// traceCtx survives a client disconnect: trace events, the budget
		// settle and the usage publish must still happen even when the
		// request context is already cancelled by the time we get there.
		traceCtx := context.WithoutCancel(ctx)
		requestID := RequestIDFromContext(ctx)
		seq := 0
		emit := func(eventType trace.EventType, node string, payload map[string]any) {
			seq++
			if err := p.Trace.RecordEvent(traceCtx, requestID, seq, eventType, node, payload); err != nil {
				slog.Warn("trace: failed to record event", "request_id", requestID, "node", node, "error", err.Error())
			}
		}

		apiKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if apiKey == "" {
			WriteError(w, requestID, http.StatusUnauthorized, "missing_api_key", "missing Authorization header")
			return
		}
		tenant, err := p.Auth.ResolveAPIKey(ctx, apiKey)
		if err != nil {
			emit(trace.Auth, "auth", map[string]any{"status": "denied"})
			WriteError(w, requestID, http.StatusUnauthorized, "invalid_api_key", "invalid API key")
			return
		}
		emit(trace.Auth, "auth", map[string]any{"tenant_id": tenant.ID, "tier": tenant.Tier})

		if allowed, retryAfter := p.RateLimit.Allow(tenant.ID, tenant.RPMLimit); !allowed {
			emit(trace.RateLimit, "ratelimit", map[string]any{"tenant_id": tenant.ID, "retry_after": retryAfter})
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			WriteError(w, requestID, http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		}
		emit(trace.RateLimit, "ratelimit", map[string]any{"tenant_id": tenant.ID, "status": "allowed"})

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			status := http.StatusBadRequest
			msg := "malformed JSON body"
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				status = http.StatusRequestEntityTooLarge
				msg = "request body too large"
			}
			WriteError(w, requestID, status, "invalid_request", msg)
			return
		}
		if len(body.Messages) == 0 {
			WriteError(w, requestID, http.StatusBadRequest, "invalid_request", "messages must not be empty")
			return
		}
		if body.Model == "" {
			WriteError(w, requestID, http.StatusBadRequest, "invalid_request", "model must not be empty")
			return
		}

		messages := make([]core.Message, 0, len(body.Messages))
		promptBytes := 0
		for _, m := range body.Messages {
			messages = append(messages, core.Message{Role: m.Role, Content: m.Content})
			promptBytes += len(m.Content)
		}
		chatReq := core.ChatRequest{RequestID: requestID, TenantID: tenant.ID, Alias: body.Model, Tier: tenant.Tier, Messages: messages, Stream: body.Stream}

		period := time.Now().UTC().Format("2006-01")
		estimated := budget.EstimateTokens(promptBytes, 0)
		reservation, err := p.Budget.Reserve(ctx, tenant.ID, period, estimated, tenant.MonthlyTokenBudget)
		if err != nil {
			emit(trace.BudgetReserve, "budget", map[string]any{"tenant_id": tenant.ID, "status": "denied"})
			if errors.Is(err, budget.ErrBudgetExceeded) {
				WriteError(w, requestID, http.StatusPaymentRequired, "budget_exceeded", "monthly token budget exceeded")
				return
			}
			WriteError(w, requestID, http.StatusInternalServerError, "internal_error", "failed to reserve budget")
			return
		}
		emit(trace.BudgetReserve, "budget", map[string]any{"tenant_id": tenant.ID, "reservation_id": reservation.ID, "estimated_tokens": estimated})

		if err := p.Trace.RecordRequest(traceCtx, requestID, tenant.ID, body.Model); err != nil {
			slog.Warn("trace: failed to record request", "request_id", requestID, "error", err.Error())
		}

		// realTokens is settled in this defer on every exit path (success,
		// error, client cancel): 0 unless a call below reports real usage,
		// which simply releases the pessimistic estimate made above.
		realTokens := 0
		defer func() {
			if err := p.Budget.Settle(traceCtx, reservation, realTokens); err != nil {
				slog.Warn("budget: failed to settle reservation", "request_id", requestID, "error", err.Error())
			}
			emit(trace.BudgetSettle, "budget", map[string]any{"tenant_id": tenant.ID, "real_tokens": realTokens})
			if p.Usage != nil {
				if err := p.Usage.Publish(traceCtx, requestID, core.Usage{CompletionTokens: realTokens}); err != nil {
					slog.Warn("usage: failed to publish usage event", "request_id", requestID, "error", err.Error())
				} else {
					emit(trace.UsagePublish, "usage", map[string]any{"tenant_id": tenant.ID})
				}
			}
		}()

		targets, err := router.Resolve(p.Routing, body.Model, tenant.Tier)
		if err != nil {
			emit(trace.Route, "router", map[string]any{"status": "error"})
			writeRouterError(w, requestID, err)
			return
		}
		emit(trace.Route, "router", map[string]any{"targets": len(targets)})

		if body.Stream {
			serveStreamPipeline(w, r, p, targets, chatReq, requestID, &realTokens, emit)
			return
		}

		resp, target, attempts, err := resilience.Call(ctx, toBackends(p.Backends), targets, chatReq)
		for _, a := range attempts {
			emit(trace.BackendAttempt, "resilience", attemptPayload(a))
		}
		if err != nil {
			if isClientCancelled(ctx, err) {
				// Client already disconnected: stop quietly, no body. The
				// deferred budget.Settle above still runs with realTokens=0,
				// releasing the estimate.
				return
			}
			emit(trace.Error, "resilience", map[string]any{"reason": "all_backends_failed"})
			writeAllBackendsFailed(w, requestID, attempts)
			return
		}
		emit(trace.BackendResult, "resilience", map[string]any{"provider": target.Provider, "model": target.Model})

		realTokens = resp.Usage.PromptTokens + resp.Usage.CompletionTokens

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":      requestID,
			"object":  "chat.completion",
			"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": resp.Message.Content}, "finish_reason": resp.FinishReason}},
			"usage":   map[string]int{"prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens},
		})
	})
}

// isClientCancelled reports whether err (from resilience.Call/CallStream)
// stems from the request context being cancelled or its deadline expiring,
// as opposed to a genuine backend failure.
func isClientCancelled(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}

// serveStreamPipeline runs the streaming cascade through resilience.CallStream,
// forwarding every chunk to the client as SSE as it arrives. Because the SSE
// headers (200 + text/event-stream) are already flushed before any backend is
// called, every failure is communicated in-band: an error before the first
// byte falls back per resilience.CallStream's rules same as the non-streaming
// path; an error after the first byte, or exhaustion of the whole cascade,
// surfaces as the SSE event "backend_stream_failed" followed by [DONE]. A
// client disconnect mid-stream is detected via onChunk and stops quietly,
// without writing an error event to a socket nobody is reading from anymore.
// realTokens is filled in from the final reported usage so the caller's
// deferred budget.Settle sees real, not estimated, tokens.
func serveStreamPipeline(w http.ResponseWriter, r *http.Request, p *Pipeline, targets []core.BackendTarget, chatReq core.ChatRequest, requestID string, realTokens *int, emit func(trace.EventType, string, map[string]any)) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteError(w, requestID, http.StatusInternalServerError, "streaming_unsupported", "response writer does not support flushing")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	resolve := func(target core.BackendTarget) (core.StreamBackend, error) {
		backend, ok := p.Backends[target.Provider]
		if !ok {
			return nil, fmt.Errorf("resilience: no backend registered for provider %q", target.Provider)
		}
		return backend, nil
	}

	onChunk := func(chunk core.ChatChunk) error {
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
		}
		payload, _ := json.Marshal(map[string]any{
			"id":      requestID,
			"object":  "chat.completion.chunk",
			"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": chunk.Delta}, "finish_reason": chunk.FinishReason}},
		})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
		return nil
	}

	usage, attempts, streamErr := resilience.CallStream(r.Context(), targets, resolve, chatReq, onChunk)
	for _, a := range attempts {
		emit(trace.BackendAttempt, "resilience", attemptPayload(a))
	}
	*realTokens = usage.PromptTokens + usage.CompletionTokens

	if streamErr != nil && isClientCancelled(r.Context(), streamErr) {
		emit(trace.Error, "resilience", map[string]any{"reason": "client_closed"})
		return
	}

	if streamErr != nil {
		reason := "all_backends_failed"
		if errors.Is(streamErr, resilience.ErrStreamFailedAfterFirstByte) {
			reason = "stream_failed_after_first_byte"
		}
		emit(trace.Error, "resilience", map[string]any{"reason": reason})
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "backend_stream_failed", "message": streamErr.Error(), "request_id": requestID}})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}

	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}
