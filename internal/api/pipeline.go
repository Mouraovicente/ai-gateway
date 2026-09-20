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
	"github.com/Mouraovicente/ai-gateway/internal/stats"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
	"github.com/Mouraovicente/ai-gateway/internal/usage"
	otelattr "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Pipeline wires every module the real /v1/chat/completions handler needs.
// This replaces the Task 5 NewChatHandler, which only called a fixed Ollama model.
type Pipeline struct {
	Auth      auth.Store
	RateLimit ratelimit.Limiter
	Budget    budget.Store
	Routing   *config.Routing
	Backends  map[string]resilience.FullBackend
	Trace     trace.Store
	// Usage and Stats are Task 12's hooks, published/recorded once a request
	// settles. Both may be left nil (the default in every pre-Task-12 test);
	// NewPipelineChatHandler is nil-safe for each and simply skips the
	// usage_publish trace event / stats recording in that case.
	Usage usage.Publisher
	Stats stats.Recorder
	// Tracer and Metrics are Task 13's OTel hooks. Both may be left nil (the
	// default in every pre-Task-13 test); NewPipelineChatHandler falls back
	// to a no-op tracer and simply skips metric recording when Metrics is
	// nil, so OTel stays fully optional.
	Tracer  oteltrace.Tracer
	Metrics *trace.Metrics
}

// NewPipelineChatHandler is the real handler for POST /v1/chat/completions:
// auth -> ratelimit -> budget.Reserve -> router.Resolve -> resilience.Call/CallStream -> budget.Settle,
// recording a trace_event at each stage. Both the non-streaming and the
// streaming path get the full retry/fallback cascade; see
// resilience.CallStream for the streaming-specific rule about stopping
// fallback once the first byte has reached the client.
func NewPipelineChatHandler(p *Pipeline) http.Handler {
	tracer := p.Tracer
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("ai-gateway")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// requestSpan is the root span of the trace: metadata only
		// (request_id/tenant_id/tier/alias/provider/model/http.status_code/
		// error.class), never messages/prompts/completions/keys.
		ctx, requestSpan := tracer.Start(r.Context(), "POST /v1/chat/completions")
		defer requestSpan.End()
		// traceCtx survives a client disconnect: trace events, the budget
		// settle and the usage publish must still happen even when the
		// request context is already cancelled by the time we get there.
		traceCtx := context.WithoutCancel(ctx)
		requestID := RequestIDFromContext(ctx)
		requestSpan.SetAttributes(otelattr.String("request_id", requestID))
		seq := 0
		emit := func(eventType trace.EventType, node string, payload map[string]any) {
			seq++
			if err := p.Trace.RecordEvent(traceCtx, requestID, seq, eventType, node, payload); err != nil {
				slog.Error("trace: failed to record event", "request_id", requestID, "seq", seq, "type", string(eventType), "error", err.Error())
			}
		}

		apiKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if apiKey == "" {
			emit(trace.Auth, "auth", map[string]any{"status": "denied", "reason": "missing_header"})
			requestSpan.SetAttributes(otelattr.Int("http.status_code", http.StatusUnauthorized))
			WriteError(w, requestID, http.StatusUnauthorized, "missing_api_key", "missing Authorization header")
			return
		}
		authCtx, authSpan := tracer.Start(ctx, "auth")
		tenant, err := p.Auth.ResolveAPIKey(authCtx, apiKey)
		authSpan.End()
		if err != nil {
			emit(trace.Auth, "auth", map[string]any{"status": "denied"})
			requestSpan.SetAttributes(otelattr.Int("http.status_code", http.StatusUnauthorized))
			WriteError(w, requestID, http.StatusUnauthorized, "invalid_api_key", "invalid API key")
			return
		}
		emit(trace.Auth, "auth", map[string]any{"tenant_id": tenant.ID, "tier": tenant.Tier})
		requestSpan.SetAttributes(otelattr.String("tenant_id", tenant.ID), otelattr.String("tier", tenant.Tier))

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
		requestSpan.SetAttributes(otelattr.String("alias", body.Model))

		period := time.Now().UTC().Format("2006-01")
		estimated := budget.EstimateTokens(promptBytes, 0)
		reserveCtx, reserveSpan := tracer.Start(ctx, "budget.reserve")
		reservation, err := p.Budget.Reserve(reserveCtx, tenant.ID, period, estimated, tenant.MonthlyTokenBudget)
		reserveSpan.End()
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
			slog.Error("trace: failed to record request", "request_id", requestID, "error", err.Error())
		}

		// respUsage is settled in this defer on every exit path (success,
		// error, client cancel): zero value unless a call below reports real
		// usage, which simply releases the pessimistic estimate made above.
		// It is the single source of truth for both budget.Settle (which
		// only wants the total) and the usage_publish event (which needs the
		// prompt/completion split intact). target/attempts/status/errorClass
		// are filled in by whichever exit path runs (success, all backends
		// failed, or router error), so the usage_event below always reflects
		// what actually happened.
		var respUsage core.Usage
		var target core.BackendTarget
		var attempts []resilience.Attempt
		var ttftMs int
		status := "ok"
		errorClass := ""
		defer func() {
			realTokens := respUsage.PromptTokens + respUsage.CompletionTokens
			_, settleSpan := tracer.Start(traceCtx, "budget.settle")
			if err := p.Budget.Settle(traceCtx, reservation, realTokens); err != nil {
				slog.Error("budget: failed to settle reservation", "request_id", requestID, "error", err.Error())
			}
			settleSpan.End()
			emit(trace.BudgetSettle, "budget", map[string]any{"tenant_id": tenant.ID, "real_tokens": realTokens})
			latencyMs := int(time.Since(start).Milliseconds())
			if p.Usage != nil {
				attemptRecords := make([]usage.AttemptRecord, 0, len(attempts))
				for _, a := range attempts {
					attemptStatus := "ok"
					if a.Err != nil {
						attemptStatus = "error"
					}
					attemptRecords = append(attemptRecords, usage.AttemptRecord{Provider: a.Provider, Model: a.Model, Status: attemptStatus})
				}
				event := usage.Event{
					EventVersion:     1,
					RequestID:        requestID,
					TenantID:         tenant.ID,
					Tier:             tenant.Tier,
					Alias:            body.Model,
					Provider:         target.Provider,
					Model:            target.Model,
					PromptTokens:     respUsage.PromptTokens,
					CompletionTokens: respUsage.CompletionTokens,
					TTFTMs:           ttftMs,
					LatencyMs:        latencyMs,
					Status:           status,
					ErrorClass:       errorClass,
					Attempts:         attemptRecords,
					TS:               time.Now().UTC().Format(time.RFC3339),
				}
				// Bounded so a slow/stuck SQS call can never hold the request
				// (or its deferred cleanup) open indefinitely; publish
				// failures — including this timeout — are logged and traced,
				// never surfaced to the caller.
				publishCtx, cancelPublish := context.WithTimeout(traceCtx, 3*time.Second)
				publishCtx, publishSpan := tracer.Start(publishCtx, "usage.publish")
				err := p.Usage.Publish(publishCtx, event)
				publishSpan.End()
				cancelPublish()
				if err != nil {
					slog.Error("usage: failed to publish usage event", "request_id", requestID, "error", err.Error())
					emit(trace.UsagePublish, "usage", map[string]any{"tenant_id": tenant.ID, "status": "failed"})
				} else {
					emit(trace.UsagePublish, "usage", map[string]any{"tenant_id": tenant.ID, "status": "ok"})
				}
			}
			if p.Stats != nil && status == "ok" {
				p.Stats.Record(body.Model, target.Model, tenant.ID, latencyMs, realTokens)
			}
		}()

		_, routeSpan := tracer.Start(ctx, "route")
		targets, err := router.Resolve(p.Routing, body.Model, tenant.Tier)
		routeSpan.End()
		if err != nil {
			emit(trace.Route, "router", map[string]any{"status": "error"})
			status = "error"
			errorClass = "permanent"
			writeRouterError(w, requestID, err)
			return
		}
		emit(trace.Route, "router", map[string]any{"targets": len(targets)})

		if body.Stream {
			serveStreamPipeline(w, r, p, tracer, targets, chatReq, requestID, &respUsage, &target, &attempts, &ttftMs, &status, &errorClass, emit)
			return
		}

		var resp core.ChatResponse
		resp, target, attempts, err = resilience.Call(ctx, toBackends(p.Backends), targets, chatReq)
		for i, a := range attempts {
			traceBackendCallSpan(ctx, tracer, i, a)
			emit(trace.BackendAttempt, "resilience", attemptPayload(a))
		}
		if err != nil {
			if isClientCancelled(ctx, err) {
				// Client already disconnected: stop quietly, no body. The
				// deferred budget.Settle above still runs with realTokens=0,
				// releasing the estimate. The usage_event still fires (so the
				// billing/consumption pipeline sees every request), but as
				// status "client_closed" with no error_class, and it must
				// never feed Stats (a disconnect isn't a real latency sample).
				status = "client_closed"
				errorClass = ""
				return
			}
			emit(trace.Error, "resilience", map[string]any{"reason": "all_backends_failed"})
			status = "error"
			errorClass = errorClassFromAttempts(attempts)
			requestSpan.SetAttributes(otelattr.Int("http.status_code", http.StatusBadGateway), otelattr.String("error.class", errorClass))
			if p.Metrics != nil {
				metricAttrs := metric.WithAttributes(
					otelattr.String("route", body.Model),
					otelattr.String("model", body.Model),
					otelattr.String("tenant", tenant.ID),
					otelattr.String("status", status),
				)
				p.Metrics.RequestsTotal.Add(ctx, 1, metricAttrs)
				p.Metrics.LatencyMs.Record(ctx, float64(time.Since(start).Milliseconds()), metricAttrs)
			}
			writeAllBackendsFailed(w, requestID, attempts)
			return
		}
		emit(trace.BackendResult, "resilience", map[string]any{"provider": target.Provider, "model": target.Model, "status": "ok", "prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens})

		respUsage = resp.Usage
		requestSpan.SetAttributes(
			otelattr.String("provider", target.Provider),
			otelattr.String("model", target.Model),
			otelattr.Int("http.status_code", http.StatusOK),
		)
		if p.Metrics != nil {
			metricAttrs := metric.WithAttributes(
				otelattr.String("route", body.Model),
				otelattr.String("model", target.Model),
				otelattr.String("tenant", tenant.ID),
				otelattr.String("status", status),
			)
			p.Metrics.RequestsTotal.Add(ctx, 1, metricAttrs)
			p.Metrics.LatencyMs.Record(ctx, float64(time.Since(start).Milliseconds()), metricAttrs)
			p.Metrics.TokensTotal.Add(ctx, int64(resp.Usage.PromptTokens), metric.WithAttributes(
				otelattr.String("route", body.Model), otelattr.String("model", target.Model), otelattr.String("tenant", tenant.ID), otelattr.String("kind", "prompt"),
			))
			p.Metrics.TokensTotal.Add(ctx, int64(resp.Usage.CompletionTokens), metric.WithAttributes(
				otelattr.String("route", body.Model), otelattr.String("model", target.Model), otelattr.String("tenant", tenant.ID), otelattr.String("kind", "completion"),
			))
		}

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

// traceBackendCallSpan records one "backend.call" child span per resilience
// attempt, carrying only metadata (provider/model/attempt index/status) —
// never request/response content.
func traceBackendCallSpan(ctx context.Context, tracer oteltrace.Tracer, index int, a resilience.Attempt) {
	attemptStatus := "ok"
	if a.Err != nil {
		attemptStatus = "error"
	}
	_, span := tracer.Start(ctx, "backend.call")
	span.SetAttributes(
		otelattr.String("provider", a.Provider),
		otelattr.String("model", a.Model),
		otelattr.Int("attempt", index+1),
		otelattr.String("status", attemptStatus),
	)
	span.End()
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
// usage is filled in from the final reported core.Usage (prompt/completion
// split intact) so the caller's deferred budget.Settle and usage_publish see
// real, not estimated, numbers.
func serveStreamPipeline(w http.ResponseWriter, r *http.Request, p *Pipeline, tracer oteltrace.Tracer, targets []core.BackendTarget, chatReq core.ChatRequest, requestID string, usage *core.Usage, outTarget *core.BackendTarget, outAttempts *[]resilience.Attempt, outTTFTMs *int, outStatus *string, outErrorClass *string, emit func(trace.EventType, string, map[string]any)) {
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

	start := time.Now()
	var ttfb time.Duration
	ttfbSet := false
	onChunk := func(chunk core.ChatChunk) error {
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
		}
		if !ttfbSet && chunk.Delta != "" {
			ttfb = time.Since(start)
			ttfbSet = true
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

	result, attempts, streamErr := resilience.CallStream(r.Context(), targets, resolve, chatReq, onChunk)
	for i, a := range attempts {
		traceBackendCallSpan(r.Context(), tracer, i, a)
		emit(trace.BackendAttempt, "resilience", attemptPayload(a))
	}
	*usage = result
	*outAttempts = attempts
	*outTTFTMs = int(ttfb.Milliseconds())

	if streamErr == nil {
		var succeeded resilience.Attempt
		for _, a := range attempts {
			if a.Err == nil {
				succeeded = a
			}
		}
		*outTarget = core.BackendTarget{Provider: succeeded.Provider, Model: succeeded.Model}
		emit(trace.BackendResult, "resilience", map[string]any{
			"provider": succeeded.Provider, "model": succeeded.Model, "status": "ok",
			"latency_ms": time.Since(start).Milliseconds(), "ttft_ms": ttfb.Milliseconds(),
			"prompt_tokens": result.PromptTokens, "completion_tokens": result.CompletionTokens,
		})
	}

	if streamErr != nil && isClientCancelled(r.Context(), streamErr) {
		emit(trace.Error, "resilience", map[string]any{"reason": "client_closed"})
		// Same rule as the non-streaming cancel path: still publish a
		// usage_event (status "client_closed", no error_class), but never
		// feed Stats with a disconnect.
		*outStatus = "client_closed"
		*outErrorClass = ""
		return
	}

	if streamErr != nil {
		// reason is the trace_event's diagnostic label (which cascade stage
		// failed); errorClass is the usage_event's classification of *why*
		// (derived from the last attempt's actual error, never a guess tied
		// to the reason string).
		reason := "all_backends_failed"
		if errors.Is(streamErr, resilience.ErrStreamFailedAfterFirstByte) {
			reason = "stream_failed_after_first_byte"
		}
		*outStatus = "error"
		*outErrorClass = errorClassFromAttempts(attempts)
		emit(trace.Error, "resilience", map[string]any{"reason": reason})
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "backend_stream_failed", "message": scrubErrorText(streamErr), "request_id": requestID}})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}

	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}
