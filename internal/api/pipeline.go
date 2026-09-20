package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
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
	// Logger receives every internal (non-caller-facing) log line this
	// pipeline emits. Defaults to slog.Default() so pre-Task-13-style tests
	// that never set it keep working, but production wiring (main.go) sets
	// it to the same *slog.Logger passed to slog.SetDefault, so trace store
	// failures etc. go through the structured, redacted logger instead of
	// slog's bare default (which never applied trace.NewLogger's redaction).
	Logger *slog.Logger
	// IPLimit is the pre-auth guard (see ratelimit.IPLimiter): consulted
	// before ResolveAPIKey, charged only on authentication failures. nil
	// disables it (the default in tests).
	IPLimit *ratelimit.IPLimiter
	// StreamWriteTimeout bounds a single SSE write, so a client that opens
	// a stream and then stops reading cannot pin the handler goroutine (and
	// the paid upstream connection behind it) forever. 0 means
	// defaultStreamWriteTimeout.
	StreamWriteTimeout time.Duration
}

// defaultStreamWriteTimeout is the per-chunk SSE write deadline. Per chunk,
// never a server-wide WriteTimeout: a long legitimate stream must survive.
const defaultStreamWriteTimeout = 30 * time.Second

// maxMessages and maxPromptBytes bound the decoded body beyond the 1 MiB
// wire limit: tens of thousands of tiny messages fit inside 1 MiB and each
// one becomes a core.Message that is re-serialized upstream.
const (
	maxMessages    = 64
	maxPromptBytes = 256 << 10
)

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
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}
	streamWriteTimeout := p.StreamWriteTimeout
	if streamWriteTimeout <= 0 {
		streamWriteTimeout = defaultStreamWriteTimeout
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// requestSpan is the root span of the trace: metadata only
		// (request_id/tenant_id/tier/alias/provider/model/http.status_code/
		// error.class), never messages/prompts/completions/keys. Every
		// exit path below goes through writeErr or sets httpStatus directly
		// (success, streaming) so this single deferred func can record the
		// real wire status on the root span regardless of which path ran.
		ctx, requestSpan := tracer.Start(r.Context(), "POST /v1/chat/completions")
		defer requestSpan.End()
		var httpStatus int
		defer func() {
			if httpStatus != 0 {
				requestSpan.SetAttributes(otelattr.Int("http.status_code", httpStatus))
			}
		}()
		// traceCtx survives a client disconnect: trace events, the budget
		// settle and the usage publish must still happen even when the
		// request context is already cancelled by the time we get there.
		traceCtx := context.WithoutCancel(ctx)
		requestID := RequestIDFromContext(ctx)
		requestSpan.SetAttributes(otelattr.String("request_id", requestID))
		seq := 0
		clientRequestID := ClientRequestIDFromContext(ctx)
		emit := func(eventType trace.EventType, node string, payload map[string]any) {
			seq++
			if clientRequestID != "" {
				if payload == nil {
					payload = map[string]any{}
				}
				payload["client_request_id"] = clientRequestID
			}
			// Bounded even though traceCtx is uncancellable: under a
			// DynamoDB throttle the SDK's own retries would otherwise hold
			// this goroutine (and, in the defer below, shutdown) open.
			eventCtx, cancelEvent := context.WithTimeout(traceCtx, 5*time.Second)
			defer cancelEvent()
			if err := p.Trace.RecordEvent(eventCtx, requestID, seq, eventType, node, payload); err != nil {
				logger.Error("trace: failed to record event", "request_id", requestID, "seq", seq, "type", string(eventType), "error", err.Error())
			}
		}
		// writeErr is the single write path for every error response below,
		// so httpStatus (and therefore the root span's http.status_code) is
		// always in sync with what actually went out on the wire.
		writeErr := func(status int, code, msg string) {
			httpStatus = status
			WriteError(w, requestID, status, code, msg)
		}

		clientIP := ClientIP(r)
		// Pre-auth: an IP that already burned its failure budget is
		// rejected here, before any store lookup happens on its behalf.
		if p.IPLimit != nil && !p.IPLimit.Allowed(clientIP) {
			w.Header().Set("Retry-After", "60")
			writeErr(http.StatusTooManyRequests, "rate_limited", "too many failed authentication attempts")
			return
		}
		denyAuth := func(code, msg string) {
			if p.IPLimit != nil {
				p.IPLimit.RecordFailure(clientIP)
			}
			writeErr(http.StatusUnauthorized, code, msg)
		}

		apiKey, keyOK := ParseBearer(r.Header.Get("Authorization"))
		if !keyOK {
			emit(trace.Auth, "auth", map[string]any{"status": "denied", "reason": "missing_header"})
			denyAuth("missing_api_key", "missing or malformed Authorization header")
			return
		}
		authCtx, authSpan := tracer.Start(ctx, "auth")
		tenant, err := p.Auth.ResolveAPIKey(authCtx, apiKey)
		authSpan.End()
		if err != nil {
			// A store outage is not a credential problem: answering every
			// caller "your key is invalid" during a DynamoDB throttle is
			// both the wrong instruction to the client and the wrong
			// diagnosis for whoever is on call.
			if errors.Is(err, auth.ErrInvalidTenantConfig) {
				// Seeding/config error, not an outage: retrying will not
				// fix it, so do not advertise Retry-After.
				logger.Error("auth: tenant record is unusable", "request_id", requestID, "error", err.Error())
				emit(trace.Auth, "auth", map[string]any{"status": "error", "reason": "tenant_misconfigured"})
				writeErr(http.StatusInternalServerError, "tenant_misconfigured", "tenant record is incomplete")
				return
			}
			if !errors.Is(err, auth.ErrUnknownAPIKey) {
				logger.Error("auth: tenant store unavailable", "request_id", requestID, "error", err.Error())
				emit(trace.Auth, "auth", map[string]any{"status": "error", "reason": "store_unavailable"})
				w.Header().Set("Retry-After", "1")
				writeErr(http.StatusServiceUnavailable, "store_unavailable", "tenant store temporarily unavailable")
				return
			}
			emit(trace.Auth, "auth", map[string]any{"status": "denied"})
			denyAuth("invalid_api_key", "invalid API key")
			return
		}
		emit(trace.Auth, "auth", map[string]any{"tenant_id": tenant.ID, "tier": tenant.Tier})
		requestSpan.SetAttributes(otelattr.String("tenant_id", tenant.ID), otelattr.String("tier", tenant.Tier))

		if allowed, retryAfter := p.RateLimit.Allow(tenant.ID, tenant.RPMLimit); !allowed {
			emit(trace.RateLimit, "ratelimit", map[string]any{"tenant_id": tenant.ID, "retry_after": retryAfter})
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			writeErr(http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		}
		emit(trace.RateLimit, "ratelimit", map[string]any{"tenant_id": tenant.ID, "status": "allowed"})

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body chatCompletionRequest
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&body); err != nil {
			status := http.StatusBadRequest
			msg := "malformed JSON body"
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				status = http.StatusRequestEntityTooLarge
				msg = "request body too large"
			}
			writeErr(status, "invalid_request", msg)
			return
		}
		if dec.More() {
			writeErr(http.StatusBadRequest, "invalid_request", "unexpected data after JSON body")
			return
		}
		if len(body.Messages) == 0 {
			writeErr(http.StatusBadRequest, "invalid_request", "messages must not be empty")
			return
		}
		if body.Model == "" {
			writeErr(http.StatusBadRequest, "invalid_request", "model must not be empty")
			return
		}
		if len(body.Messages) > maxMessages {
			writeErr(http.StatusRequestEntityTooLarge, "invalid_request", "too many messages")
			return
		}
		maxTokens := 0
		if body.MaxTokens != nil {
			if *body.MaxTokens <= 0 || *body.MaxTokens > maxTokensLimit {
				writeErr(http.StatusBadRequest, "invalid_request", "max_tokens must be between 1 and 32768")
				return
			}
			maxTokens = *body.MaxTokens
		}

		messages := make([]core.Message, 0, len(body.Messages))
		promptBytes := 0
		for _, m := range body.Messages {
			// An unknown role is a 4xx from the provider anyway — after a
			// budget reservation and a paid round trip. Cheaper and more
			// honest to answer it here.
			if m.Role != "system" && m.Role != "user" && m.Role != "assistant" {
				writeErr(http.StatusBadRequest, "invalid_request", "message role must be system, user or assistant")
				return
			}
			messages = append(messages, core.Message{Role: m.Role, Content: m.Content})
			promptBytes += len(m.Content)
		}
		if promptBytes > maxPromptBytes {
			writeErr(http.StatusRequestEntityTooLarge, "invalid_request", "prompt too large")
			return
		}
		chatReq := core.ChatRequest{RequestID: requestID, TenantID: tenant.ID, Alias: body.Model, Tier: tenant.Tier, Messages: messages, MaxTokens: maxTokens, Stream: body.Stream}
		requestSpan.SetAttributes(otelattr.String("alias", body.Model))

		period := time.Now().UTC().Format("2006-01")
		estimated := budget.EstimateTokens(promptBytes, maxTokens)
		reserveCtx, reserveSpan := tracer.Start(ctx, "budget.reserve")
		reservation, err := p.Budget.Reserve(reserveCtx, tenant.ID, period, estimated, tenant.MonthlyTokenBudget)
		reserveSpan.End()
		if err != nil {
			emit(trace.BudgetReserve, "budget", map[string]any{"tenant_id": tenant.ID, "status": "denied"})
			if errors.Is(err, budget.ErrBudgetExceeded) {
				writeErr(http.StatusPaymentRequired, "budget_exceeded", "monthly token budget exceeded")
				return
			}
			// Same reasoning as the auth path: a store outage is a
			// retryable 503, not a permanent failure of this request.
			logger.Error("budget: store unavailable", "request_id", requestID, "error", err.Error())
			w.Header().Set("Retry-After", "1")
			writeErr(http.StatusServiceUnavailable, "store_unavailable", "budget store temporarily unavailable")
			return
		}
		emit(trace.BudgetReserve, "budget", map[string]any{"tenant_id": tenant.ID, "reservation_id": reservation.ID, "estimated_tokens": estimated})

		if err := p.Trace.RecordRequest(traceCtx, requestID, tenant.ID, body.Model); err != nil {
			logger.Error("trace: failed to record request", "request_id", requestID, "error", err.Error())
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
			realTokens := sanitizeRealTokens(respUsage, estimated, requestID, logger)
			settleCtx, cancelSettle := context.WithTimeout(traceCtx, 5*time.Second)
			settleCtx, settleSpan := tracer.Start(settleCtx, "budget.settle")
			if err := p.Budget.Settle(settleCtx, reservation, realTokens); err != nil {
				logger.Error("budget: failed to settle reservation", "request_id", requestID, "error", err.Error())
			}
			settleSpan.End()
			cancelSettle()
			emit(trace.BudgetSettle, "budget", map[string]any{"tenant_id": tenant.ID, "real_tokens": realTokens})
			latencyMs := int(time.Since(start).Milliseconds())
			if p.Usage != nil {
				attemptRecords := make([]usage.AttemptRecord, 0, len(attempts))
				for _, a := range attempts {
					attemptStatus := "ok"
					if a.Err != nil {
						attemptStatus = "error"
					}
					attemptRecords = append(attemptRecords, usage.AttemptRecord{Provider: a.Provider, Model: a.Model, Status: attemptStatus, LatencyMs: a.LatencyMs})
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
					logger.Error("usage: failed to publish usage event", "request_id", requestID, "error", err.Error())
					emit(trace.UsagePublish, "usage", map[string]any{"tenant_id": tenant.ID, "status": "failed"})
				} else {
					// An async publisher only accepted the event into its
					// queue; claiming "ok" here would assert a delivery
					// nobody has confirmed yet. The worker logs the real
					// outcome with the request id.
					publishStatus := "ok"
					if _, async := p.Usage.(interface{ QueueDepth() int64 }); async {
						publishStatus = "queued"
					}
					emit(trace.UsagePublish, "usage", map[string]any{"tenant_id": tenant.ID, "status": publishStatus})
				}
			}
			if p.Stats != nil && status == "ok" {
				// Same cardinality rule the metrics use: a premium caller
				// naming a model directly must not be able to mint a new
				// recorder key per request.
				statsRoute, statsModel := metricLabels(p.Routing, body.Model, target.Model)
				p.Stats.Record(statsRoute, statsModel, tenant.ID, latencyMs, realTokens)
			}
		}()

		_, routeSpan := tracer.Start(ctx, "route")
		targets, err := router.Resolve(p.Routing, body.Model, tenant.Tier)
		routeSpan.End()
		if err != nil {
			emit(trace.Route, "router", map[string]any{"status": "error"})
			status = "error"
			errorClass = "permanent"
			httpStatus = writeRouterError(w, requestID, err)
			return
		}
		emit(trace.Route, "router", map[string]any{"targets": len(targets)})

		if body.Stream {
			serveStreamPipeline(streamArgs{
				w: w, r: r, ctx: ctx, p: p, tracer: tracer, tenant: tenant, alias: body.Model,
				targets: targets, chatReq: chatReq, requestID: requestID,
				usage: &respUsage, outTarget: &target, outAttempts: &attempts, outTTFTMs: &ttftMs,
				outStatus: &status, outErrorClass: &errorClass, outHTTPStatus: &httpStatus,
				start: start, emit: emit, writeTimeout: streamWriteTimeout,
			})
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
				// No httpStatus is set: no bytes ever reached the client.
				status = "client_closed"
				errorClass = ""
				return
			}
			emit(trace.Error, "resilience", map[string]any{"reason": "all_backends_failed"})
			status = "error"
			errorClass = errorClassFromAttempts(attempts)
			httpStatus = http.StatusBadGateway
			requestSpan.SetAttributes(otelattr.String("error.class", errorClass))
			recordRequestMetrics(ctx, p.Metrics, requestMetricsArgs{
				routing: p.Routing, alias: body.Model, resolvedModel: lastAttemptModel(attempts),
				tenantID: tenant.ID, httpStatus: httpStatus, latency: time.Since(start),
			})
			writeAllBackendsFailed(w, requestID, attempts)
			return
		}
		emit(trace.BackendResult, "resilience", map[string]any{"provider": target.Provider, "model": target.Model, "status": "ok", "prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens})

		respUsage = resp.Usage
		httpStatus = http.StatusOK
		requestSpan.SetAttributes(
			otelattr.String("provider", target.Provider),
			otelattr.String("model", target.Model),
		)
		recordRequestMetrics(ctx, p.Metrics, requestMetricsArgs{
			routing: p.Routing, alias: body.Model, resolvedModel: target.Model,
			tenantID: tenant.ID, httpStatus: httpStatus, latency: time.Since(start),
			promptTokens: resp.Usage.PromptTokens, completionTokens: resp.Usage.CompletionTokens,
		})

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"id":      requestID,
			"object":  "chat.completion",
			"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": resp.Message.Content}, "finish_reason": resp.FinishReason}},
			"usage":   map[string]int{"prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens},
		}); err != nil {
			// Mid-body write failure: the status line already said 200, so
			// the only honest thing left is to log it rather than let it
			// pass as a clean success.
			logger.Error("response: failed to write completion body", "request_id", requestID, "error", err.Error())
		}
	})
}

// sanitizeRealTokens bounds the provider-reported usage before it reaches
// budget.Settle, Stats and the token metric. A negative total would *credit*
// the tenant's monthly budget permanently (Settle does ADD used :delta), and
// an absurdly large one would overcharge; neither is something a provider
// should be trusted to get right. The accepted window is
// [0, 4*estimated] — four times the pessimistic pre-call estimate is far
// above any honest reading — and anything outside it falls back to the
// estimate, which is what was already reserved.
func sanitizeRealTokens(u core.Usage, estimated int, requestID string, logger *slog.Logger) int {
	real := u.PromptTokens + u.CompletionTokens
	if real < 0 || real > estimated*4 {
		logger.Warn("usage: provider reported implausible token usage, falling back to the estimate",
			"request_id", requestID, "reported_tokens", real, "estimated_tokens", estimated)
		return estimated
	}
	return real
}

// lastAttemptModel returns the model name of the last resilience attempt, or
// "" if none were made — used so an all_backends_failed metric/label can
// still reflect which model was actually (attempted to be) reached.
func lastAttemptModel(attempts []resilience.Attempt) string {
	if len(attempts) == 0 {
		return ""
	}
	return attempts[len(attempts)-1].Model
}

// metricLabels returns the "route" and "model" metric label values per the
// controller's cardinality rule: both must be bounded by config, never by
// arbitrary client input. route is the alias the request used (bounded by
// routing.yaml's Aliases) when it used one; otherwise (a direct
// "provider/model" target) route is the literal "direct". model is the
// resolved backend model when it came from that alias's cascade (also
// bounded by routing.yaml), or the literal "direct" otherwise.
func metricLabels(routing *config.Routing, alias, resolvedModel string) (route, model string) {
	if routing != nil {
		if _, ok := routing.Aliases[alias]; ok {
			return alias, resolvedModel
		}
	}
	return "direct", "direct"
}

// requestMetricsArgs bundles recordRequestMetrics' inputs so the call sites
// (success, all_backends_failed, streaming) stay one-liners.
type requestMetricsArgs struct {
	routing                        *config.Routing
	alias, resolvedModel, tenantID string
	httpStatus                     int
	latency                        time.Duration
	promptTokens, completionTokens int
	ttft                           time.Duration
	recordTTFT                     bool
}

// recordRequestMetrics records gateway_requests_total, gateway_latency_ms,
// gateway_tokens_total (split prompt/completion) and, when recordTTFT is
// set, gateway_ttft_ms — a no-op when metrics is nil (OTel disabled). status
// is always the literal HTTP status code returned to the client, per the
// controller's cardinality rule (never a free-form ok/error string).
func recordRequestMetrics(ctx context.Context, metrics *trace.Metrics, a requestMetricsArgs) {
	if metrics == nil {
		return
	}
	route, model := metricLabels(a.routing, a.alias, a.resolvedModel)
	statusLabel := strconv.Itoa(a.httpStatus)
	baseAttrs := metric.WithAttributes(
		otelattr.String("route", route),
		otelattr.String("model", model),
		otelattr.String("tenant", a.tenantID),
		otelattr.String("status", statusLabel),
	)
	metrics.RequestsTotal.Add(ctx, 1, baseAttrs)
	metrics.LatencyMs.Record(ctx, float64(a.latency.Milliseconds()), baseAttrs)
	if a.promptTokens > 0 || a.completionTokens > 0 {
		metrics.TokensTotal.Add(ctx, int64(a.promptTokens), metric.WithAttributes(
			otelattr.String("route", route), otelattr.String("model", model), otelattr.String("tenant", a.tenantID), otelattr.String("kind", "prompt"),
		))
		metrics.TokensTotal.Add(ctx, int64(a.completionTokens), metric.WithAttributes(
			otelattr.String("route", route), otelattr.String("model", model), otelattr.String("tenant", a.tenantID), otelattr.String("kind", "completion"),
		))
	}
	if a.recordTTFT {
		metrics.TTFTMs.Record(ctx, float64(a.ttft.Milliseconds()), baseAttrs)
	}
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
// never request/response content. The span is backdated to a.StartedAt and
// ended at a.StartedAt+a.LatencyMs, so it shows the backend call's real
// duration instead of a zero-length span created after the fact.
func traceBackendCallSpan(ctx context.Context, tracer oteltrace.Tracer, index int, a resilience.Attempt) {
	attemptStatus := "ok"
	if a.Err != nil {
		attemptStatus = "error"
	}
	startedAt := a.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	_, span := tracer.Start(ctx, "backend.call", oteltrace.WithTimestamp(startedAt))
	span.SetAttributes(
		otelattr.String("provider", a.Provider),
		otelattr.String("model", a.Model),
		otelattr.Int("attempt", index+1),
		otelattr.String("status", attemptStatus),
	)
	span.End(oteltrace.WithTimestamp(startedAt.Add(time.Duration(a.LatencyMs) * time.Millisecond)))
}

// streamArgs bundles serveStreamPipeline's inputs (it outgrew a positional
// parameter list once OTel needed tenant/alias/http-status plumbed through
// too).
type streamArgs struct {
	w         http.ResponseWriter
	r         *http.Request
	ctx       context.Context // the span-carrying context from the root span, NOT r.Context()
	p         *Pipeline
	tracer    oteltrace.Tracer
	tenant    core.Tenant
	alias     string
	targets   []core.BackendTarget
	chatReq   core.ChatRequest
	requestID string
	start        time.Time
	emit         func(trace.EventType, string, map[string]any)
	writeTimeout time.Duration

	usage         *core.Usage
	outTarget     *core.BackendTarget
	outAttempts   *[]resilience.Attempt
	outTTFTMs     *int
	outStatus     *string
	outErrorClass *string
	outHTTPStatus *int
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
// real, not estimated, numbers. Every span/cancellation check below uses
// args.ctx (the root span's context, derived from r.Context()), never
// args.r.Context() directly, so backend.call/resilience spans nest under the
// root span instead of starting a disconnected trace.
func serveStreamPipeline(args streamArgs) {
	w, requestID := args.w, args.requestID
	flusher, ok := w.(http.Flusher)
	if !ok {
		*args.outHTTPStatus = http.StatusInternalServerError
		WriteError(w, requestID, http.StatusInternalServerError, "streaming_unsupported", "response writer does not support flushing")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	// The SSE headers are already on the wire at this point: every outcome
	// from here on (success or an in-band "backend_stream_failed" event) is
	// still wire status 200, so that is what the root span/metrics record.
	*args.outHTTPStatus = http.StatusOK

	resolve := func(target core.BackendTarget) (core.StreamBackend, error) {
		backend, ok := args.p.Backends[target.Provider]
		if !ok {
			return nil, fmt.Errorf("resilience: no backend registered for provider %q", target.Provider)
		}
		return backend, nil
	}

	// rc drives the per-write deadline. On the real net/http writer this
	// sets a socket deadline; on a writer that does not support one
	// (httptest.ResponseRecorder) it reports ErrNotSupported and the write
	// proceeds undeadlined.
	rc := http.NewResponseController(w)
	writeSSE := func(format string, a ...any) error {
		if err := rc.SetWriteDeadline(time.Now().Add(args.writeTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		if _, err := fmt.Fprintf(w, format, a...); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	streamStart := time.Now()
	var ttfb time.Duration
	ttfbSet := false
	onChunk := func(chunk core.ChatChunk) error {
		select {
		case <-args.ctx.Done():
			return args.ctx.Err()
		default:
		}
		if !ttfbSet && chunk.Delta != "" {
			ttfb = time.Since(streamStart)
			ttfbSet = true
		}
		payload, _ := json.Marshal(map[string]any{
			"id":      requestID,
			"object":  "chat.completion.chunk",
			"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": chunk.Delta}, "finish_reason": chunk.FinishReason}},
		})
		// A write that blocks past the deadline (client stopped reading)
		// surfaces here as an error and ends the stream, exactly like a
		// disconnect would — never an indefinitely pinned goroutine.
		return writeSSE("data: %s\n\n", payload)
	}

	result, attempts, streamErr := resilience.CallStream(args.ctx, args.targets, resolve, args.chatReq, onChunk)
	for i, a := range attempts {
		traceBackendCallSpan(args.ctx, args.tracer, i, a)
		args.emit(trace.BackendAttempt, "resilience", attemptPayload(a))
	}
	*args.usage = result
	*args.outAttempts = attempts
	*args.outTTFTMs = int(ttfb.Milliseconds())

	var succeeded resilience.Attempt
	if streamErr == nil {
		for _, a := range attempts {
			if a.Err == nil {
				succeeded = a
			}
		}
		*args.outTarget = core.BackendTarget{Provider: succeeded.Provider, Model: succeeded.Model}
		args.emit(trace.BackendResult, "resilience", map[string]any{
			"provider": succeeded.Provider, "model": succeeded.Model, "status": "ok",
			"latency_ms": time.Since(streamStart).Milliseconds(), "ttft_ms": ttfb.Milliseconds(),
			"prompt_tokens": result.PromptTokens, "completion_tokens": result.CompletionTokens,
		})
	}

	if streamErr != nil && isClientCancelled(args.ctx, streamErr) {
		args.emit(trace.Error, "resilience", map[string]any{"reason": "client_closed"})
		// Same rule as the non-streaming cancel path: still publish a
		// usage_event (status "client_closed", no error_class), but never
		// feed Stats with a disconnect, and skip metrics same as non-streaming.
		*args.outStatus = "client_closed"
		*args.outErrorClass = ""
		return
	}

	// Record metrics for every outcome that actually reached (or tried to
	// reach) a backend — success or all_backends_failed/stream-failed —
	// mirroring the non-streaming path. TTFTMs is recorded here specifically
	// because streaming is the only path with a real time-to-first-byte;
	// the non-streaming path has no meaningful TTFT distinct from full
	// latency, so it is skipped there.
	resolvedModel := succeeded.Model
	if resolvedModel == "" {
		resolvedModel = lastAttemptModel(attempts)
	}
	recordRequestMetrics(args.ctx, args.p.Metrics, requestMetricsArgs{
		routing: args.p.Routing, alias: args.alias, resolvedModel: resolvedModel,
		tenantID: args.tenant.ID, httpStatus: *args.outHTTPStatus, latency: time.Since(streamStart),
		promptTokens: result.PromptTokens, completionTokens: result.CompletionTokens,
		ttft: ttfb, recordTTFT: ttfbSet,
	})

	if streamErr != nil {
		// reason is the trace_event's diagnostic label (which cascade stage
		// failed); errorClass is the usage_event's classification of *why*
		// (derived from the last attempt's actual error, never a guess tied
		// to the reason string).
		reason := "all_backends_failed"
		if errors.Is(streamErr, resilience.ErrStreamFailedAfterFirstByte) {
			reason = "stream_failed_after_first_byte"
		}
		*args.outStatus = "error"
		*args.outErrorClass = errorClassFromAttempts(attempts)
		oteltrace.SpanFromContext(args.ctx).SetAttributes(otelattr.String("error.class", *args.outErrorClass))
		args.emit(trace.Error, "resilience", map[string]any{"reason": reason})
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "backend_stream_failed", "message": scrubErrorText(streamErr), "request_id": requestID}})
		_ = writeSSE("data: %s\n\n", payload)
	}

	_ = writeSSE("data: [DONE]\n\n")
}
