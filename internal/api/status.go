package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/router"
)

// maxErrorTextBytes bounds any upstream error text placed in a response body
// or SSE event. errorTextCap also cuts at the first newline, since an
// upstream body that echoes request/prompt content back in an error message
// would otherwise leak it verbatim into a client-visible field.
const maxErrorTextBytes = 200

// scrubErrorText returns err's message, truncated to the first line and to
// maxErrorTextBytes, so an upstream error body can never smuggle arbitrary
// multi-line content (or excessive length) into a client-facing error field.
func scrubErrorText(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if idx := strings.IndexAny(s, "\r\n"); idx >= 0 {
		s = s[:idx]
	}
	if len(s) > maxErrorTextBytes {
		s = s[:maxErrorTextBytes]
	}
	return s
}

// toBackends narrows a map[string]resilience.FullBackend to the
// map[string]resilience.Backend shape resilience.Call expects. Every
// FullBackend value already satisfies Backend, so this is a plain copy.
func toBackends(full map[string]resilience.FullBackend) map[string]resilience.Backend {
	out := make(map[string]resilience.Backend, len(full))
	for k, v := range full {
		out[k] = v
	}
	return out
}

// attemptPayload turns a resilience.Attempt into a trace_event payload:
// metadata only (provider, model, status), never request/response content.
func attemptPayload(a resilience.Attempt) map[string]any {
	status := "ok"
	if a.Err != nil {
		status = "error"
	}
	return map[string]any{"provider": a.Provider, "model": a.Model, "status": status}
}

// errorClassFromAttempts derives usage_event.error_class from the last
// failed attempt's underlying error, instead of a hardcoded literal: a
// *core.BackendError carries the real Transient/Permanent classification
// (via errors.As, so a wrapped error still unwraps correctly); anything
// else (a plain error, or no failed attempt at all) is reported as
// "unknown" rather than guessed at.
func errorClassFromAttempts(attempts []resilience.Attempt) string {
	for i := len(attempts) - 1; i >= 0; i-- {
		if attempts[i].Err == nil {
			continue
		}
		var be *core.BackendError
		if errors.As(attempts[i].Err, &be) {
			if be.Class == core.Transient {
				return "transient"
			}
			return "permanent"
		}
		return "unknown"
	}
	return "unknown"
}

// writeRouterError maps a router.Resolve error to its HTTP status/type per
// the controller's binding decisions for Task 11, and returns the status
// written so the caller can record it (e.g. on the OTel root span).
func writeRouterError(w http.ResponseWriter, requestID string, err error) int {
	var unknownProvider *router.ErrUnknownProvider
	var status int
	var errType string
	switch {
	case errors.Is(err, router.ErrDirectTargetForbidden):
		status, errType = http.StatusForbidden, "tier_forbidden"
	case errors.As(err, &unknownProvider):
		status, errType = http.StatusBadRequest, "unknown_provider"
	default:
		// router.ErrUnknownModel and anything else Resolve can return.
		status, errType = http.StatusBadRequest, "unknown_model"
	}
	// scrubErrorText also bounds length: the router's message embeds the
	// model string the caller sent, which can be most of a 1 MiB body.
	WriteError(w, requestID, status, errType, scrubErrorText(err))
	return status
}

// writeAllBackendsFailed writes the 502 all_backends_failed body, including
// the attempts array the controller's decisions require.
func writeAllBackendsFailed(w http.ResponseWriter, requestID string, attempts []resilience.Attempt) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	attemptBodies := make([]map[string]any, 0, len(attempts))
	for _, a := range attempts {
		attemptBodies = append(attemptBodies, map[string]any{"provider": a.Provider, "model": a.Model, "status": a.Status, "error": scrubErrorText(a.Err)})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":    map[string]any{"type": "all_backends_failed", "message": "every backend in the cascade failed", "request_id": requestID},
		"attempts": attemptBodies,
	})
}
