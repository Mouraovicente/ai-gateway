package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/router"
)

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

// writeRouterError maps a router.Resolve error to its HTTP status/type per
// the controller's binding decisions for Task 11.
func writeRouterError(w http.ResponseWriter, requestID string, err error) {
	var unknownProvider *router.ErrUnknownProvider
	switch {
	case errors.Is(err, router.ErrDirectTargetForbidden):
		WriteError(w, requestID, http.StatusForbidden, "tier_forbidden", err.Error())
	case errors.As(err, &unknownProvider):
		WriteError(w, requestID, http.StatusBadRequest, "unknown_provider", err.Error())
	default:
		// router.ErrUnknownModel and anything else Resolve can return.
		WriteError(w, requestID, http.StatusBadRequest, "unknown_model", err.Error())
	}
}

// writeAllBackendsFailed writes the 502 all_backends_failed body, including
// the attempts array the controller's decisions require.
func writeAllBackendsFailed(w http.ResponseWriter, requestID string, attempts []resilience.Attempt) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	attemptBodies := make([]map[string]any, 0, len(attempts))
	for _, a := range attempts {
		errMsg := ""
		if a.Err != nil {
			errMsg = a.Err.Error()
		}
		attemptBodies = append(attemptBodies, map[string]any{"provider": a.Provider, "model": a.Model, "status": a.Status, "error": errMsg})
	}
	json.NewEncoder(w).Encode(map[string]any{
		"error":    map[string]any{"type": "all_backends_failed", "message": "every backend in the cascade failed", "request_id": requestID},
		"attempts": attemptBodies,
	})
}
