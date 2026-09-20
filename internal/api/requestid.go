package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

type contextKey string

const (
	requestIDKey       contextKey = "request_id"
	clientRequestIDKey contextKey = "client_request_id"
)

// RequestIDMiddleware generates a uuid v4 for every request, sets it as the
// X-Request-Id response header, and makes it retrievable via context.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.NewString()
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		// A client-supplied id is never the request id (that would let a
		// caller spoof correlation and pollute logs), but echoing a
		// sanitized copy back keeps client-side correlation possible.
		if client := sanitizeClientRequestID(r.Header.Get("X-Request-Id")); client != "" {
			w.Header().Set("X-Client-Request-Id", client)
			ctx = context.WithValue(ctx, clientRequestIDKey, client)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// maxClientRequestIDLen bounds the echoed value.
const maxClientRequestIDLen = 128

// sanitizeClientRequestID keeps only printable ASCII and caps the length,
// so a header cannot smuggle control characters into a response header or
// a trace payload.
func sanitizeClientRequestID(v string) string {
	if v == "" {
		return ""
	}
	if len(v) > maxClientRequestIDLen {
		v = v[:maxClientRequestIDLen]
	}
	var b strings.Builder
	for _, c := range v {
		if c >= 0x20 && c < 0x7f {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// ClientRequestIDFromContext returns the sanitized client-supplied id, or
// "" when the client sent none.
func ClientRequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(clientRequestIDKey).(string)
	return id
}

// RequestIDFromContext retrieves the request id set by RequestIDMiddleware.
// Returns "" if none was set (e.g. in a test that skips the middleware).
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}
