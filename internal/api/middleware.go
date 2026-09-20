package api

import (
	"net"
	"net/http"
	"os"
	"strings"
)

// securityHeaders are set on every response: nosniff because LLM-generated
// text travels inside these JSON bodies, no-store because a completion is
// tenant data that must not sit in a shared cache, and HSTS so the ALB's
// 80->443 redirect cannot be downgraded on a repeat visit.
func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Strict-Transport-Security", "max-age=31536000")
		next.ServeHTTP(w, r)
	})
}

// trustProxy reports whether X-Forwarded-For may be believed. Off by
// default: with no proxy in front, the header is attacker-controlled and
// trusting it would turn the per-IP limiter into a no-op.
func trustProxy() bool {
	return strings.EqualFold(os.Getenv("TRUST_PROXY"), "true")
}

// ClientIP returns the source address used to key the pre-auth limiter:
// the first hop of X-Forwarded-For when TRUST_PROXY=true, otherwise the
// peer address of the connection.
func ClientIP(r *http.Request) string {
	if trustProxy() {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			first := strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])
			if first != "" {
				return first
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
