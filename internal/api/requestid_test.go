package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDMiddleware_SetsHeaderAndContext(t *testing.T) {
	var gotFromContext string
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFromContext = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler.ServeHTTP(rec, req)

	headerID := rec.Header().Get("X-Request-Id")
	if headerID == "" {
		t.Fatalf("expected X-Request-Id header to be set")
	}
	if headerID != gotFromContext {
		t.Fatalf("header id %q does not match context id %q", headerID, gotFromContext)
	}
	if len(headerID) != 36 {
		t.Fatalf("expected a uuid-shaped id, got %q", headerID)
	}
}
