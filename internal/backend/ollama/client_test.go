package ollama

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

func serveFixture(t *testing.T, status int, fixturePath string) *httptest.Server {
	t.Helper()
	body, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", fixturePath, err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/chat" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(body)
	}))
}

func TestChat_Success(t *testing.T) {
	srv := serveFixture(t, http.StatusOK, "testdata/chat_success.json")
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{
		RequestID: "req-1",
		Messages:  []core.Message{{Role: "user", Content: "oi"}},
	}

	resp, err := client.Chat(context.Background(), "qwen2.5-coder:1.5b", req)
	if err != nil {
		t.Fatalf("Chat returned error: %v", err)
	}
	if resp.Message.Content == "" {
		t.Fatalf("expected non-empty message content")
	}
	if resp.Usage.PromptTokens <= 0 || resp.Usage.CompletionTokens <= 0 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
	if resp.FinishReason == "" {
		t.Fatalf("expected non-empty finish reason")
	}
}

func TestChat_ModelNotFound_IsPermanentError(t *testing.T) {
	srv := serveFixture(t, http.StatusNotFound, "testdata/chat_error.json")
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-2", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	_, err := client.Chat(context.Background(), "does-not-exist", req)
	if err == nil {
		t.Fatalf("expected error for 404 response")
	}
	var be *core.BackendError
	if !errorsAs(err, &be) {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Permanent {
		t.Fatalf("expected Permanent class for 404, got %v", be.Class)
	}
	if be.Status != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", be.Status)
	}
}

func errorsAs(err error, target **core.BackendError) bool {
	be, ok := err.(*core.BackendError)
	if ok {
		*target = be
	}
	return ok
}

func TestChat_ErrorBodyOn200_IsPermanentError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"model":"does-not-exist","done":true,"error":"model not found"}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-4", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	_, err := client.Chat(context.Background(), "does-not-exist", req)
	if err == nil {
		t.Fatalf("expected error for body with error field")
	}
	var be *core.BackendError
	if !errorsAs(err, &be) {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Permanent {
		t.Fatalf("expected Permanent class for error body, got %v", be.Class)
	}
}

func TestChat_NotDone_IsTransientError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"model":"qwen2.5-coder:1.5b","message":{"role":"assistant","content":"partial"},"done":false}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-5", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	_, err := client.Chat(context.Background(), "qwen2.5-coder:1.5b", req)
	if err == nil {
		t.Fatalf("expected error for done:false response")
	}
	var be *core.BackendError
	if !errorsAs(err, &be) {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Transient {
		t.Fatalf("expected Transient class for done:false, got %v", be.Class)
	}
}

func TestChatStream_EmitsDeltasThenFinalUsage(t *testing.T) {
	body, err := os.ReadFile("testdata/chat_stream.ndjson")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-3", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	chunks, errs := client.ChatStream(context.Background(), "qwen2.5-coder:1.5b", req)

	var deltas []string
	var final *core.ChatChunk
	for chunk := range chunks {
		chunk := chunk
		if chunk.Usage != nil {
			final = &chunk
		} else {
			deltas = append(deltas, chunk.Delta)
		}
	}
	if err := <-errs; err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}

	if len(deltas) == 0 || joinDeltas(deltas) == "" {
		t.Fatalf("unexpected deltas: %+v", deltas)
	}
	if final == nil {
		t.Fatalf("expected a final chunk with usage")
	}
	if final.Usage.PromptTokens <= 0 || final.Usage.CompletionTokens <= 0 {
		t.Fatalf("unexpected final usage: %+v", final.Usage)
	}
	if final.FinishReason != "stop" {
		t.Fatalf("unexpected finish reason: %q", final.FinishReason)
	}
}

func joinDeltas(deltas []string) string {
	out := ""
	for _, d := range deltas {
		out += d
	}
	return out
}
