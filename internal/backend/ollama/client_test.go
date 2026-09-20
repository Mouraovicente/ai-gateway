package ollama

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

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
		_, _ = w.Write(body)
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
		_, _ = w.Write([]byte(`{"model":"does-not-exist","done":true,"error":"model not found"}`))
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
		_, _ = w.Write([]byte(`{"model":"qwen2.5-coder:1.5b","message":{"role":"assistant","content":"partial"},"done":false}`))
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
		_, _ = w.Write(body)
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

func TestChatStream_ConsumerCancelsAfterFirstDelta_NoGoroutineLeak(t *testing.T) {
	body := []byte(`{"model":"qwen2.5-coder:1.5b","message":{"role":"assistant","content":"Ol"},"done":false}
{"model":"qwen2.5-coder:1.5b","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":12,"eval_count":8}
`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-6", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	ctx, cancel := context.WithCancel(context.Background())
	chunks, errs := client.ChatStream(ctx, "qwen2.5-coder:1.5b", req)

	first := <-chunks
	if first.Delta == "" {
		t.Fatalf("expected first chunk to carry a delta, got %+v", first)
	}
	cancel()

	done := make(chan struct{})
	go func() {
		for range chunks {
			// drain, but consumer never reads again per the scenario except this drain
			// loop exists only to detect channel close; real consumers may stop reading
			// entirely, which is exactly the leak scenario the terminal-chunk fix covers.
		}
		<-errs
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for ChatStream goroutine to finish after ctx cancellation")
	}
}

func TestChatStream_TruncatedStreamIsTransientError(t *testing.T) {
	// Stream ends (server closes the body) without ever sending done:true.
	body := []byte(`{"model":"qwen2.5-coder:1.5b","message":{"role":"assistant","content":"Ol"},"done":false}
`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{Messages: []core.Message{{Role: "user", Content: "oi"}}}
	chunks, errs := client.ChatStream(context.Background(), "qwen2.5-coder:1.5b", req)
	for range chunks {
	}
	err := <-errs
	if err == nil {
		t.Fatal("expected an error for a stream missing its terminal done:true")
	}
	var be *core.BackendError
	if !errorsAs(err, &be) {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Transient {
		t.Fatalf("expected Transient class for truncated stream, got %v", be.Class)
	}
}

func TestChat_ForwardsRequestIDHeader(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Request-Id")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":{"role":"assistant","content":"oi"},"done":true,"done_reason":"stop","prompt_eval_count":1,"eval_count":1}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-xyz", Messages: []core.Message{{Role: "user", Content: "oi"}}}
	if _, err := client.Chat(context.Background(), "qwen2.5-coder:1.5b", req); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotHeader != "req-xyz" {
		t.Fatalf("expected X-Request-Id %q forwarded, got %q", "req-xyz", gotHeader)
	}
}

func TestChat_ForwardsMaxTokensAsNumPredict(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":{"role":"assistant","content":"oi"},"done":true,"done_reason":"stop","prompt_eval_count":1,"eval_count":1}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{MaxTokens: 128, Messages: []core.Message{{Role: "user", Content: "oi"}}}
	if _, err := client.Chat(context.Background(), "qwen2.5-coder:1.5b", req); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	opts, ok := gotBody["options"].(map[string]any)
	if !ok {
		t.Fatalf("expected options object in request body, got %+v", gotBody)
	}
	if opts["num_predict"] != float64(128) {
		t.Fatalf("expected num_predict 128, got %v", opts["num_predict"])
	}
}
