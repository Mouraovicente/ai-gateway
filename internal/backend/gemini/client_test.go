// synthetic: recorded shape from Gemini API docs; replace with a real capture when GEMINI_API_KEY is available
package gemini

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

func TestChat_Success_ParsesCandidateAndUsageMetadata(t *testing.T) {
	fixture, err := os.ReadFile("testdata/generate_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "generateContent") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("key") != "" {
			t.Fatalf("api key must not be sent in the URL query, got %q", r.URL.RawQuery)
		}
		if r.Header.Get("x-goog-api-key") != "test-key" {
			t.Fatalf("expected x-goog-api-key header, got %q", r.Header.Get("x-goog-api-key"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-1", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	resp, err := client.Chat(context.Background(), "gemini-1.5-pro", req)
	if err != nil {
		t.Fatalf("Chat returned error: %v", err)
	}
	if resp.Message.Content != "Olá! Como posso ajudar?" {
		t.Fatalf("unexpected content: %q", resp.Message.Content)
	}
	if resp.Usage.PromptTokens != 10 || resp.Usage.CompletionTokens != 7 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
	if resp.FinishReason != "STOP" {
		t.Fatalf("unexpected finish reason: %q", resp.FinishReason)
	}
}

func TestChatStream_EmitsDeltasThenFinalUsage(t *testing.T) {
	fixture, err := os.ReadFile("testdata/generate_stream.ndjson")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "streamGenerateContent") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("key") != "" {
			t.Fatalf("api key must not be sent in the URL query, got %q", r.URL.RawQuery)
		}
		if r.Header.Get("x-goog-api-key") != "test-key" {
			t.Fatalf("expected x-goog-api-key header, got %q", r.Header.Get("x-goog-api-key"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-2", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	chunks, errs := client.ChatStream(context.Background(), "gemini-1.5-pro", req)

	var deltas []string
	var sawUsage bool
	for chunk := range chunks {
		// The Gemini client's final chunk carries both the last text delta
		// and the usage metadata together (see client.go doc comment), so
		// both checks apply independently to the same chunk rather than
		// being mutually exclusive branches.
		if chunk.Delta != "" {
			deltas = append(deltas, chunk.Delta)
		}
		if chunk.Usage != nil {
			sawUsage = true
			if chunk.Usage.PromptTokens != 10 || chunk.Usage.CompletionTokens != 7 {
				t.Fatalf("unexpected final usage: %+v", chunk.Usage)
			}
		}
	}
	if err := <-errs; err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}
	if len(deltas) != 2 || deltas[0] != "Ol" || deltas[1] != "á!" {
		t.Fatalf("unexpected deltas: %+v", deltas)
	}
	if !sawUsage {
		t.Fatalf("expected a final chunk carrying usageMetadata")
	}
}

func TestChatStream_ConsumerCancelsAfterFirstDelta_NoGoroutineLeak(t *testing.T) {
	fixture, err := os.ReadFile("testdata/generate_stream.ndjson")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-3", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	ctx, cancel := context.WithCancel(context.Background())
	chunks, errs := client.ChatStream(ctx, "gemini-1.5-pro", req)

	first := <-chunks
	if first.Delta == "" {
		t.Fatalf("expected first chunk to carry a delta, got %+v", first)
	}
	cancel()

	done := make(chan struct{})
	go func() {
		for range chunks {
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

func TestChat_PromptBlocked_ReturnsPermanentError(t *testing.T) {
	fixture := []byte(`{"promptFeedback":{"blockReason":"SAFETY"}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-4", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	_, err := client.Chat(context.Background(), "gemini-1.5-pro", req)
	if err == nil {
		t.Fatalf("expected an error for a blocked prompt")
	}
	var be *core.BackendError
	if !errors.As(err, &be) {
		t.Fatalf("expected a *core.BackendError, got %T: %v", err, err)
	}
	if be.Class != core.Permanent {
		t.Fatalf("expected Permanent class, got %s", be.Class)
	}
	if be.Status != http.StatusOK {
		t.Fatalf("expected status 200, got %d", be.Status)
	}
	if !strings.Contains(be.Error(), "SAFETY") {
		t.Fatalf("expected error message to mention SAFETY, got %q", be.Error())
	}
}

func TestChatStream_PromptBlocked_ErrorsWithNoChunks(t *testing.T) {
	fixture := []byte("data: {\"promptFeedback\":{\"blockReason\":\"SAFETY\"}}\n\n")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-5", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	chunks, errs := client.ChatStream(context.Background(), "gemini-1.5-pro", req)

	var gotChunks int
	for range chunks {
		gotChunks++
	}
	if gotChunks != 0 {
		t.Fatalf("expected no chunks for a blocked prompt, got %d", gotChunks)
	}

	err := <-errs
	if err == nil {
		t.Fatalf("expected an error for a blocked prompt")
	}
	var be *core.BackendError
	if !errors.As(err, &be) {
		t.Fatalf("expected a *core.BackendError, got %T: %v", err, err)
	}
	if be.Class != core.Permanent {
		t.Fatalf("expected Permanent class, got %s", be.Class)
	}
	if !strings.Contains(be.Error(), "SAFETY") {
		t.Fatalf("expected error message to mention SAFETY, got %q", be.Error())
	}
}

func TestToRequestBody_SystemMessageBecomesSystemInstruction(t *testing.T) {
	messages := []core.Message{
		{Role: "system", Content: "be concise"},
		{Role: "user", Content: "oi"},
		{Role: "assistant", Content: "olá"},
	}
	body := toRequestBody(core.ChatRequest{Messages: messages})

	if body.SystemInstruction == nil {
		t.Fatalf("expected systemInstruction to be set")
	}
	if len(body.SystemInstruction.Parts) != 1 || body.SystemInstruction.Parts[0].Text != "be concise" {
		t.Fatalf("unexpected systemInstruction: %+v", body.SystemInstruction)
	}
	for _, c := range body.Contents {
		if c.Role == "system" {
			t.Fatalf("contents must not carry role \"system\": %+v", body.Contents)
		}
	}
	if len(body.Contents) != 2 {
		t.Fatalf("expected 2 contents (user, model), got %d: %+v", len(body.Contents), body.Contents)
	}
	if body.Contents[0].Role != "user" || body.Contents[1].Role != "model" {
		t.Fatalf("unexpected content roles: %+v", body.Contents)
	}
}

func TestClassify_MapsStatusToErrorClass(t *testing.T) {
	cases := []struct {
		status int
		want   core.ErrorClass
	}{
		{http.StatusBadRequest, core.Permanent},
		{http.StatusUnauthorized, core.Permanent},
		{http.StatusNotFound, core.Permanent},
		{http.StatusTooManyRequests, core.Transient},
		{http.StatusInternalServerError, core.Transient},
		{http.StatusServiceUnavailable, core.Transient},
	}
	for _, tc := range cases {
		if got := classify(tc.status); got != tc.want {
			t.Errorf("classify(%d) = %s, want %s", tc.status, got, tc.want)
		}
	}
}

func TestChatStream_TruncatedStreamIsTransientError(t *testing.T) {
	// A candidate delta with neither finishReason nor usageMetadata, then the
	// server closes the connection: never a legitimate terminal event.
	sse := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Oi\"}]}}]}\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sse))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "key")
	req := core.ChatRequest{Messages: []core.Message{{Role: "user", Content: "oi"}}}
	chunks, errs := client.ChatStream(context.Background(), "gemini-pro", req)
	for range chunks {
	}
	err := <-errs
	if err == nil {
		t.Fatal("expected an error for a stream missing its terminal marker")
	}
	be, ok := err.(*core.BackendError)
	if !ok {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Transient {
		t.Fatalf("expected Transient class for truncated stream, got %v", be.Class)
	}
}

func TestChat_ForwardsRequestIDAndMaxTokens(t *testing.T) {
	var gotHeader string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Request-Id")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"oi"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "key")
	req := core.ChatRequest{RequestID: "req-xyz", MaxTokens: 64, Messages: []core.Message{{Role: "user", Content: "oi"}}}
	if _, err := client.Chat(context.Background(), "gemini-pro", req); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotHeader != "req-xyz" {
		t.Fatalf("expected X-Request-Id forwarded, got %q", gotHeader)
	}
	cfg, ok := gotBody["generationConfig"].(map[string]any)
	if !ok {
		t.Fatalf("expected generationConfig in request body, got %+v", gotBody)
	}
	if cfg["maxOutputTokens"] != float64(64) {
		t.Fatalf("expected maxOutputTokens 64, got %v", cfg["maxOutputTokens"])
	}
}
