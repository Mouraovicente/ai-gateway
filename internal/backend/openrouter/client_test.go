package openrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

func TestChat_Success_IncludesUsage(t *testing.T) {
	fixture, err := os.ReadFile("testdata/chat_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	var capturedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("expected Authorization header with test-key, got %q", r.Header.Get("Authorization"))
		}
		json.NewDecoder(r.Body).Decode(&capturedBody)
		w.Header().Set("Content-Type", "application/json")
		w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-1", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	resp, err := client.Chat(context.Background(), "openai/gpt-4o-mini", req)
	if err != nil {
		t.Fatalf("Chat returned error: %v", err)
	}
	if resp.Message.Content == "" {
		t.Fatalf("expected non-empty content: %q", resp.Message.Content)
	}
	if resp.Usage.PromptTokens == 0 && resp.Usage.CompletionTokens == 0 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
}

func TestChat_429_IsTransientError(t *testing.T) {
	fixture, err := os.ReadFile("testdata/chat_error.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-2", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	_, err = client.Chat(context.Background(), "openai/gpt-4o-mini", req)
	be, ok := err.(*core.BackendError)
	if !ok {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Transient {
		t.Fatalf("expected Transient for 429, got %v", be.Class)
	}
}

func TestChatStream_TruncatedStreamIsTransientError(t *testing.T) {
	// Server closes the connection mid-stream, without ever sending [DONE].
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"Oi\"}}]}\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(sse))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "key")
	req := core.ChatRequest{Messages: []core.Message{{Role: "user", Content: "oi"}}}
	chunks, errs := client.ChatStream(context.Background(), "some/model", req)
	for range chunks {
	}
	err := <-errs
	if err == nil {
		t.Fatal("expected an error for a stream missing [DONE]")
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
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"oi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "key")
	req := core.ChatRequest{RequestID: "req-xyz", MaxTokens: 256, Messages: []core.Message{{Role: "user", Content: "oi"}}}
	if _, err := client.Chat(context.Background(), "some/model", req); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotHeader != "req-xyz" {
		t.Fatalf("expected X-Request-Id forwarded, got %q", gotHeader)
	}
	if gotBody["max_tokens"] != float64(256) {
		t.Fatalf("expected max_tokens 256, got %v", gotBody["max_tokens"])
	}
}
