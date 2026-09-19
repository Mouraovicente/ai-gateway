package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/backend/ollama"
)

func TestChatHandler_NonStreaming_ReturnsOpenAIShapedResponse(t *testing.T) {
	fixture, err := os.ReadFile("../backend/ollama/testdata/chat_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture)
	}))
	defer ollamaSrv.Close()

	client := ollama.NewClient(ollamaSrv.URL)
	handler := RequestIDMiddleware(NewChatHandler(client, "qwen2.5-coder:1.5b"))

	reqBody := `{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatalf("expected X-Request-Id header on response")
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("parsing response body: %v", err)
	}
	if len(parsed.Choices) != 1 || parsed.Choices[0].Message.Content == "" {
		t.Fatalf("unexpected choices: %+v", parsed.Choices)
	}
}

func TestChatHandler_Streaming_EmitsSSEWithDoneSentinel(t *testing.T) {
	fixture, err := os.ReadFile("../backend/ollama/testdata/chat_stream.ndjson")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture)
	}))
	defer ollamaSrv.Close()

	client := ollama.NewClient(ollamaSrv.URL)
	handler := RequestIDMiddleware(NewChatHandler(client, "qwen2.5-coder:1.5b"))

	reqBody := `{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}

	body := rec.Body.String()
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("expected body to end with the DONE sentinel event, got %q", body)
	}

	// SSE events are separated by a blank line ("\n\n"); split on that
	// boundary rather than by line, and drop the trailing empty element left
	// by the final event's closing blank line.
	events := strings.Split(strings.TrimSuffix(body, "\n\n"), "\n\n")
	last := events[len(events)-1]
	if last != "data: [DONE]" {
		t.Fatalf("expected last SSE event to be the DONE sentinel, got %q", last)
	}
}
