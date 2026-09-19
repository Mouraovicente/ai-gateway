// synthetic: recorded shape from Gemini API docs; replace with a real capture when GEMINI_API_KEY is available
package gemini

import (
	"context"
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
		if r.URL.Query().Get("key") != "test-key" {
			t.Fatalf("expected key=test-key query param, got %q", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(fixture)
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
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write(fixture)
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
		w.Write(fixture)
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
