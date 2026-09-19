package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Mouraovicente/ai-gateway/internal/backend/ollama"
	"github.com/Mouraovicente/ai-gateway/internal/core"
)

type incomingMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionRequest struct {
	Model    string            `json:"model"`
	Messages []incomingMessage `json:"messages"`
	Stream   bool              `json:"stream"`
}

// NewChatHandler is the Task 5 bootstrap handler: it always calls the given
// fixed Ollama model, with no auth/ratelimit/budget/routing. The full
// pipeline (auth -> ratelimit -> budget -> router -> resilience) replaces
// the target selection in Task 9, reusing this same HTTP translation layer.
func NewChatHandler(client *ollama.Client, model string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := RequestIDFromContext(r.Context())

		var body chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, requestID, http.StatusBadRequest, "invalid_request", "malformed JSON body")
			return
		}
		if len(body.Messages) == 0 {
			WriteError(w, requestID, http.StatusBadRequest, "invalid_request", "messages must not be empty")
			return
		}
		if body.Model == "" {
			WriteError(w, requestID, http.StatusBadRequest, "invalid_request", "model must not be empty")
			return
		}

		messages := make([]core.Message, 0, len(body.Messages))
		for _, m := range body.Messages {
			messages = append(messages, core.Message{Role: m.Role, Content: m.Content})
		}
		chatReq := core.ChatRequest{RequestID: requestID, Messages: messages, Stream: body.Stream}

		if body.Stream {
			serveStream(w, r, client, model, chatReq, requestID)
			return
		}
		serveNonStream(r.Context(), w, client, model, chatReq, requestID)
	})
}

func serveNonStream(ctx context.Context, w http.ResponseWriter, client *ollama.Client, model string, chatReq core.ChatRequest, requestID string) {
	resp, err := client.Chat(ctx, model, chatReq)
	if err != nil {
		WriteError(w, requestID, http.StatusBadGateway, "backend_error", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":      requestID,
		"object":  "chat.completion",
		"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": resp.Message.Content}, "finish_reason": resp.FinishReason}},
		"usage":   map[string]int{"prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens},
	})
}

func serveStream(w http.ResponseWriter, r *http.Request, client *ollama.Client, model string, chatReq core.ChatRequest, requestID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteError(w, requestID, http.StatusInternalServerError, "streaming_unsupported", "response writer does not support flushing")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	chunks, errs := client.ChatStream(r.Context(), model, chatReq)
	for chunk := range chunks {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		payload, _ := json.Marshal(map[string]any{
			"id":      requestID,
			"object":  "chat.completion.chunk",
			"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": chunk.Delta}, "finish_reason": chunk.FinishReason}},
		})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}
	if err := <-errs; err != nil {
		payload, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}
