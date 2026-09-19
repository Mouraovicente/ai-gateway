package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// Client talks to Ollama's native /api/chat endpoint (not the OpenAI-compatible one).
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{}}
}

type chatRequestBody struct {
	Model    string         `json:"model"`
	Messages []core.Message `json:"messages"`
	Stream   bool           `json:"stream"`
}

type chatResponseBody struct {
	Message         core.Message `json:"message"`
	Done            bool         `json:"done"`
	DoneReason      string       `json:"done_reason"`
	PromptEvalCount int          `json:"prompt_eval_count"`
	EvalCount       int          `json:"eval_count"`
	Error           string       `json:"error"`
}

// errMessage extracts Ollama's {"error": "..."} message from an error
// response body, falling back to the raw body when it doesn't parse as that shape.
func errMessage(raw []byte) string {
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &body); err == nil && body.Error != "" {
		return body.Error
	}
	return string(raw)
}

// Chat performs a single non-streaming call to Ollama's /api/chat.
func (c *Client) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	body, err := json.Marshal(chatRequestBody{Model: model, Messages: req.Messages, Stream: false})
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: marshaling request: %w", err)}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: building request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: request failed: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: reading response: %w", err)}
	}

	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Status: resp.StatusCode, Err: fmt.Errorf("ollama: %s", errMessage(raw))}
	}
	if resp.StatusCode >= 400 {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Status: resp.StatusCode, Err: fmt.Errorf("ollama: %s", errMessage(raw))}
	}

	var parsed chatResponseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: parsing response: %w", err)}
	}
	if parsed.Error != "" {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Status: resp.StatusCode, Err: fmt.Errorf("ollama: %s", parsed.Error)}
	}
	if !parsed.Done {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Status: resp.StatusCode, Err: fmt.Errorf("ollama: response not done")}
	}

	return core.ChatResponse{
		Message:      parsed.Message,
		FinishReason: parsed.DoneReason,
		Usage: core.Usage{
			PromptTokens:     parsed.PromptEvalCount,
			CompletionTokens: parsed.EvalCount,
		},
	}, nil
}
