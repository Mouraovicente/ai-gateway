package ollama

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Mouraovicente/ai-gateway/internal/core"
	"github.com/Mouraovicente/ai-gateway/internal/httpx"
)

// Client talks to Ollama's native /api/chat endpoint (not the OpenAI-compatible one).
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: httpx.NewClient()}
}

type chatRequestBody struct {
	Model    string         `json:"model"`
	Messages []core.Message `json:"messages"`
	Stream   bool           `json:"stream"`
	Options  *options       `json:"options,omitempty"`
}

type options struct {
	NumPredict int `json:"num_predict"`
}

func requestOptions(req core.ChatRequest) *options {
	if req.MaxTokens <= 0 {
		return nil
	}
	return &options{NumPredict: req.MaxTokens}
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
	// Total budget for one non-streaming attempt; the streaming path relies
	// on the transport's ResponseHeaderTimeout plus the request context instead.
	ctx, cancel := context.WithTimeout(ctx, httpx.NonStreamTimeout)
	defer cancel()
	body, err := json.Marshal(chatRequestBody{Model: model, Messages: req.Messages, Stream: false, Options: requestOptions(req)})
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: marshaling request: %w", err)}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: building request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.RequestID != "" {
		httpReq.Header.Set("X-Request-Id", req.RequestID)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: request failed: %w", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := httpx.ReadAllLimited(resp.Body)
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

// ChatStream performs a streaming call to Ollama's /api/chat, decoding one
// NDJSON object per line. The chunk channel is closed when the stream ends;
// the error channel receives at most one value.
func (c *Client) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		body, err := json.Marshal(chatRequestBody{Model: model, Messages: req.Messages, Stream: true, Options: requestOptions(req)})
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: marshaling request: %w", err)}
			return
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(body))
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: building request: %w", err)}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if req.RequestID != "" {
			httpReq.Header.Set("X-Request-Id", req.RequestID)
		}

		resp, err := c.http.Do(httpReq)
		if err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: request failed: %w", err)}
			return
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode >= 400 {
			raw, _ := httpx.ReadAllLimited(resp.Body)
			class := core.Permanent
			if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
				class = core.Transient
			}
			errs <- &core.BackendError{Class: class, Status: resp.StatusCode, Err: fmt.Errorf("ollama: %s", string(raw))}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64<<10), httpx.MaxStreamLineBytes)
		sawDone := false
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var parsed chatResponseBody
			if err := json.Unmarshal(line, &parsed); err != nil {
				errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: parsing stream line: %w", err)}
				return
			}
			if parsed.Done {
				sawDone = true
				select {
				case chunks <- core.ChatChunk{
					FinishReason: parsed.DoneReason,
					Usage: &core.Usage{
						PromptTokens:     parsed.PromptEvalCount,
						CompletionTokens: parsed.EvalCount,
					},
				}:
				case <-ctx.Done():
					errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: %w", ctx.Err())}
				}
				return
			}
			select {
			case chunks <- core.ChatChunk{Delta: parsed.Message.Content}:
			case <-ctx.Done():
				errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: %w", ctx.Err())}
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: reading stream: %w", err)}
			return
		}
		if !sawDone {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: stream truncated (no done:true)")}
		}
	}()

	return chunks, errs
}
