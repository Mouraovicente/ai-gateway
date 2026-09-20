package openrouter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// Client talks to OpenRouter's OpenAI-compatible /v1/chat/completions endpoint.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{baseURL: baseURL, apiKey: apiKey, http: &http.Client{}}
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type requestBody struct {
	Model         string         `json:"model"`
	Messages      []core.Message `json:"messages"`
	Stream        bool           `json:"stream"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
	MaxTokens     int            `json:"max_tokens,omitempty"`
}

type responseBody struct {
	Choices []struct {
		Message      core.Message `json:"message"`
		Delta        core.Message `json:"delta"`
		FinishReason string       `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"error"`
}

func classify(status int) core.ErrorClass {
	if status >= 500 || status == http.StatusTooManyRequests {
		return core.Transient
	}
	return core.Permanent
}

// Chat performs a single non-streaming call to OpenRouter.
func (c *Client) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	body, err := json.Marshal(requestBody{Model: model, Messages: req.Messages, Stream: false, MaxTokens: req.MaxTokens})
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: marshaling request: %w", err)}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: building request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	if req.RequestID != "" {
		httpReq.Header.Set("X-Request-Id", req.RequestID)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: request failed: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: reading response: %w", err)}
	}
	if resp.StatusCode >= 400 {
		return core.ChatResponse{}, &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("openrouter: %s", string(raw))}
	}

	var parsed responseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: parsing response: %w", err)}
	}
	if len(parsed.Choices) == 0 {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: no choices in response")}
	}

	usage := core.Usage{}
	if parsed.Usage != nil {
		usage = core.Usage{PromptTokens: parsed.Usage.PromptTokens, CompletionTokens: parsed.Usage.CompletionTokens}
	}
	return core.ChatResponse{
		Message:      parsed.Choices[0].Message,
		FinishReason: parsed.Choices[0].FinishReason,
		Usage:        usage,
	}, nil
}

// ChatStream performs a streaming call. OpenRouter requires
// stream_options.include_usage=true to get a final usage chunk (spec S1).
func (c *Client) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		body, err := json.Marshal(requestBody{Model: model, Messages: req.Messages, Stream: true, StreamOptions: &streamOptions{IncludeUsage: true}, MaxTokens: req.MaxTokens})
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: marshaling request: %w", err)}
			return
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: building request: %w", err)}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
		if req.RequestID != "" {
			httpReq.Header.Set("X-Request-Id", req.RequestID)
		}

		resp, err := c.http.Do(httpReq)
		if err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: request failed: %w", err)}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			raw, _ := io.ReadAll(resp.Body)
			errs <- &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("openrouter: %s", string(raw))}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")
			if data == "[DONE]" {
				return
			}
			var parsed responseBody
			if err := json.Unmarshal([]byte(data), &parsed); err != nil {
				errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: parsing stream chunk: %w", err)}
				return
			}
			if parsed.Usage != nil {
				select {
				case chunks <- core.ChatChunk{Usage: &core.Usage{PromptTokens: parsed.Usage.PromptTokens, CompletionTokens: parsed.Usage.CompletionTokens}}:
				case <-ctx.Done():
					errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: %w", ctx.Err())}
					return
				}
				continue
			}
			if len(parsed.Choices) > 0 {
				select {
				case chunks <- core.ChatChunk{Delta: parsed.Choices[0].Delta.Content, FinishReason: parsed.Choices[0].FinishReason}:
				case <-ctx.Done():
					errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: %w", ctx.Err())}
					return
				}
			}
		}
		if err := scanner.Err(); err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: reading stream: %w", err)}
			return
		}
		errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: stream truncated (no [DONE] marker)")}
	}()

	return chunks, errs
}
