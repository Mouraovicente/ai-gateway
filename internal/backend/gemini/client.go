package gemini

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

// Client talks to Gemini's generateContent / streamGenerateContent endpoints.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{baseURL: baseURL, apiKey: apiKey, http: &http.Client{}}
}

type part struct {
	Text string `json:"text"`
}

type content struct {
	Role  string `json:"role,omitempty"`
	Parts []part `json:"parts"`
}

type requestBody struct {
	Contents []content `json:"contents"`
}

type candidate struct {
	Content      content `json:"content"`
	FinishReason string  `json:"finishReason"`
}

type usageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
}

type responseBody struct {
	Candidates    []candidate    `json:"candidates"`
	UsageMetadata *usageMetadata `json:"usageMetadata"`
}

func toContents(messages []core.Message) []content {
	contents := make([]content, 0, len(messages))
	for _, m := range messages {
		role := m.Role
		if role == "assistant" {
			role = "model" // Gemini uses "model", not "assistant"
		}
		contents = append(contents, content{Role: role, Parts: []part{{Text: m.Content}}})
	}
	return contents
}

func classify(status int) core.ErrorClass {
	if status >= 500 || status == http.StatusTooManyRequests {
		return core.Transient
	}
	return core.Permanent
}

// Chat performs a single non-streaming call to Gemini's generateContent.
func (c *Client) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	body, err := json.Marshal(requestBody{Contents: toContents(req.Messages)})
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: marshaling request: %w", err)}
	}
	url := fmt.Sprintf("%s/v1beta/models/%s:generateContent", c.baseURL, model)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: building request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-goog-api-key", c.apiKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: request failed: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: reading response: %w", err)}
	}
	if resp.StatusCode >= 400 {
		return core.ChatResponse{}, &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("gemini: %s", string(raw))}
	}

	var parsed responseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: parsing response: %w", err)}
	}
	if len(parsed.Candidates) == 0 || len(parsed.Candidates[0].Content.Parts) == 0 {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: no candidates in response")}
	}

	usage := core.Usage{}
	if parsed.UsageMetadata != nil {
		usage = core.Usage{PromptTokens: parsed.UsageMetadata.PromptTokenCount, CompletionTokens: parsed.UsageMetadata.CandidatesTokenCount}
	}
	return core.ChatResponse{
		Message:      core.Message{Role: "assistant", Content: parsed.Candidates[0].Content.Parts[0].Text},
		FinishReason: parsed.Candidates[0].FinishReason,
		Usage:        usage,
	}, nil
}

// ChatStream performs a streaming call to Gemini's streamGenerateContent?alt=sse.
func (c *Client) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		body, err := json.Marshal(requestBody{Contents: toContents(req.Messages)})
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: marshaling request: %w", err)}
			return
		}
		url := fmt.Sprintf("%s/v1beta/models/%s:streamGenerateContent?alt=sse", c.baseURL, model)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: building request: %w", err)}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("x-goog-api-key", c.apiKey)

		resp, err := c.http.Do(httpReq)
		if err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: request failed: %w", err)}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			raw, _ := io.ReadAll(resp.Body)
			errs <- &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("gemini: %s", string(raw))}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")
			var parsed responseBody
			if err := json.Unmarshal([]byte(data), &parsed); err != nil {
				errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: parsing stream chunk: %w", err)}
				return
			}
			if len(parsed.Candidates) == 0 {
				continue
			}
			cand := parsed.Candidates[0]
			var text string
			if len(cand.Content.Parts) > 0 {
				text = cand.Content.Parts[0].Text
			}
			var usage *core.Usage
			if parsed.UsageMetadata != nil {
				usage = &core.Usage{PromptTokens: parsed.UsageMetadata.PromptTokenCount, CompletionTokens: parsed.UsageMetadata.CandidatesTokenCount}
			}
			select {
			case chunks <- core.ChatChunk{Delta: text, FinishReason: cand.FinishReason, Usage: usage}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return chunks, errs
}
