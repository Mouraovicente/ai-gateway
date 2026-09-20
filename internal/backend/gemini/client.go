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
	SystemInstruction *content  `json:"systemInstruction,omitempty"`
	Contents          []content `json:"contents"`
}

type candidate struct {
	Content      content `json:"content"`
	FinishReason string  `json:"finishReason"`
}

type usageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
}

type promptFeedback struct {
	BlockReason string `json:"blockReason"`
}

type responseBody struct {
	Candidates     []candidate     `json:"candidates"`
	UsageMetadata  *usageMetadata  `json:"usageMetadata"`
	PromptFeedback *promptFeedback `json:"promptFeedback"`
}

// toRequestBody splits messages into Gemini's contents (user/model turns)
// and systemInstruction (system messages don't belong in contents; Gemini
// rejects role "system" there), concatenating multiple system messages.
func toRequestBody(messages []core.Message) requestBody {
	contents := make([]content, 0, len(messages))
	var systemParts []string
	for _, m := range messages {
		if m.Role == "system" {
			systemParts = append(systemParts, m.Content)
			continue
		}
		role := m.Role
		if role == "assistant" {
			role = "model" // Gemini uses "model", not "assistant"
		}
		contents = append(contents, content{Role: role, Parts: []part{{Text: m.Content}}})
	}
	body := requestBody{Contents: contents}
	if len(systemParts) > 0 {
		body.SystemInstruction = &content{Parts: []part{{Text: strings.Join(systemParts, "\n")}}}
	}
	return body
}

// blockedError builds the Permanent BackendError for a 200 response that
// carries no candidates: either the prompt was blocked (promptFeedback set)
// or the response is simply empty. Neither is worth retrying.
func blockedError(status int, feedback *promptFeedback) *core.BackendError {
	if feedback != nil && feedback.BlockReason != "" {
		return &core.BackendError{Class: core.Permanent, Status: status, Err: fmt.Errorf("gemini: prompt blocked (%s)", feedback.BlockReason)}
	}
	return &core.BackendError{Class: core.Permanent, Status: status, Err: fmt.Errorf("gemini: empty candidates")}
}

func classify(status int) core.ErrorClass {
	if status >= 500 || status == http.StatusTooManyRequests {
		return core.Transient
	}
	return core.Permanent
}

// Chat performs a single non-streaming call to Gemini's generateContent.
func (c *Client) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	body, err := json.Marshal(toRequestBody(req.Messages))
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
	if len(parsed.Candidates) == 0 {
		return core.ChatResponse{}, blockedError(resp.StatusCode, parsed.PromptFeedback)
	}
	if len(parsed.Candidates[0].Content.Parts) == 0 {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: candidate has no content parts")}
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

		body, err := json.Marshal(toRequestBody(req.Messages))
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
				errs <- blockedError(resp.StatusCode, parsed.PromptFeedback)
				return
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
