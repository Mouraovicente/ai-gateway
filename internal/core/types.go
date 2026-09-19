package core

import "context"

// Message is one turn in a chat conversation, OpenAI-compatible shape.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the gateway's internal representation of an incoming
// POST /v1/chat/completions request, after the api module parses HTTP.
type ChatRequest struct {
	RequestID string
	TenantID  string
	Alias     string // e.g. "nuva/fast", or a direct "ollama/qwen2.5-coder:1.5b"
	Tier      string
	Messages  []Message
	MaxTokens int
	Stream    bool
}

// Usage reports token counts for a completed (or partially completed) call.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// ChatChunk is one Server-Sent Events delta emitted during streaming.
type ChatChunk struct {
	Delta        string
	FinishReason string
	Usage        *Usage // only set on the final chunk, when the backend reports it
}

// ChatResponse is the full, non-streaming completion result.
type ChatResponse struct {
	Message      Message
	Usage        Usage
	FinishReason string
}

// StreamBackend is implemented by every backend adapter (ollama, openrouter,
// gemini) for the streaming call path. resilience.CallStream (Task 9) uses
// this interface, resolved per BackendTarget, to try each target's stream in
// order without depending on any concrete backend package.
type StreamBackend interface {
	ChatStream(ctx context.Context, model string, req ChatRequest) (<-chan ChatChunk, <-chan error)
}

// BackendTarget names one entry in a routing cascade: a provider and its model.
type BackendTarget struct {
	Provider string
	Model    string
}

// Tenant is the authenticated caller's identity and limits.
type Tenant struct {
	ID                 string
	APIKeyHash         string
	Tier               string
	RPMLimit           int
	MonthlyTokenBudget int
}

// Reservation is a pending budget hold created by budget.Reserve.
type Reservation struct {
	ID              string
	TenantID        string
	Period          string // "YYYY-MM"
	EstimatedTokens int
}
