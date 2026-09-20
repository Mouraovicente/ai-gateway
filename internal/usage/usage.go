package usage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// AttemptRecord is one backend attempt, as recorded in usage_event.attempts.
type AttemptRecord struct {
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	Status    string `json:"status"`
	LatencyMs int    `json:"latency_ms"`
}

// Event is usage_event v1, published to SQS for every completed or failed
// request. It never contains message content — metadata only.
type Event struct {
	EventVersion     int             `json:"event_version"`
	RequestID        string          `json:"request_id"`
	TenantID         string          `json:"tenant_id"`
	Tier             string          `json:"tier"`
	Alias            string          `json:"alias"`
	Provider         string          `json:"provider"`
	Model            string          `json:"model"`
	PromptTokens     int             `json:"prompt_tokens"`
	CompletionTokens int             `json:"completion_tokens"`
	TTFTMs           int             `json:"ttft_ms"`
	LatencyMs        int             `json:"latency_ms"`
	Status           string          `json:"status"`
	ErrorClass       string          `json:"error_class"`
	Attempts         []AttemptRecord `json:"attempts"`
	TS               string          `json:"ts"`
}

// Publisher sends a usage_event to the queue.
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

// sendMessageAPI is the subset of *sqs.Client this package needs, so unit
// tests can substitute a fake without a real AWS endpoint; the integration
// test in usage_test.go uses the real client against LocalStack.
type sendMessageAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type sqsPublisher struct {
	client   sendMessageAPI
	queueURL string
}

// NewSQSPublisher builds a Publisher backed by the given SQS queue URL.
func NewSQSPublisher(client *sqs.Client, queueURL string) Publisher {
	return &sqsPublisher{client: client, queueURL: queueURL}
}

func (p *sqsPublisher) Publish(ctx context.Context, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("usage: marshaling event: %w", err)
	}
	bodyStr := string(body)
	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &p.queueURL, MessageBody: &bodyStr})
	if err != nil {
		return fmt.Errorf("usage: sending message: %w", err)
	}
	return nil
}
