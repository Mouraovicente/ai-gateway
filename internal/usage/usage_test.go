//go:build integration

package usage

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func newTestSQSClient(t *testing.T) (*sqs.Client, string) {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = &endpoint })

	out, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: strPtr("usage-events")})
	if err != nil {
		t.Fatalf("getting queue url (did terraform apply run?): %v", err)
	}
	return client, *out.QueueUrl
}

func strPtr(s string) *string { return &s }

func TestPublish_SendsEventVersionOneJSON(t *testing.T) {
	client, queueURL := newTestSQSClient(t)
	publisher := NewSQSPublisher(client, queueURL)

	event := Event{
		EventVersion:     1,
		RequestID:        "req-1",
		TenantID:         "tenant-1",
		Tier:             "free",
		Alias:            "nuva/fast",
		Provider:         "ollama",
		Model:            "qwen2.5-coder:1.5b",
		PromptTokens:     10,
		CompletionTokens: 5,
		LatencyMs:        120,
		Status:           "ok",
		TS:               "2026-09-19T12:00:00Z",
	}
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	recv, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(recv.Messages) == 0 {
		t.Fatalf("expected to receive the published message back, got none")
	}

	var got Event
	if err := json.Unmarshal([]byte(*recv.Messages[0].Body), &got); err != nil {
		t.Fatalf("unmarshaling received message: %v", err)
	}
	if got.EventVersion != 1 {
		t.Fatalf("expected event_version 1, got %d", got.EventVersion)
	}
	if got.RequestID != event.RequestID || got.TenantID != event.TenantID || got.Alias != event.Alias {
		t.Fatalf("received event does not match published event: %+v", got)
	}

	// Clean up so the queue doesn't accumulate test messages across runs.
	client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: recv.Messages[0].ReceiptHandle,
	})
}
