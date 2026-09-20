//go:build integration

package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

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

	// RequestID is unique per run so this test can never be confused by a
	// leftover message from a previous run, a smoke test, or another test
	// sharing the same LocalStack queue.
	event := Event{
		EventVersion:     1,
		RequestID:        fmt.Sprintf("req-%d", time.Now().UnixNano()),
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

	// The queue may hold leftover messages from a previous run, a smoke
	// test, or another test sharing the same LocalStack instance, so scan
	// (and drain) several receives looking for the one matching this run's
	// unique RequestID rather than trusting the very first message back.
	var got Event
	found := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !found {
		recv, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     2,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		for _, m := range recv.Messages {
			var candidate Event
			if err := json.Unmarshal([]byte(*m.Body), &candidate); err == nil && candidate.RequestID == event.RequestID {
				got = candidate
				found = true
			}
			// Drain every message seen (matching or not) so a leftover
			// message never blocks a later run either.
			client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
				QueueUrl:      &queueURL,
				ReceiptHandle: m.ReceiptHandle,
			})
		}
	}
	if !found {
		t.Fatalf("expected to receive the published message (request_id=%s) back, got none", event.RequestID)
	}

	if got.EventVersion != 1 {
		t.Fatalf("expected event_version 1, got %d", got.EventVersion)
	}
	if got.TenantID != event.TenantID || got.Alias != event.Alias {
		t.Fatalf("received event does not match published event: %+v", got)
	}
}
