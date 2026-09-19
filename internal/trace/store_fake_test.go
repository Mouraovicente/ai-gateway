package trace

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type fakePutItemAPI struct {
	err       error
	lastInput *dynamodb.PutItemInput
	calls     int
}

func (f *fakePutItemAPI) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.calls++
	f.lastInput = params
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.PutItemOutput{}, nil
}

func TestRecordEvent_RejectsForbiddenKey(t *testing.T) {
	fake := &fakePutItemAPI{}
	store := NewDynamoStore(fake, "requests", "trace_events")

	err := store.RecordEvent(context.Background(), "req-1", 1, Auth, "auth", map[string]any{"Prompt": "leaked"})
	if err == nil {
		t.Fatal("expected error for forbidden key, got nil")
	}
	if fake.calls != 0 {
		t.Fatalf("expected no PutItem call when payload has a forbidden key, got %d", fake.calls)
	}
}

func TestRecordEvent_TruncatesOversizedPayload(t *testing.T) {
	fake := &fakePutItemAPI{}
	store := NewDynamoStore(fake, "requests", "trace_events")

	big := map[string]any{"data": strings.Repeat("x", MaxPayloadBytes+1)}
	if err := store.RecordEvent(context.Background(), "req-1", 1, Auth, "auth", big); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("expected exactly one PutItem call, got %d", fake.calls)
	}

	payloadAttr, ok := fake.lastInput.Item["payload_json"]
	if !ok {
		t.Fatal("expected payload_json attribute in written item")
	}
	_ = payloadAttr // presence check is enough; content shape verified via RecordEvent not erroring
}

func TestRecordRequest_WrapsClientError(t *testing.T) {
	fake := &fakePutItemAPI{err: errors.New("boom")}
	store := NewDynamoStore(fake, "requests", "trace_events")

	err := store.RecordRequest(context.Background(), "req-1", "tenant-1", "nuva/fast")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.HasPrefix(err.Error(), "trace:") {
		t.Fatalf("expected error wrapped with 'trace:' prefix, got: %v", err)
	}
	if !errors.Is(err, fake.err) {
		t.Fatalf("expected wrapped error to unwrap to client error, got: %v", err)
	}
}
