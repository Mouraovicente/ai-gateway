package trace

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// batchWriteAPI is the optional BatchWriteItem capability. *dynamodb.Client
// has it; the narrow fakes in the unit tests do not, and those simply fall
// back to one PutItem per event.
type batchWriteAPI interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// RecordEvents writes up to maxBatchItems trace_events in one
// BatchWriteItem, retrying whatever DynamoDB reports as unprocessed exactly
// once. It makes *dynamoStore a BatchStore, which is what lets AsyncStore
// collapse a request's ~8 event writes into a single round trip.
func (s *dynamoStore) RecordEvents(ctx context.Context, events []EventRecord) error {
	if len(events) == 0 {
		return nil
	}
	client, ok := s.client.(batchWriteAPI)
	if !ok {
		for _, e := range events {
			if err := s.RecordEvent(ctx, e.RequestID, e.Seq, e.Type, e.Node, e.Payload); err != nil {
				return err
			}
		}
		return nil
	}

	requests := make([]types.WriteRequest, 0, len(events))
	for _, e := range events {
		item, err := s.eventItem(e.RequestID, e.Seq, e.Type, e.Node, e.Payload)
		if err != nil {
			// One bad event must not sink the batch: skip it, keep the rest,
			// but log it — this is the only signal that a forbidden key made
			// it into a trace payload on the async path. Never log the value.
			key := "unknown"
			for k := range e.Payload {
				if isForbidden(k) {
					key = k
					break
				}
			}
			slog.Default().Warn("trace: dropping event with forbidden payload key",
				"request_id", e.RequestID, "seq", e.Seq, "type", e.Type, "key", key)
			continue
		}
		requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}
	if len(requests) == 0 {
		return nil
	}

	for start := 0; start < len(requests); start += maxBatchItems {
		end := min(start+maxBatchItems, len(requests))
		unprocessed := map[string][]types.WriteRequest{s.eventsTable: requests[start:end]}
		// One retry of whatever came back unprocessed; beyond that the
		// events are dropped (and logged by the caller), never retried into
		// an unbounded loop.
		for attempt := 0; attempt < 2 && len(unprocessed) > 0; attempt++ {
			out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: unprocessed})
			if err != nil {
				return fmt.Errorf("trace: batch writing events: %w", err)
			}
			unprocessed = out.UnprocessedItems
		}
		if len(unprocessed) > 0 {
			return fmt.Errorf("trace: %d event(s) left unprocessed after retry", len(unprocessed[s.eventsTable]))
		}
	}
	return nil
}
