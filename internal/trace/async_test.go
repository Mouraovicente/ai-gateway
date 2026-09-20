package trace

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeBatchStore records every batch it is handed, so a test can assert
// batch sizes and per-request ordering.
type fakeBatchStore struct {
	mu       sync.Mutex
	batches  [][]EventRecord
	requests []string
	block    chan struct{} // when non-nil, RecordEvents waits on it
	err      error
}

func (f *fakeBatchStore) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, requestID)
	return nil
}

func (f *fakeBatchStore) RecordEvent(ctx context.Context, requestID string, seq int, eventType EventType, node string, payload map[string]any) error {
	return f.RecordEvents(ctx, []EventRecord{{RequestID: requestID, Seq: seq, Type: eventType, Node: node, Payload: payload}})
}

func (f *fakeBatchStore) RecordEvents(ctx context.Context, events []EventRecord) error {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	batch := make([]EventRecord, len(events))
	copy(batch, events)
	f.batches = append(f.batches, batch)
	return f.err
}

func (f *fakeBatchStore) snapshot() [][]EventRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]EventRecord, len(f.batches))
	copy(out, f.batches)
	return out
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAsyncStore_BatchesAndPreservesOrder(t *testing.T) {
	under := &fakeBatchStore{}
	s := NewAsyncStore(under, 1000, quietLogger(), nil)

	const n = 120
	for i := 1; i <= n; i++ {
		if err := s.RecordEvent(context.Background(), "req-1", i, Auth, "auth", map[string]any{"i": i}); err != nil {
			t.Fatalf("RecordEvent: %v", err)
		}
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var seqs []int
	for _, b := range under.snapshot() {
		if len(b) > maxBatchItems {
			t.Fatalf("batch of %d events exceeds DynamoDB's limit of %d", len(b), maxBatchItems)
		}
		for _, e := range b {
			seqs = append(seqs, e.Seq)
		}
	}
	if len(seqs) != n {
		t.Fatalf("wrote %d events, want %d", len(seqs), n)
	}
	for i, seq := range seqs {
		if seq != i+1 {
			t.Fatalf("event order broken at %d: got seq %d", i, seq)
		}
	}
}

func TestAsyncStore_DropsWhenFullWithoutBlocking(t *testing.T) {
	block := make(chan struct{})
	under := &fakeBatchStore{block: block}
	s := NewAsyncStore(under, 4, quietLogger(), nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			_ = s.RecordEvent(context.Background(), "req-1", i, Auth, "auth", nil)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RecordEvent blocked on a full queue; it must drop instead")
	}
	if s.Dropped() == 0 {
		t.Fatal("expected dropped events to be counted")
	}
	close(block)
	_ = s.Close(context.Background())
}

func TestAsyncStore_CloseDrainsQueue(t *testing.T) {
	under := &fakeBatchStore{}
	s := NewAsyncStore(under, 1000, quietLogger(), nil)
	if err := s.RecordRequest(context.Background(), "req-1", "tenant-1", "nuva/fast"); err != nil {
		t.Fatalf("RecordRequest: %v", err)
	}
	for i := 1; i <= 10; i++ {
		_ = s.RecordEvent(context.Background(), "req-1", i, Auth, "auth", nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var total int
	for _, b := range under.snapshot() {
		total += len(b)
	}
	if total != 10 {
		t.Fatalf("drained %d events, want 10", total)
	}
	if len(under.requests) != 1 {
		t.Fatalf("drained %d request rows, want 1", len(under.requests))
	}
	// Close is idempotent and post-close writes are dropped, never a panic.
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := s.RecordEvent(context.Background(), "req-2", 1, Auth, "auth", nil); err != nil {
		t.Fatalf("post-close RecordEvent: %v", err)
	}
}

func TestAsyncStore_CloseHonorsDeadline(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	under := &fakeBatchStore{block: block}
	s := NewAsyncStore(under, 100, quietLogger(), nil)
	_ = s.RecordEvent(context.Background(), "req-1", 1, Auth, "auth", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close err = %v, want DeadlineExceeded", err)
	}
}
