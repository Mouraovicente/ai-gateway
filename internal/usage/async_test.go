package usage

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakePublisher struct {
	mu     sync.Mutex
	events []Event
	block  chan struct{}
	err    error
}

func (f *fakePublisher) Publish(ctx context.Context, event Event) error {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return f.err
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestAsyncPublisher_PublishesAndDrainsOnClose(t *testing.T) {
	under := &fakePublisher{}
	p := NewAsyncPublisher(under, 100, quietLogger(), nil)
	for i := 0; i < 20; i++ {
		if err := p.Publish(context.Background(), Event{RequestID: "req-1"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if under.count() != 20 {
		t.Fatalf("delivered %d events, want 20", under.count())
	}
}

func TestAsyncPublisher_DropsWhenFullWithoutBlocking(t *testing.T) {
	block := make(chan struct{})
	under := &fakePublisher{block: block}
	p := NewAsyncPublisher(under, 2, quietLogger(), nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			_ = p.Publish(context.Background(), Event{RequestID: "req-1"})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on a full queue; it must drop instead")
	}
	if p.Dropped() == 0 {
		t.Fatal("expected dropped events to be counted")
	}
	close(block)
	_ = p.Close(context.Background())
}

func TestAsyncPublisher_CloseHonorsDeadline(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	p := NewAsyncPublisher(&fakePublisher{block: block}, 10, quietLogger(), nil)
	_ = p.Publish(context.Background(), Event{RequestID: "req-1"})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := p.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close err = %v, want DeadlineExceeded", err)
	}
}

func TestAsyncPublisher_WorkerFailureIsNotSurfacedToCaller(t *testing.T) {
	under := &fakePublisher{err: errors.New("sqs down")}
	p := NewAsyncPublisher(under, 10, quietLogger(), nil)
	if err := p.Publish(context.Background(), Event{RequestID: "req-1"}); err != nil {
		t.Fatalf("Publish must never fail the request: %v", err)
	}
	_ = p.Close(context.Background())
}
