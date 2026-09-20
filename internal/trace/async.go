package trace

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// DefaultTraceQueueSize is how many pending trace writes the gateway holds
// before it starts dropping them. Trace persistence is observability, not
// correctness: dropping is always preferable to adding a DynamoDB round
// trip to the client's latency.
const DefaultTraceQueueSize = 10_000

// maxBatchItems is DynamoDB's hard ceiling for BatchWriteItem.
const maxBatchItems = 25

// EventRecord is one queued trace_event, carrying exactly what
// Store.RecordEvent takes.
type EventRecord struct {
	RequestID string
	Seq       int
	Type      EventType
	Node      string
	Payload   map[string]any
}

// BatchStore is the optional batching extension of Store. The DynamoDB
// store implements it (BatchWriteItem, 25 items per call); a store that
// does not is driven one event at a time by AsyncStore instead.
type BatchStore interface {
	Store
	RecordEvents(ctx context.Context, events []EventRecord) error
}

// AsyncQueueMetrics are the two instruments AsyncStore/usage.AsyncPublisher
// report: how deep the queue is, and how much was dropped because it was
// full. Both may be nil (OTel disabled).
type AsyncQueueMetrics struct {
	Dropped metric.Int64Counter
}

// AsyncStore is a Store that never blocks the request: RecordRequest and
// RecordEvent enqueue and return, and a single worker goroutine does the
// DynamoDB writes, batching trace_events up to 25 per BatchWriteItem.
//
// One worker, one FIFO queue: per-request event ordering is preserved
// because seq order into the channel is seq order out of it.
type AsyncStore struct {
	under Store
	batch BatchStore // nil when under cannot batch

	queue  chan any // *EventRecord or *requestRecord
	logger *slog.Logger

	dropped     atomic.Int64
	lastWarnSec atomic.Int64
	droppedCtr  metric.Int64Counter

	mu     sync.RWMutex
	closed bool

	workerDone chan struct{}
}

type requestRecord struct {
	RequestID, TenantID, Alias string
}

// NewAsyncStore wraps under with a bounded queue and starts its worker.
// queueSize <= 0 uses DefaultTraceQueueSize. Call Close to drain.
func NewAsyncStore(under Store, queueSize int, logger *slog.Logger, m *AsyncQueueMetrics) *AsyncStore {
	if queueSize <= 0 {
		queueSize = DefaultTraceQueueSize
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &AsyncStore{
		under:      under,
		queue:      make(chan any, queueSize),
		logger:     logger,
		workerDone: make(chan struct{}),
	}
	if b, ok := under.(BatchStore); ok {
		s.batch = b
	}
	if m != nil {
		s.droppedCtr = m.Dropped
	}
	go s.worker()
	return s
}

// QueueDepth reports how many writes are pending, for the
// gateway_trace_queue_depth gauge.
func (s *AsyncStore) QueueDepth() int64 { return int64(len(s.queue)) }

// Dropped reports the total number of dropped writes
// (gateway_trace_dropped_total).
func (s *AsyncStore) Dropped() int64 { return s.dropped.Load() }

// enqueue is the non-blocking send both Record* methods share.
func (s *AsyncStore) enqueue(item any) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		s.drop()
		return
	}
	select {
	case s.queue <- item:
	default:
		s.drop()
	}
}

// drop counts a dropped write and logs at WARN at most once per second, so
// a sustained overflow cannot turn into a log flood of its own.
func (s *AsyncStore) drop() {
	total := s.dropped.Add(1)
	if s.droppedCtr != nil {
		s.droppedCtr.Add(context.Background(), 1)
	}
	nowSec := time.Now().Unix()
	last := s.lastWarnSec.Load()
	if nowSec > last && s.lastWarnSec.CompareAndSwap(last, nowSec) {
		s.logger.Warn("trace: queue full, dropping trace writes", "dropped_total", total)
	}
}

func (s *AsyncStore) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error {
	s.enqueue(&requestRecord{RequestID: requestID, TenantID: tenantID, Alias: alias})
	return nil
}

func (s *AsyncStore) RecordEvent(ctx context.Context, requestID string, seq int, eventType EventType, node string, payload map[string]any) error {
	s.enqueue(&EventRecord{RequestID: requestID, Seq: seq, Type: eventType, Node: node, Payload: payload})
	return nil
}

// worker drains the queue until it is closed, coalescing consecutive
// EventRecords into one batch of at most maxBatchItems.
func (s *AsyncStore) worker() {
	defer close(s.workerDone)
	pending := make([]EventRecord, 0, maxBatchItems)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		// Detached: the worker outlives any single request context.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		s.writeEvents(ctx, pending)
		cancel()
		pending = pending[:0]
	}
	for item := range s.queue {
		switch v := item.(type) {
		case *EventRecord:
			pending = append(pending, *v)
			// Flush when the batch is full, or when nothing else is
			// waiting (so a quiet gateway does not sit on an event).
			if len(pending) >= maxBatchItems || len(s.queue) == 0 {
				flush()
			}
		case *requestRecord:
			flush() // keep the request row ahead of its own events
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.under.RecordRequest(ctx, v.RequestID, v.TenantID, v.Alias); err != nil {
				s.logger.Error("trace: failed to record request", "request_id", v.RequestID, "error", err.Error())
			}
			cancel()
		}
	}
	flush()
}

func (s *AsyncStore) writeEvents(ctx context.Context, events []EventRecord) {
	if s.batch != nil {
		if err := s.batch.RecordEvents(ctx, events); err != nil {
			s.logger.Error("trace: failed to write event batch", "count", len(events), "error", err.Error())
		}
		return
	}
	for _, e := range events {
		if err := s.under.RecordEvent(ctx, e.RequestID, e.Seq, e.Type, e.Node, e.Payload); err != nil {
			s.logger.Error("trace: failed to record event", "request_id", e.RequestID, "seq", e.Seq, "error", err.Error())
		}
	}
}

// Close stops accepting new writes and waits for the worker to drain what
// is already queued, bounded by ctx.
func (s *AsyncStore) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.queue)
	s.mu.Unlock()

	select {
	case <-s.workerDone:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("trace: draining queue: %w", ctx.Err())
	}
}
