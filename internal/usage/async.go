package usage

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// DefaultQueueSize is how many pending usage events the gateway holds
// before dropping. Smaller than the trace queue: one event per request,
// not ~8.
const DefaultQueueSize = 1_000

// publishTimeout is the per-message budget the worker gives SQS, unchanged
// from the synchronous path it replaces.
const publishTimeout = 3 * time.Second

// AsyncPublisher hands Publish off to a single worker goroutine so an SQS
// round trip never sits in the request's deferred cleanup. The queued event
// is metadata only (see Event), so nothing here can hold prompt content.
type AsyncPublisher struct {
	under  Publisher
	queue  chan queuedEvent
	logger *slog.Logger

	dropped     atomic.Int64
	lastWarnSec atomic.Int64
	droppedCtr  metric.Int64Counter

	mu     sync.RWMutex
	closed bool

	workerDone chan struct{}
}

type queuedEvent struct {
	event Event
}

// NewAsyncPublisher wraps under with a bounded queue and starts its worker.
// queueSize <= 0 uses DefaultQueueSize.
func NewAsyncPublisher(under Publisher, queueSize int, logger *slog.Logger, dropped metric.Int64Counter) *AsyncPublisher {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	if logger == nil {
		logger = slog.Default()
	}
	p := &AsyncPublisher{
		under:      under,
		queue:      make(chan queuedEvent, queueSize),
		logger:     logger,
		droppedCtr: dropped,
		workerDone: make(chan struct{}),
	}
	go p.worker()
	return p
}

// QueueDepth reports pending events (gateway_usage_queue_depth).
func (p *AsyncPublisher) QueueDepth() int64 { return int64(len(p.queue)) }

// Dropped reports events lost to a full queue (gateway_usage_dropped_total).
func (p *AsyncPublisher) Dropped() int64 { return p.dropped.Load() }

// Publish enqueues and returns immediately. The returned error is always
// nil: the caller's trace event records "queued", not "delivered" — actual
// delivery failures are logged by the worker with the request id.
func (p *AsyncPublisher) Publish(ctx context.Context, event Event) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		p.drop(event)
		return nil
	}
	select {
	case p.queue <- queuedEvent{event: event}:
	default:
		p.drop(event)
	}
	return nil
}

func (p *AsyncPublisher) drop(event Event) {
	total := p.dropped.Add(1)
	if p.droppedCtr != nil {
		p.droppedCtr.Add(context.Background(), 1)
	}
	nowSec := time.Now().Unix()
	last := p.lastWarnSec.Load()
	if nowSec > last && p.lastWarnSec.CompareAndSwap(last, nowSec) {
		p.logger.Warn("usage: queue full, dropping usage events", "dropped_total", total, "request_id", event.RequestID)
	}
}

func (p *AsyncPublisher) worker() {
	defer close(p.workerDone)
	for q := range p.queue {
		ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
		if err := p.under.Publish(ctx, q.event); err != nil {
			p.logger.Error("usage: failed to publish usage event", "request_id", q.event.RequestID, "error", err.Error())
		}
		cancel()
	}
}

// Close stops accepting events and waits for the queue to drain, bounded
// by ctx.
func (p *AsyncPublisher) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.queue)
	p.mu.Unlock()

	select {
	case <-p.workerDone:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("usage: draining queue: %w", ctx.Err())
	}
}
