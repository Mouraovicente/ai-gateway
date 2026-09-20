package trace

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// gatewayResource tags every exported span/metric with service.name=ai-gateway,
// which is how the Tempo dashboard panel and its trace search filter on
// "service.name=ai-gateway" find this service's data.
var gatewayResource = resource.NewSchemaless(semconv.ServiceName(tracerName))

// splitEndpoint parses otlpEndpoint (a full URL, e.g. "http://localhost:4318",
// matching the standard OTEL_EXPORTER_OTLP_ENDPOINT convention) into the
// host:port and insecure flag the otlptracehttp/otlpmetrichttp WithEndpoint
// option wants (host and port only, no scheme). Falls back to treating the
// input as already being a bare host:port if it fails to parse as a URL.
func splitEndpoint(otlpEndpoint string) (hostPort string, insecure bool) {
	u, err := url.Parse(otlpEndpoint)
	if err != nil || u.Host == "" {
		return otlpEndpoint, true
	}
	return u.Host, u.Scheme != "https"
}

const tracerName = "ai-gateway"

// NewTracerProvider builds an OTLP-over-HTTP tracer provider pointed at the
// collector. otlpEndpoint is a full URL (e.g. "http://localhost:4318"),
// matching the standard OTEL_EXPORTER_OTLP_ENDPOINT convention. Spans
// emitted per request: the root span for "POST /v1/chat/completions" and
// child spans auth, budget.reserve, route, backend.call (one per attempt),
// budget.settle, usage.publish.
func NewTracerProvider(ctx context.Context, otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	hostPort, insecure := splitEndpoint(otlpEndpoint)
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(hostPort)}
	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("trace: creating OTLP trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(gatewayResource))
	otel.SetTracerProvider(provider)
	return provider, nil
}

// NewMeterProvider builds an OTLP-over-HTTP meter provider for the metrics
// registered by NewMetrics. otlpEndpoint is a full URL (e.g.
// "http://localhost:4318").
func NewMeterProvider(ctx context.Context, otlpEndpoint string) (*sdkmetric.MeterProvider, error) {
	hostPort, insecure := splitEndpoint(otlpEndpoint)
	opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(hostPort)}
	if insecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}
	exporter, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("trace: creating OTLP metric exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(gatewayResource),
	)
	otel.SetMeterProvider(provider)
	return provider, nil
}

// Tracer returns the gateway's named tracer for creating request/stage spans.
// When no tracer provider has been installed (OTel disabled), this returns
// the global no-op tracer, so callers never need a nil check.
func Tracer() oteltrace.Tracer {
	return otel.Tracer(tracerName)
}

// Metrics holds the instruments the spec requires:
//   - gateway_requests_total{route,model,tenant,status}
//   - gateway_latency_ms histogram
//   - gateway_tokens_total{route,model,tenant,kind=prompt|completion}
//   - gateway_ttft_ms histogram
type Metrics struct {
	RequestsTotal metric.Int64Counter
	LatencyMs     metric.Float64Histogram
	TokensTotal   metric.Int64Counter
	TTFTMs        metric.Float64Histogram
}

// NewMetrics registers the four instruments on the given meter.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	requestsTotal, err := meter.Int64Counter("gateway_requests_total")
	if err != nil {
		return nil, fmt.Errorf("trace: creating gateway_requests_total counter: %w", err)
	}
	latencyMs, err := meter.Float64Histogram("gateway_latency_ms")
	if err != nil {
		return nil, fmt.Errorf("trace: creating gateway_latency_ms histogram: %w", err)
	}
	tokensTotal, err := meter.Int64Counter("gateway_tokens_total")
	if err != nil {
		return nil, fmt.Errorf("trace: creating gateway_tokens_total counter: %w", err)
	}
	ttftMs, err := meter.Float64Histogram("gateway_ttft_ms")
	if err != nil {
		return nil, fmt.Errorf("trace: creating gateway_ttft_ms histogram: %w", err)
	}
	return &Metrics{RequestsTotal: requestsTotal, LatencyMs: latencyMs, TokensTotal: tokensTotal, TTFTMs: ttftMs}, nil
}

// QueueInstruments are the async-queue instruments:
//   - gateway_trace_queue_depth / gateway_usage_queue_depth (observable gauges)
//   - gateway_trace_dropped_total / gateway_usage_dropped_total (counters)
//
// The gauges are observed from the supplied closures, so the queues stay
// plain structs with no metric wiring inside them.
type QueueInstruments struct {
	TraceDropped metric.Int64Counter
	UsageDropped metric.Int64Counter
}

// NewQueueInstruments registers the drop counters and the two depth gauges.
func NewQueueInstruments(meter metric.Meter, traceDepth, usageDepth func() int64) (*QueueInstruments, error) {
	traceDropped, err := meter.Int64Counter("gateway_trace_dropped_total")
	if err != nil {
		return nil, fmt.Errorf("trace: creating gateway_trace_dropped_total counter: %w", err)
	}
	usageDropped, err := meter.Int64Counter("gateway_usage_dropped_total")
	if err != nil {
		return nil, fmt.Errorf("trace: creating gateway_usage_dropped_total counter: %w", err)
	}
	if _, err := meter.Int64ObservableGauge("gateway_trace_queue_depth",
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(traceDepth())
			return nil
		})); err != nil {
		return nil, fmt.Errorf("trace: creating gateway_trace_queue_depth gauge: %w", err)
	}
	if _, err := meter.Int64ObservableGauge("gateway_usage_queue_depth",
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(usageDepth())
			return nil
		})); err != nil {
		return nil, fmt.Errorf("trace: creating gateway_usage_queue_depth gauge: %w", err)
	}
	return &QueueInstruments{TraceDropped: traceDropped, UsageDropped: usageDropped}, nil
}
