package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"strconv"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Mouraovicente/ai-gateway/internal/api"
	"github.com/Mouraovicente/ai-gateway/internal/auth"
	"github.com/Mouraovicente/ai-gateway/internal/backend/gemini"
	"github.com/Mouraovicente/ai-gateway/internal/backend/ollama"
	"github.com/Mouraovicente/ai-gateway/internal/backend/openrouter"
	"github.com/Mouraovicente/ai-gateway/internal/budget"
	"github.com/Mouraovicente/ai-gateway/internal/config"
	"github.com/Mouraovicente/ai-gateway/internal/memstore"
	"github.com/Mouraovicente/ai-gateway/internal/ratelimit"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/stats"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
	"github.com/Mouraovicente/ai-gateway/internal/usage"
)

// strPtr returns a pointer to s, for AWS SDK request fields that take *string.
func strPtr(s string) *string { return &s }

// atoiDefault parses s as an int, falling back to def when it is unset or
// not a positive number.
func atoiDefault(s string, def int) int {
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

// getenv returns the environment variable named by key, or def if it is unset or empty.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	logger := trace.NewLogger(os.Stdout)
	slog.SetDefault(logger)
	ctx := context.Background()

	routingPath := getenv("ROUTING_CONFIG", "config/routing.yaml")
	routing, err := config.LoadRouting(routingPath)
	if err != nil {
		logger.Error("loading routing config", "error", err)
		os.Exit(1)
	}

	// STORE_BACKEND=memory swaps all four storage ports for in-process
	// implementations: no AWS at all. It exists for local development and
	// for the benchmark that needs to measure this gateway rather than
	// LocalStack's emulator. Never for production, hence the warning below.
	memoryMode := strings.EqualFold(getenv("STORE_BACKEND", "dynamodb"), "memory")

	var dynamoClient *dynamodb.Client
	var sqsClient *sqs.Client
	if !memoryMode {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			logger.Error("loading AWS config", "error", err)
			os.Exit(1)
		}
		// AWS_ENDPOINT_URL points DynamoDB calls at LocalStack in local/dev;
		// in a real AWS account it is unset and the SDK resolves the real
		// endpoint.
		if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
			awsCfg.BaseEndpoint = &endpoint
		}
		dynamoClient = dynamodb.NewFromConfig(awsCfg)
		sqsClient = sqs.NewFromConfig(awsCfg)
	}

	// Only providers with the credentials/URL to actually reach them are
	// registered. Resolving to an unregistered provider surfaces later as an
	// ordinary resilience "no backend registered" attempt, not a boot-time
	// failure — so a gateway missing one API key can still serve the others.
	// The env var holding a provider's key comes from routing.yaml
	// (Provider.APIKeyEnv), never a name hardcoded here: adding a new
	// provider to routing.yaml is enough, no code change needed to pick up
	// its key. Ollama needs no key, only a reachable base URL.
	backends := map[string]resilience.FullBackend{}
	registered := make([]string, 0, len(routing.Providers))
	// hasPaidProvider is /readyz's explicit signal that a non-Ollama backend
	// is configured (a key is present), replacing the earlier
	// len(registered) > 1 heuristic, which broke as soon as Ollama itself
	// was ever left unregistered.
	hasPaidProvider := false

	var ollamaClient *ollama.Client
	if p, ok := routing.Providers["ollama"]; ok {
		baseURL := getenv("OLLAMA_BASE_URL", p.BaseURL)
		if baseURL != "" {
			ollamaClient = ollama.NewClient(baseURL)
			backends["ollama"] = ollamaClient
			registered = append(registered, "ollama")
		}
	}
	if p, ok := routing.Providers["openrouter"]; ok && p.APIKeyEnv != "" {
		if key := os.Getenv(p.APIKeyEnv); key != "" {
			backends["openrouter"] = openrouter.NewClient(p.BaseURL, key)
			registered = append(registered, "openrouter")
			hasPaidProvider = true
		}
	}
	if p, ok := routing.Providers["gemini"]; ok && p.APIKeyEnv != "" {
		if key := os.Getenv(p.APIKeyEnv); key != "" {
			backends["gemini"] = gemini.NewClient(p.BaseURL, key)
			registered = append(registered, "gemini")
			hasPaidProvider = true
		}
	}
	logger.Info("registered backend providers", "providers", registered)

	// The four storage ports, resolved once by backend so nothing below
	// needs to know which one is in play.
	var (
		authStore   auth.Store
		budgetStore budget.Store
		traceBase   trace.Store
		usageBase   usage.Publisher
	)
	if memoryMode {
		logger.Warn("STORE_BACKEND=memory: tenants, budgets, traces and usage events live in process memory only. " +
			"Nothing is persisted, nothing is shared between replicas, and the dev API keys are public. " +
			"This mode is for local development and benchmarks ONLY — never run it in production.")
		devTenantsPath := getenv("DEV_TENANTS_CONFIG", "config/tenants.dev.yaml")
		devTenants, err := config.LoadDevTenants(devTenantsPath)
		if err != nil {
			logger.Error("loading dev tenants", "error", err)
			os.Exit(1)
		}
		authStore = memstore.NewAuthStore(devTenants)
		budgetStore = memstore.NewBudgetStore()
		traceBase = memstore.NewTraceStore()
		usageBase = memstore.NewUsagePublisher()
		logger.Info("memory store backend ready", "tenants", len(devTenants), "config", devTenantsPath)
	} else {
		usageQueueName := getenv("USAGE_QUEUE_NAME", "usage-events")
		usageQueueOut, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: strPtr(usageQueueName)})
		if err != nil {
			logger.Error("resolving usage queue URL (did terraform apply run?)", "queue", usageQueueName, "error", err)
			os.Exit(1)
		}
		authStore = auth.NewDynamoStore(dynamoClient, getenv("TENANTS_TABLE", "tenants"))
		budgetStore = budget.NewDynamoStore(dynamoClient, getenv("BUDGETS_TABLE", "budgets"), getenv("RESERVATIONS_TABLE", "reservations"))
		traceBase = trace.NewDynamoStore(dynamoClient, getenv("REQUESTS_TABLE", "requests"), getenv("TRACE_EVENTS_TABLE", "trace_events"))
		usageBase = usage.NewSQSPublisher(sqsClient, *usageQueueOut.QueueUrl)
	}

	statsRecorder := stats.NewInMemoryRecorder(5 * time.Minute)

	// OTel is optional: OTEL_EXPORTER_OTLP_ENDPOINT unset means no exporter is
	// installed and trace.Tracer()/the meter fall back to OTel's global
	// no-op implementations, so the gateway behaves exactly as before.
	// Exporter failures never affect requests — spans/metrics are just
	// dropped in the background by the SDK's own batching/export goroutines.
	var tracerProvider *sdktrace.TracerProvider
	var meterProvider *sdkmetric.MeterProvider
	meter := otel.GetMeterProvider().Meter("ai-gateway")
	if otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); otlpEndpoint != "" {
		tracerProvider, err = trace.NewTracerProvider(ctx, otlpEndpoint)
		if err != nil {
			logger.Error("starting tracer provider", "error", err)
			os.Exit(1)
		}
		meterProvider, err = trace.NewMeterProvider(ctx, otlpEndpoint)
		if err != nil {
			logger.Error("starting meter provider", "error", err)
			os.Exit(1)
		}
		meter = meterProvider.Meter("ai-gateway")
	}
	metrics, err := trace.NewMetrics(meter)
	if err != nil {
		logger.Error("registering metrics", "error", err)
		os.Exit(1)
	}

	// Trace and usage writes leave the request path here: the pipeline still
	// calls RecordRequest/RecordEvent/Publish, but those now enqueue and
	// return instead of doing ~9 DynamoDB round trips plus one SQS publish
	// inline. Both queues are bounded and drop (counted, logged, never
	// blocking) rather than add latency under pressure.
	var asyncTrace *trace.AsyncStore
	var asyncUsage *usage.AsyncPublisher
	queueInstruments, err := trace.NewQueueInstruments(meter,
		func() int64 {
			if asyncTrace == nil {
				return 0
			}
			return asyncTrace.QueueDepth()
		},
		func() int64 {
			if asyncUsage == nil {
				return 0
			}
			return asyncUsage.QueueDepth()
		})
	if err != nil {
		logger.Error("registering queue metrics", "error", err)
		os.Exit(1)
	}
	asyncTrace = trace.NewAsyncStore(traceBase,
		atoiDefault(os.Getenv("TRACE_QUEUE_SIZE"), trace.DefaultTraceQueueSize),
		logger, &trace.AsyncQueueMetrics{Dropped: queueInstruments.TraceDropped})
	asyncUsage = usage.NewAsyncPublisher(usageBase,
		atoiDefault(os.Getenv("USAGE_QUEUE_SIZE"), usage.DefaultQueueSize),
		logger, queueInstruments.UsageDropped)

	// One IP limiter shared by the chat pipeline and /stats: both hit the
	// same tenant store, so they must share the same pre-auth budget.
	ipLimiter := ratelimit.NewIPLimiter(atoiDefault(os.Getenv("IP_AUTH_FAILURES_PER_MINUTE"), ratelimit.DefaultIPFailuresPerMinute))

	pipeline := &api.Pipeline{
		IPLimit:   ipLimiter,
		Auth:      authStore,
		RateLimit: ratelimit.NewInMemoryLimiter(),
		Budget:    budgetStore,
		Routing:   routing,
		Backends:  backends,
		Trace:     asyncTrace,
		Usage:     asyncUsage,
		Stats:     statsRecorder,
		Tracer:    trace.Tracer(),
		Metrics:   metrics,
		Logger:    logger,
	}

	tenantsTable := getenv("TENANTS_TABLE", "tenants")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		// In memory mode there is no store to be unavailable.
		if dynamoClient != nil {
			if _, err := dynamoClient.DescribeTable(checkCtx, &dynamodb.DescribeTableInput{TableName: strPtr(tenantsTable)}); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"status":"store unavailable"}`))
				return
			}
		}
		// At least one backend must be reachable: Ollama is checked live via
		// Tags; a paid provider counts as ready simply by having a
		// configured API key (never call a paid backend just to warm up).
		backendReady := hasPaidProvider
		if ollamaClient != nil {
			if _, err := ollamaClient.Tags(checkCtx); err == nil {
				backendReady = true
			}
		}
		if !backendReady {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"no backend reachable"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	aliases := make([]string, 0, len(routing.Aliases))
	for alias := range routing.Aliases {
		aliases = append(aliases, alias)
	}

	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
		var tags []string
		if ollamaClient != nil {
			tags, _ = ollamaClient.Tags(r.Context())
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": api.BuildModelsList(aliases, tags)})
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Same pre-auth guard as the chat path: /stats hits the same tenant
		// store, so it is the same free brute-force oracle if left open.
		clientIP := api.ClientIP(r)
		if !ipLimiter.Allowed(clientIP) {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":{"type":"rate_limited"}}`))
			return
		}
		apiKey, ok := api.ParseBearer(r.Header.Get("Authorization"))
		if !ok {
			ipLimiter.RecordFailure(clientIP)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":{"type":"missing_api_key"}}`))
			return
		}
		tenant, err := authStore.ResolveAPIKey(r.Context(), apiKey)
		if err != nil {
			if !errors.Is(err, auth.ErrUnknownAPIKey) {
				logger.Error("auth: tenant store unavailable", "route", "/stats", "error", err.Error())
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"error":{"type":"store_unavailable"}}`))
				return
			}
			ipLimiter.RecordFailure(clientIP)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":{"type":"invalid_api_key"}}`))
			return
		}
		json.NewEncoder(w).Encode(statsRecorder.Snapshot(tenant.ID))
	})

	mux.Handle("POST /v1/chat/completions", api.NewPipelineChatHandler(pipeline))

	handler := api.SecurityHeadersMiddleware(api.RequestIDMiddleware(mux))

	addr := getenv("GATEWAY_ADDR", ":8080")
	// No WriteTimeout on purpose: it would cut legitimate long SSE streams.
	// The slow-client cut comes from the per-chunk write deadline in the
	// pipeline instead (api.Pipeline.StreamWriteTimeout).
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// serveErr carries a real listen failure out of the goroutine;
	// shutdownDone is what main blocks on, so the process does not exit the
	// instant ListenAndServe returns ErrServerClosed (which happens as soon
	// as Shutdown *starts*, aborting in-flight streams and skipping every
	// flush below).
	serveErr := make(chan error, 1)
	shutdownDone := make(chan struct{})

	go func() {
		defer close(shutdownDone)
		<-shutdownCtx.Done()
		logger.Info("shutting down ai-gateway")
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := server.Shutdown(timeoutCtx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
		}
		// Only once in-flight requests are done: drain the async queues, so
		// the last requests' trace and usage events are not lost.
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
		if err := asyncTrace.Close(drainCtx); err != nil {
			logger.Error("trace queue drain failed", "error", err)
		}
		if err := asyncUsage.Close(drainCtx); err != nil {
			logger.Error("usage queue drain failed", "error", err)
		}
		cancelDrain()
		// Independent 5s timeouts: a stuck trace exporter must not delay (or
		// get short-changed by) the metrics provider's own shutdown, and
		// vice versa.
		if tracerProvider != nil {
			tracerShutdownCtx, cancelTracer := context.WithTimeout(context.Background(), 5*time.Second)
			if err := tracerProvider.Shutdown(tracerShutdownCtx); err != nil {
				logger.Error("tracer provider shutdown failed", "error", err)
			}
			cancelTracer()
		}
		if meterProvider != nil {
			meterShutdownCtx, cancelMeter := context.WithTimeout(context.Background(), 5*time.Second)
			if err := meterProvider.Shutdown(meterShutdownCtx); err != nil {
				logger.Error("meter provider shutdown failed", "error", err)
			}
			cancelMeter()
		}
	}()

	logger.Info("starting ai-gateway", "addr", addr)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	if err := <-serveErr; err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
	// ListenAndServe returned ErrServerClosed: wait for the shutdown
	// sequence (in-flight requests, queue drains, exporter flushes) to
	// actually finish before the process exits.
	<-shutdownDone
	logger.Info("ai-gateway stopped")
}
