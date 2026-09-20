package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Mouraovicente/ai-gateway/internal/api"
	"github.com/Mouraovicente/ai-gateway/internal/auth"
	"github.com/Mouraovicente/ai-gateway/internal/backend/gemini"
	"github.com/Mouraovicente/ai-gateway/internal/backend/ollama"
	"github.com/Mouraovicente/ai-gateway/internal/backend/openrouter"
	"github.com/Mouraovicente/ai-gateway/internal/budget"
	"github.com/Mouraovicente/ai-gateway/internal/config"
	"github.com/Mouraovicente/ai-gateway/internal/ratelimit"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/stats"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
	"github.com/Mouraovicente/ai-gateway/internal/usage"
)

// strPtr returns a pointer to s, for AWS SDK request fields that take *string.
func strPtr(s string) *string { return &s }

// getenv returns the environment variable named by key, or def if it is unset or empty.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	logger := trace.NewLogger(os.Stdout)
	ctx := context.Background()

	routingPath := getenv("ROUTING_CONFIG", "config/routing.yaml")
	routing, err := config.LoadRouting(routingPath)
	if err != nil {
		logger.Error("loading routing config", "error", err)
		os.Exit(1)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("loading AWS config", "error", err)
		os.Exit(1)
	}
	// AWS_ENDPOINT_URL points DynamoDB calls at LocalStack in local/dev; in a
	// real AWS account it is unset and the SDK resolves the real endpoint.
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		awsCfg.BaseEndpoint = &endpoint
	}
	dynamoClient := dynamodb.NewFromConfig(awsCfg)

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

	sqsClient := sqs.NewFromConfig(awsCfg)
	usageQueueName := getenv("USAGE_QUEUE_NAME", "usage-events")
	usageQueueOut, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: strPtr(usageQueueName)})
	if err != nil {
		logger.Error("resolving usage queue URL (did terraform apply run?)", "queue", usageQueueName, "error", err)
		os.Exit(1)
	}
	usageQueueURL := *usageQueueOut.QueueUrl

	authStore := auth.NewDynamoStore(dynamoClient, getenv("TENANTS_TABLE", "tenants"))
	statsRecorder := stats.NewInMemoryRecorder(5 * time.Minute)

	pipeline := &api.Pipeline{
		Auth:      authStore,
		RateLimit: ratelimit.NewInMemoryLimiter(),
		Budget:    budget.NewDynamoStore(dynamoClient, getenv("BUDGETS_TABLE", "budgets"), getenv("RESERVATIONS_TABLE", "reservations")),
		Routing:   routing,
		Backends:  backends,
		Trace:     trace.NewDynamoStore(dynamoClient, getenv("REQUESTS_TABLE", "requests"), getenv("TRACE_EVENTS_TABLE", "trace_events")),
		Usage:     usage.NewSQSPublisher(sqsClient, usageQueueURL),
		Stats:     statsRecorder,
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
		if _, err := dynamoClient.DescribeTable(checkCtx, &dynamodb.DescribeTableInput{TableName: strPtr(tenantsTable)}); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"store unavailable"}`))
			return
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
		apiKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if apiKey == "" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":{"type":"missing_api_key"}}`))
			return
		}
		if _, err := authStore.ResolveAPIKey(r.Context(), apiKey); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":{"type":"invalid_api_key"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(statsRecorder.Snapshot())
	})

	mux.Handle("POST /v1/chat/completions", api.NewPipelineChatHandler(pipeline))

	handler := api.RequestIDMiddleware(mux)

	addr := getenv("GATEWAY_ADDR", ":8080")
	server := &http.Server{Addr: addr, Handler: handler}

	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-shutdownCtx.Done()
		logger.Info("shutting down ai-gateway")
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(timeoutCtx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
		}
	}()

	logger.Info("starting ai-gateway", "addr", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
