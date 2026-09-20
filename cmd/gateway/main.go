package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/Mouraovicente/ai-gateway/internal/api"
	"github.com/Mouraovicente/ai-gateway/internal/auth"
	"github.com/Mouraovicente/ai-gateway/internal/backend/gemini"
	"github.com/Mouraovicente/ai-gateway/internal/backend/ollama"
	"github.com/Mouraovicente/ai-gateway/internal/backend/openrouter"
	"github.com/Mouraovicente/ai-gateway/internal/budget"
	"github.com/Mouraovicente/ai-gateway/internal/config"
	"github.com/Mouraovicente/ai-gateway/internal/ratelimit"
	"github.com/Mouraovicente/ai-gateway/internal/resilience"
	"github.com/Mouraovicente/ai-gateway/internal/trace"
)

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

	if p, ok := routing.Providers["ollama"]; ok {
		baseURL := getenv("OLLAMA_BASE_URL", p.BaseURL)
		if baseURL != "" {
			backends["ollama"] = ollama.NewClient(baseURL)
			registered = append(registered, "ollama")
		}
	}
	if p, ok := routing.Providers["openrouter"]; ok && p.APIKeyEnv != "" {
		if key := os.Getenv(p.APIKeyEnv); key != "" {
			backends["openrouter"] = openrouter.NewClient(p.BaseURL, key)
			registered = append(registered, "openrouter")
		}
	}
	if p, ok := routing.Providers["gemini"]; ok && p.APIKeyEnv != "" {
		if key := os.Getenv(p.APIKeyEnv); key != "" {
			backends["gemini"] = gemini.NewClient(p.BaseURL, key)
			registered = append(registered, "gemini")
		}
	}
	logger.Info("registered backend providers", "providers", registered)

	pipeline := &api.Pipeline{
		Auth:      auth.NewDynamoStore(dynamoClient, getenv("TENANTS_TABLE", "tenants")),
		RateLimit: ratelimit.NewInMemoryLimiter(),
		Budget:    budget.NewDynamoStore(dynamoClient, getenv("BUDGETS_TABLE", "budgets"), getenv("RESERVATIONS_TABLE", "reservations")),
		Routing:   routing,
		Backends:  backends,
		Trace:     trace.NewDynamoStore(dynamoClient, getenv("REQUESTS_TABLE", "requests"), getenv("TRACE_EVENTS_TABLE", "trace_events")),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
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
