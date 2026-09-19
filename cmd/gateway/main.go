package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Mouraovicente/ai-gateway/internal/api"
	"github.com/Mouraovicente/ai-gateway/internal/backend/ollama"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ollamaBaseURL := os.Getenv("OLLAMA_BASE_URL")
	if ollamaBaseURL == "" {
		ollamaBaseURL = "http://localhost:11434"
	}
	client := ollama.NewClient(ollamaBaseURL)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("POST /v1/chat/completions", api.NewChatHandler(client, "qwen2.5-coder:1.5b"))

	handler := api.RequestIDMiddleware(mux)

	addr := ":8080"
	logger.Info("starting ai-gateway", "addr", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
