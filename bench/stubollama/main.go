// Command stubollama is a tiny stand-in for Ollama's native /api/chat, used
// to benchmark the gateway's own overhead (auth, rate-limit, budget,
// routing, trace, usage publish) independent of a real LLM backend.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type chatResponse struct {
	Message         message `json:"message"`
	Done            bool    `json:"done"`
	DoneReason      string  `json:"done_reason,omitempty"`
	PromptEvalCount int     `json:"prompt_eval_count"`
	EvalCount       int     `json:"eval_count"`
}

func main() {
	addr := flag.String("addr", ":11435", "listen address")
	latency := flag.Duration("latency", 0, "fixed artificial delay before responding")
	tokens := flag.Int("tokens", 5, "number of stream chunks to emit before done")
	flag.Parse()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/tags", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"models": []map[string]any{
			{"name": "qwen2.5-coder:1.5b", "model": "qwen2.5-coder:1.5b"},
		}})
	})

	mux.HandleFunc("POST /api/chat", func(w http.ResponseWriter, r *http.Request) {
		var req chatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if *latency > 0 {
			time.Sleep(*latency)
		}

		if !req.Stream {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(chatResponse{
				Message:         message{Role: "assistant", Content: "pong"},
				Done:            true,
				DoneReason:      "stop",
				PromptEvalCount: 3,
				EvalCount:       1,
			})
			return
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		flusher, _ := w.(http.Flusher)
		enc := json.NewEncoder(w)
		for i := 0; i < *tokens; i++ {
			enc.Encode(chatResponse{Message: message{Role: "assistant", Content: "p"}})
			if flusher != nil {
				flusher.Flush()
			}
		}
		enc.Encode(chatResponse{
			Message:         message{Role: "assistant", Content: ""},
			Done:            true,
			DoneReason:      "stop",
			PromptEvalCount: 3,
			EvalCount:       *tokens,
		})
	})

	log.Printf("stubollama listening on %s (latency=%s tokens=%d)", *addr, *latency, *tokens)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
