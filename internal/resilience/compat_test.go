package resilience

import (
	"github.com/Mouraovicente/ai-gateway/internal/backend/gemini"
	"github.com/Mouraovicente/ai-gateway/internal/backend/ollama"
	"github.com/Mouraovicente/ai-gateway/internal/backend/openrouter"
)

// Compile-time checks that every concrete backend adapter satisfies
// resilience.FullBackend, so a signature drift in either package fails the
// build here instead of silently breaking Task 11's pipeline wiring.
var (
	_ FullBackend = (*ollama.Client)(nil)
	_ FullBackend = (*openrouter.Client)(nil)
	_ FullBackend = (*gemini.Client)(nil)
)
