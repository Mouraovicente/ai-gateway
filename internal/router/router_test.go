package router

import (
	"testing"

	"github.com/Mouraovicente/ai-gateway/internal/config"
)

func testRouting() *config.Routing {
	return &config.Routing{
		Aliases: map[string]config.AliasTiers{
			"nuva/fast": {Tiers: map[string][]config.BackendTargetConfig{
				"free":     {{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}},
				"standard": {{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}, {Provider: "openrouter", Model: "openai/gpt-4o-mini"}},
				"premium":  {{Provider: "openrouter", Model: "openai/gpt-4o-mini"}},
			}},
		},
	}
}

func TestResolve_KnownAliasReturnsOrderedCascade(t *testing.T) {
	targets, err := Resolve(testRouting(), "nuva/fast", "standard")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if len(targets) != 2 || targets[0].Provider != "ollama" || targets[1].Provider != "openrouter" {
		t.Fatalf("unexpected targets: %+v", targets)
	}
}

func TestResolve_UnknownAliasReturnsErrUnknownModel(t *testing.T) {
	if _, err := Resolve(testRouting(), "nuva/does-not-exist", "standard"); err != ErrUnknownModel {
		t.Fatalf("expected ErrUnknownModel, got %v", err)
	}
}

func TestResolve_DirectBackendNameOnlyAllowedInPremium(t *testing.T) {
	targets, err := Resolve(testRouting(), "ollama/qwen2.5-coder:1.5b", "premium")
	if err != nil {
		t.Fatalf("expected direct backend name to resolve in premium tier: %v", err)
	}
	if len(targets) != 1 || targets[0].Provider != "ollama" || targets[0].Model != "qwen2.5-coder:1.5b" {
		t.Fatalf("unexpected targets: %+v", targets)
	}

	if _, err := Resolve(testRouting(), "ollama/qwen2.5-coder:1.5b", "free"); err != ErrUnknownModel {
		t.Fatalf("expected direct backend name to be rejected outside premium, got %v", err)
	}
}
