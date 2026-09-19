package config

import "testing"

func TestLoadRouting_ParsesAliasesAndProviders(t *testing.T) {
	r, err := LoadRouting("testdata/routing.yaml")
	if err != nil {
		t.Fatalf("LoadRouting returned error: %v", err)
	}

	fast, ok := r.Aliases["nuva/fast"]
	if !ok {
		t.Fatalf("expected alias nuva/fast to be present")
	}
	freeTier, ok := fast.Tiers["free"]
	if !ok || len(freeTier) != 1 {
		t.Fatalf("expected nuva/fast free tier with 1 target, got %+v", freeTier)
	}
	if freeTier[0].Provider != "ollama" || freeTier[0].Model != "qwen2.5-coder:1.5b" {
		t.Fatalf("unexpected free tier target: %+v", freeTier[0])
	}

	ollama, ok := r.Providers["ollama"]
	if !ok || ollama.BaseURL != "http://localhost:11434" {
		t.Fatalf("unexpected ollama provider config: %+v", ollama)
	}
}

func TestLoadRouting_MissingFile(t *testing.T) {
	if _, err := LoadRouting("testdata/does-not-exist.yaml"); err == nil {
		t.Fatalf("expected error for missing file")
	}
}
