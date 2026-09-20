package api

import "testing"

func TestBuildModelsList_EveryEntryIsAModelObject(t *testing.T) {
	data := BuildModelsList([]string{"nuva/fast"}, []string{"qwen2.5-coder:1.5b"})
	if len(data) != 2 {
		t.Fatalf("expected 2 entries, got %d: %+v", len(data), data)
	}
	for _, m := range data {
		if m.Object != "model" {
			t.Fatalf("expected object=model, got %+v", m)
		}
		if m.OwnedBy != "ai-gateway" {
			t.Fatalf("expected owned_by=ai-gateway, got %+v", m)
		}
	}
	if data[0].ID != "nuva/fast" {
		t.Fatalf("expected alias id nuva/fast, got %+v", data[0])
	}
	if data[1].ID != "ollama/qwen2.5-coder:1.5b" {
		t.Fatalf("expected ollama-prefixed id, got %+v", data[1])
	}
}
