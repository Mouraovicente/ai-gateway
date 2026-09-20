package api

import "testing"

func TestBuildModelsList_ListsAliasesOnly(t *testing.T) {
	data := BuildModelsList([]string{"nuva/fast", "nuva/smart"})
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
	if data[0].ID != "nuva/fast" || data[1].ID != "nuva/smart" {
		t.Fatalf("unexpected ids: %+v", data)
	}
}

// The endpoint is unauthenticated, so the backend inventory must not be in
// the response at all — no provider-prefixed entries, ever.
func TestBuildModelsList_DoesNotExposeBackendInventory(t *testing.T) {
	for _, m := range BuildModelsList([]string{"nuva/fast"}) {
		if m.ID != "nuva/fast" {
			t.Fatalf("unexpected non-alias entry %q in /v1/models", m.ID)
		}
	}
}
