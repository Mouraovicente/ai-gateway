package ollama

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestTags_ListsModelNames(t *testing.T) {
	body, err := os.ReadFile("testdata/tags_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	names, err := client.Tags(context.Background())
	if err != nil {
		t.Fatalf("Tags returned error: %v", err)
	}
	if len(names) == 0 {
		t.Fatalf("expected at least one model name")
	}
	found := false
	for _, n := range names {
		if n == "qwen2.5-coder:1.5b" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected qwen2.5-coder:1.5b in names: %+v", names)
	}
}
