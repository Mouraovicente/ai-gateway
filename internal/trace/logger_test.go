package trace

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLogger_DropsForbiddenKeys(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf)

	logger.Info("request handled",
		"request_id", "req-123",
		"prompt", "this must never be logged",
		"content", "neither must this",
		"messages", []string{"nope"},
		"answer", "nope",
		"payload", map[string]string{"x": "y"},
		"trace", "nope",
		"completion", "nope",
		"status", 200,
	)

	output := buf.String()
	for _, key := range ForbiddenKeys {
		if strings.Contains(output, `"`+key+`"`) {
			t.Fatalf("forbidden key %q leaked into log output: %s", key, output)
		}
	}
	if !strings.Contains(output, `"request_id":"req-123"`) {
		t.Fatalf("expected allowed key request_id to be present: %s", output)
	}
	if !strings.Contains(output, `"status":200`) {
		t.Fatalf("expected allowed key status to be present: %s", output)
	}
}

func TestNewLogger_DropsForbiddenKeysCaseInsensitive(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf)

	logger.Info("request handled",
		"Prompt", "must never be logged",
		"CONTENT", "must never be logged either",
		"status", 200,
	)

	output := buf.String()
	if strings.Contains(output, "Prompt") || strings.Contains(output, "must never be logged") {
		t.Fatalf("forbidden key %q leaked into log output (case-insensitive): %s", "Prompt", output)
	}
	if strings.Contains(output, "CONTENT") || strings.Contains(output, "either") {
		t.Fatalf("forbidden key %q leaked into log output (case-insensitive): %s", "CONTENT", output)
	}
	if !strings.Contains(output, `"status":200`) {
		t.Fatalf("expected allowed key status to be present: %s", output)
	}
}

func TestNewLogger_DropsForbiddenKeysInNestedGroup(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf)

	logger.Info("x", slog.Group("meta", slog.String("prompt", "secret"), slog.String("node", "router")))

	output := buf.String()
	if strings.Contains(output, "prompt") || strings.Contains(output, "secret") {
		t.Fatalf("forbidden key leaked into nested group output: %s", output)
	}
	if !strings.Contains(output, `"node":"router"`) {
		t.Fatalf("expected allowed nested key node to be present: %s", output)
	}
}
