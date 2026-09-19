package trace

import (
	"bytes"
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
