package ollama

import (
	"net"
	"net/url"
	"os"
	"testing"
	"time"
)

// skipIfNoOllama skips the calling test when the live Ollama service isn't
// reachable, so a machine without Ollama running (e.g. most CI runners)
// doesn't fail tests that intentionally exercise the real backend rather
// than an httptest fixture. baseURL defaults to OLLAMA_BASE_URL or
// http://localhost:11434 when empty.
func skipIfNoOllama(t *testing.T, baseURL string) {
	t.Helper()
	if baseURL == "" {
		baseURL = os.Getenv("OLLAMA_BASE_URL")
	}
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Skipf("skipping: invalid Ollama base URL %q: %v", baseURL, err)
		return
	}
	host := u.Host
	if host == "" {
		host = baseURL
	}
	conn, err := net.DialTimeout("tcp", host, 500*time.Millisecond)
	if err != nil {
		t.Skipf("skipping: Ollama not reachable at %s: %v", baseURL, err)
		return
	}
	conn.Close()
}
