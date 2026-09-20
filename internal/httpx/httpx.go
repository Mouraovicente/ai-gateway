// Package httpx holds the HTTP client policy every backend adapter shares:
// one process-wide *http.Transport (so connection pooling and TLS sessions
// are reused across providers instead of each adapter paying a fresh
// handshake) plus the timeout constants the adapters apply per call.
package httpx

import (
	"io"
	"net/http"
	"time"
)

// NonStreamTimeout is the total wall-clock budget for one non-streaming
// backend attempt, applied by the adapters as a context deadline (not as
// http.Client.Timeout, which would also cut legitimate long streams).
const NonStreamTimeout = 120 * time.Second

// MaxResponseBytes bounds any fully-buffered upstream body (non-streaming
// responses and every error body). Upstream output is untrusted input: a
// provider — or a misconfigured base URL — answering with gigabytes must not
// be able to OOM the gateway.
const MaxResponseBytes = 8 << 20

// MaxStreamLineBytes bounds one line of a streaming response (SSE event or
// NDJSON object). Well above any real chunk, low enough that a hostile
// upstream cannot stream one unbounded "line" into memory.
const MaxStreamLineBytes = 1 << 20

// sharedTransport is the single Transport behind every backend client.
// ResponseHeaderTimeout is the TTFT guard the spec asks for: it fires when
// an upstream accepts the connection but never sends response headers, and
// it applies to the streaming path too (where no total Timeout can).
var sharedTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	MaxIdleConns:          200,
	MaxIdleConnsPerHost:   100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	ResponseHeaderTimeout: 30 * time.Second,
	ForceAttemptHTTP2:     true,
}

// NewClient returns a client on the shared transport. It deliberately has no
// Timeout: the per-call budget comes from the request context, so streaming
// calls are not cut mid-stream.
func NewClient() *http.Client {
	return &http.Client{Transport: sharedTransport}
}

// ReadAllLimited reads at most MaxResponseBytes from r.
func ReadAllLimited(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, MaxResponseBytes))
}
