package api

import "strings"

// minAPIKeyLen / maxAPIKeyLen bound what is even worth hashing: anything
// outside this range cannot be a key this gateway ever issued.
const (
	minAPIKeyLen = 8
	maxAPIKeyLen = 256
)

// ParseBearer extracts the API key from an Authorization header value. It
// requires the scheme explicitly (case-insensitive, exactly one space), so
// "sk-key" with no scheme is rejected instead of silently accepted — an
// ambiguity that would break every such caller the day it was tightened.
// The provided key is never echoed anywhere, so a caller learns only that
// the header was rejected.
func ParseBearer(header string) (string, bool) {
	if len(header) < len("Bearer ") || !strings.EqualFold(header[:7], "bearer ") {
		return "", false
	}
	key := header[7:]
	if key == "" || strings.HasPrefix(key, " ") || len(key) < minAPIKeyLen || len(key) > maxAPIKeyLen {
		return "", false
	}
	return key, true
}
