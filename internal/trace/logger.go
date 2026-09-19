package trace

import (
	"io"
	"log/slog"
	"strings"
)

// ForbiddenKeys is the tested list of attribute keys the logger must never
// emit, because they could contain prompt/completion content or secrets.
var ForbiddenKeys = []string{"message", "messages", "answer", "trace", "content", "payload", "prompt", "completion"}

// isForbidden reports whether key matches a forbidden key, case-insensitively.
func isForbidden(key string) bool {
	for _, k := range ForbiddenKeys {
		if strings.EqualFold(k, key) {
			return true
		}
	}
	return false
}

// NewLogger returns a JSON slog.Logger that drops any attribute whose key is
// in ForbiddenKeys, so prompts/completions/secrets never reach stdout logs.
func NewLogger(w io.Writer) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if isForbidden(a.Key) {
				return slog.Attr{}
			}
			return a
		},
	})
	return slog.New(handler)
}
