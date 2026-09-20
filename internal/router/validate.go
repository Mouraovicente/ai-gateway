package router

import (
	"regexp"
	"strings"
)

// modelPattern is the charset a "model" field may use. It is a trust
// boundary: the premium tier lets the caller name a backend model directly,
// and that string ends up inside a provider URL path (Gemini) as well as in
// request bodies and trace payloads. Everything outside this set —
// traversal ("../"), query/fragment injection ("?", "#"), NUL, control
// characters, non-ASCII — is rejected here, once, before any adapter sees
// it.
var modelPattern = regexp.MustCompile(`^[A-Za-z0-9._:/-]{1,128}$`)

// ValidModel reports whether model is an acceptable alias or
// "provider/model" target. "." and "/" are legal inside model names
// ("gemini-1.5-pro", "provider/model"), so traversal has to be rejected
// per segment rather than by charset alone.
func ValidModel(model string) bool {
	if !modelPattern.MatchString(model) {
		return false
	}
	for _, segment := range strings.Split(model, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}
