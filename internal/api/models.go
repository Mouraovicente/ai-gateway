package api

// ModelInfo is one entry in GET /v1/models' data array, OpenAI-compatible shape.
type ModelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

// BuildModelsList assembles the /v1/models response body: the configured
// aliases from routing.yaml, and nothing else.
//
// The endpoint is unauthenticated (aliases are tenant-agnostic), so it
// deliberately stops at the alias layer: it no longer echoes the backend
// inventory (which provider, which model tags are pulled), because that is
// free reconnaissance about infrastructure for anyone who curls it. A
// caller that is entitled to a specific backend model already knows its
// name from the docs.
func BuildModelsList(aliases []string) []ModelInfo {
	data := make([]ModelInfo, 0, len(aliases))
	for _, alias := range aliases {
		data = append(data, ModelInfo{ID: alias, Object: "model", OwnedBy: "ai-gateway"})
	}
	return data
}
