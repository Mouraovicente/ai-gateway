package api

// ModelInfo is one entry in GET /v1/models' data array, OpenAI-compatible shape.
type ModelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

// BuildModelsList assembles the /v1/models response body: every configured
// alias (routing.yaml's aliases, tenant-agnostic — no auth required to list
// them), plus "ollama/<tag>" for every model Ollama currently has pulled.
// ollamaTags may be nil (Ollama unreachable): the alias list alone is still
// a valid response. Paid providers are never called here.
func BuildModelsList(aliases []string, ollamaTags []string) []ModelInfo {
	data := make([]ModelInfo, 0, len(aliases)+len(ollamaTags))
	for _, alias := range aliases {
		data = append(data, ModelInfo{ID: alias, Object: "model", OwnedBy: "ai-gateway"})
	}
	for _, tag := range ollamaTags {
		data = append(data, ModelInfo{ID: "ollama/" + tag, Object: "model", OwnedBy: "ai-gateway"})
	}
	return data
}
