package ollama

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

type tagsResponseBody struct {
	Models []struct {
		Name string `json:"name"`
	} `json:"models"`
}

// Tags returns the list of model names Ollama currently has pulled, used by
// GET /v1/models and by the /healthz warm-up check.
func (c *Client) Tags(ctx context.Context) ([]string, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/tags", nil)
	if err != nil {
		return nil, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: building tags request: %w", err)}
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: tags request failed: %w", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		class := core.Permanent
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			class = core.Transient
		}
		return nil, &core.BackendError{Class: class, Status: resp.StatusCode, Err: fmt.Errorf("ollama: tags returned %d", resp.StatusCode)}
	}

	var parsed tagsResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: parsing tags: %w", err)}
	}

	names := make([]string, 0, len(parsed.Models))
	for _, m := range parsed.Models {
		names = append(names, m.Name)
	}
	return names, nil
}
