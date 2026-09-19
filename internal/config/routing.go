package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// BackendTargetConfig is one entry in an alias/tier cascade, as read from YAML.
type BackendTargetConfig struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
}

// AliasTiers maps tier name ("free", "standard", "premium") to its ordered cascade.
type AliasTiers struct {
	Tiers map[string][]BackendTargetConfig `yaml:"tiers"`
}

// Provider holds connection info for one backend provider.
type Provider struct {
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
}

// Routing is the parsed content of config/routing.yaml.
type Routing struct {
	Aliases   map[string]AliasTiers `yaml:"aliases"`
	Providers map[string]Provider   `yaml:"providers"`
}

// LoadRouting reads and parses a routing YAML file from disk.
func LoadRouting(path string) (*Routing, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading routing file %s: %w", path, err)
	}
	var r Routing
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("config: parsing routing file %s: %w", path, err)
	}
	return &r, nil
}
