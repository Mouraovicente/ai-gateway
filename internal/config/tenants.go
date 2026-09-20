package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// DevTenant is one entry of config/tenants.dev.yaml: a plaintext API key
// plus the limits that key gets. Only ever used by STORE_BACKEND=memory.
type DevTenant struct {
	APIKey             string `yaml:"api_key"`
	TenantID           string `yaml:"tenant_id"`
	Tier               string `yaml:"tier"`
	RPMLimit           int    `yaml:"rpm_limit"`
	MonthlyTokenBudget int    `yaml:"monthly_token_budget"`
}

type devTenantsFile struct {
	Tenants []DevTenant `yaml:"tenants"`
}

// LoadDevTenants reads the dev tenant file and returns it keyed by
// plaintext API key, the shape memstore.NewAuthStore wants. Every entry is
// validated the same way the DynamoDB path validates a tenant item, so a
// half-written dev file fails at boot instead of turning into silent 429s.
func LoadDevTenants(path string) (map[string]core.Tenant, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading dev tenants file %s: %w", path, err)
	}
	var f devTenantsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("config: parsing dev tenants file %s: %w", path, err)
	}
	if len(f.Tenants) == 0 {
		return nil, fmt.Errorf("config: dev tenants file %s has no tenants", path)
	}
	out := make(map[string]core.Tenant, len(f.Tenants))
	for _, t := range f.Tenants {
		if t.APIKey == "" || t.TenantID == "" || t.Tier == "" {
			return nil, fmt.Errorf("config: dev tenant %q is missing api_key/tenant_id/tier", t.TenantID)
		}
		if t.RPMLimit <= 0 || t.MonthlyTokenBudget <= 0 {
			return nil, fmt.Errorf("config: dev tenant %q needs positive rpm_limit and monthly_token_budget", t.TenantID)
		}
		out[t.APIKey] = core.Tenant{
			ID:                 t.TenantID,
			Tier:               t.Tier,
			RPMLimit:           t.RPMLimit,
			MonthlyTokenBudget: t.MonthlyTokenBudget,
		}
	}
	return out, nil
}
