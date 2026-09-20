package router

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Mouraovicente/ai-gateway/internal/config"
	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// ErrUnknownModel is returned when the requested alias cannot be resolved
// (unknown alias, or unknown tier / empty cascade within a known alias).
var ErrUnknownModel = errors.New("router: unknown model or alias")

// ErrDirectTargetForbidden is returned when a direct "provider/model" name
// is requested outside the "premium" tier.
var ErrDirectTargetForbidden = errors.New("router: direct provider/model target only allowed in premium tier")

// ErrUnknownProvider is returned when a target (from an alias cascade or a
// direct "provider/model" name) references a provider that has no entry in
// config.Routing.Providers. This is a config/input error, not a backend
// outage, so it must not be confused with resilience's runtime failures.
type ErrUnknownProvider struct {
	Provider string
	Alias    string // set when the provider came from an alias cascade; "" for a direct target
}

func (e *ErrUnknownProvider) Error() string {
	if e.Alias != "" {
		return fmt.Sprintf("router: alias %q references unknown provider %q", e.Alias, e.Provider)
	}
	return fmt.Sprintf("router: unknown provider %q", e.Provider)
}

func (e *ErrUnknownProvider) Is(target error) bool {
	_, ok := target.(*ErrUnknownProvider)
	return ok
}

// Resolve converts a "model" field from the request (a gateway alias like
// "nuva/fast", or a direct "provider/model" name) plus the caller's tier
// into an ordered cascade of backend targets to try in order.
func Resolve(routing *config.Routing, model, tier string) ([]core.BackendTarget, error) {
	// Charset check first, for aliases and direct targets alike: a name
	// that cannot be a legitimate model is never worth resolving.
	if !ValidModel(model) {
		return nil, ErrUnknownModel
	}
	if alias, ok := routing.Aliases[model]; ok {
		cascade, ok := alias.Tiers[tier]
		if !ok || len(cascade) == 0 {
			return nil, ErrUnknownModel
		}
		return toTargets(routing, cascade, model)
	}

	// Not a known alias. It might be a direct "provider/model" name.
	provider, backendModel, ok := strings.Cut(model, "/")
	if !ok || provider == "" || backendModel == "" {
		return nil, ErrUnknownModel
	}

	if _, known := routing.Providers[provider]; !known {
		// Outside premium, we can't tell a mistyped alias from a mistyped
		// direct target, so treat it uniformly as an unknown model/alias.
		// In premium, direct-target format is explicitly allowed, so an
		// unregistered provider is unambiguously a config error.
		if tier != "premium" {
			return nil, ErrUnknownModel
		}
		return nil, &ErrUnknownProvider{Provider: provider}
	}

	// Registered provider named directly: only allowed for tier "premium".
	if tier != "premium" {
		return nil, ErrDirectTargetForbidden
	}
	return []core.BackendTarget{{Provider: provider, Model: backendModel}}, nil
}

func toTargets(routing *config.Routing, cascade []config.BackendTargetConfig, alias string) ([]core.BackendTarget, error) {
	targets := make([]core.BackendTarget, 0, len(cascade))
	for _, c := range cascade {
		if _, known := routing.Providers[c.Provider]; !known {
			return nil, &ErrUnknownProvider{Provider: c.Provider, Alias: alias}
		}
		targets = append(targets, core.BackendTarget{Provider: c.Provider, Model: c.Model})
	}
	return targets, nil
}
