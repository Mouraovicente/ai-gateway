package router

import (
	"errors"
	"strings"

	"github.com/Mouraovicente/ai-gateway/internal/config"
	"github.com/Mouraovicente/ai-gateway/internal/core"
)

// ErrUnknownModel is returned when the requested alias (or direct backend
// name) cannot be resolved to a cascade for the given tier.
var ErrUnknownModel = errors.New("router: unknown model or alias")

// Resolve converts a "model" field from the request (a gateway alias like
// "nuva/fast", or a direct "provider/model" name) plus the caller's tier
// into an ordered cascade of backend targets to try in order.
func Resolve(routing *config.Routing, model, tier string) ([]core.BackendTarget, error) {
	if alias, ok := routing.Aliases[model]; ok {
		cascade, ok := alias.Tiers[tier]
		if !ok || len(cascade) == 0 {
			return nil, ErrUnknownModel
		}
		return toTargets(cascade), nil
	}

	// Direct backend name ("provider/model"), only allowed for tier "premium".
	if tier != "premium" {
		return nil, ErrUnknownModel
	}
	provider, backendModel, ok := strings.Cut(model, "/")
	if !ok || provider == "" || backendModel == "" {
		return nil, ErrUnknownModel
	}
	return []core.BackendTarget{{Provider: provider, Model: backendModel}}, nil
}

func toTargets(cascade []config.BackendTargetConfig) []core.BackendTarget {
	targets := make([]core.BackendTarget, 0, len(cascade))
	for _, c := range cascade {
		targets = append(targets, core.BackendTarget{Provider: c.Provider, Model: c.Model})
	}
	return targets
}
