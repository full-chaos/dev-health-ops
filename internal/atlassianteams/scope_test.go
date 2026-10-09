package atlassianteams

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// integrationCensus is a scope census over a fixed set of the organization's
// Jira integrations: id -> is active. It counts the ACTIVE ones other than the
// run's own, as the worker's census reads public.integrations.is_active.
type integrationCensus map[string]bool

func (census integrationCensus) CountActiveSiblingIntegrations(_ context.Context, _, _, integrationID string) (int, error) {
	siblings := 0
	for id, active := range census {
		if active && id != integrationID {
			siblings++
		}
	}
	return siblings, nil
}

// scopeOf is the scope gate's answer for a run of integration id in an
// organization that holds the integrations of census.
func scopeOf(census integrationCensus, id string) providersync.ScopeProof {
	return providersync.ProveSoleScope(context.Background(), census, "org-1", Provider, id)
}

// soleScope is the gate's answer for the only Jira integration of the organization.
func soleScope() providersync.ScopeProof {
	return scopeOf(integrationCensus{"integration-a": true}, "integration-a")
}
