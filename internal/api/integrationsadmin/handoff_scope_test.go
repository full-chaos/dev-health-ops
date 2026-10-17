package integrationsadmin

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

const (
	scopeSelectedA = "00000000-0000-4000-8000-00000000000a"
	scopeSelectedB = "00000000-0000-4000-8000-00000000000b"
	scopeUntagged  = "00000000-0000-4000-8000-00000000000c"
	scopeDisabled  = "00000000-0000-4000-8000-00000000000d"
)

func scopeTarget(plannerManaged bool, pinned *string) handoffScope {
	return handoffScope{
		config:  &synchandoff.Config{ID: "cfg", PlannerManaged: plannerManaged, SourceID: pinned},
		sources: []string{scopeSelectedA, scopeSelectedB},
		enabled: []string{scopeSelectedA, scopeSelectedB, scopeUntagged},
	}
}

func TestRefuseUnselectedSourcesRefusesAnEnabledSourceTheConfigurationDoesNotSelect(t *testing.T) {
	err := refuseUnselectedSources(scopeTarget(true, nil), triggerScope{sourceIDs: []string{scopeSelectedA, strings.ToUpper(scopeUntagged)}, sourceIDsSet: true})
	var refused refusal
	if !errors.As(err, &refused) || refused.status != http.StatusBadRequest || !strings.Contains(refused.message, scopeUntagged) {
		t.Fatalf("err = %v, want a 400 naming the unselected source", err)
	}
}

func TestRefuseUnselectedSourcesLeavesTheRestToTheScheduler(t *testing.T) {
	pinned := scopeSelectedA
	for name, c := range map[string]struct {
		target handoffScope
		scope  triggerScope
	}{
		"no source ids":                    {scopeTarget(true, nil), triggerScope{}},
		"selected sources":                 {scopeTarget(true, nil), triggerScope{sourceIDs: []string{scopeSelectedB, "{" + scopeSelectedA + "}"}, sourceIDsSet: true}},
		"a disabled source (not planned)":  {scopeTarget(true, nil), triggerScope{sourceIDs: []string{scopeDisabled}, sourceIDsSet: true}},
		"an empty list":                    {scopeTarget(true, nil), triggerScope{sourceIDs: []string{}, sourceIDsSet: true}},
		"a configuration pinned to source": {scopeTarget(false, &pinned), triggerScope{sourceIDs: []string{scopeUntagged}, sourceIDsSet: true}},
		"not planner-managed":              {scopeTarget(false, nil), triggerScope{sourceIDs: []string{scopeUntagged}, sourceIDsSet: true}},
	} {
		if err := refuseUnselectedSources(c.target, c.scope); err != nil {
			t.Errorf("%s: err = %v, want none", name, err)
		}
	}
}
