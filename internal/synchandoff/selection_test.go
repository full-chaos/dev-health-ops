package synchandoff

import "testing"

func TestSelectionAppliesToAPlannerManagedParentOtherThanPagerDuty(t *testing.T) {
	integration, source := "00000000-0000-4000-8000-0000000000aa", "00000000-0000-4000-8000-0000000000bb"
	for name, c := range map[string]struct {
		config Config
		want   bool
	}{
		"github parent":           {Config{Provider: "github", PlannerManaged: true, IntegrationID: &integration}, true},
		"gitlab parent":           {Config{Provider: "gitlab", PlannerManaged: true, IntegrationID: &integration}, true},
		"pagerduty parent":        {Config{Provider: "pagerduty", PlannerManaged: true, IntegrationID: &integration}, false},
		"pagerduty, other casing": {Config{Provider: "PagerDuty", PlannerManaged: true, IntegrationID: &integration}, false},
		"padded pagerduty":        {Config{Provider: " PagerDuty ", PlannerManaged: true, IntegrationID: &integration}, true},
		"pinned to one source":    {Config{Provider: "github", PlannerManaged: true, IntegrationID: &integration, SourceID: &source}, false},
		"not planner-managed":     {Config{Provider: "github", IntegrationID: &integration}, false},
		"no integration":          {Config{Provider: "github", PlannerManaged: true}, false},
	} {
		if got := SelectionApplies(&c.config); got != c.want {
			t.Errorf("%s: SelectionApplies = %v, want %v", name, got, c.want)
		}
	}
}
