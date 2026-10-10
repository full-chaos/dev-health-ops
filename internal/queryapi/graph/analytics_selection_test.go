package graph

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

// A call that is not the resolution of a GraphQL field has no selection to
// read. Everything is then selected: a part is left out only on the positive
// knowledge that the operation did not ask for it.
func TestAnAnalyticsCallWithNoSelectionToReadSelectsEverything(t *testing.T) {
	if got := analyticsSelection(context.Background()); got != analytics.EverythingSelected() {
		t.Fatalf("selection with no field on the context = %+v, want everything selected", got)
	}
	everything := analytics.EverythingSelected()
	if !everything.Breakdowns || !everything.EvidenceQualityStats || !everything.EvidenceQualityByGroup || !everything.SankeyCoverage {
		t.Fatalf("EverythingSelected leaves a part out: %+v", everything)
	}
	if (analytics.Selection{}) == everything {
		t.Fatal("the zero selection selects nothing")
	}
}
