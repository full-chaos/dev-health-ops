package sync

import (
	"testing"
	"time"
)

// The planner and the admin freshness view decide "this family skips the
// ratchet" through DatasetFullQuery and PlannedWindowClass. For the same
// dataset row, the window the planner plans and the class the view judges
// "catching up" with must agree: one window iff not catching up.
func TestPlannerAndFreshnessAgreeOnAFullQueryRow(t *testing.T) {
	for name, tc := range map[string]struct {
		provider string
		enabled  bool
		raw      string
		full     bool
	}{
		"fetch_all":        {"jira", true, `{"fetch_all": true}`, true},
		"jql_override":     {"jira", true, `{"jql_override": "project = X"}`, true},
		"empty":            {"jira", true, `{}`, false},
		"unreadable":       {"jira", true, `nope`, false},
		"disabled":         {"jira", false, `{"fetch_all": true}`, false},
		"github fetch_all": {"github", true, `{"fetch_all": true}`, false},
		"jira mixed-case":  {"Jira", true, `{"fetch_all": true}`, true},
	} {
		t.Run(name, func(t *testing.T) {
			full := DatasetFullQuery(tc.provider, canonicalWorkItemsDataset, tc.enabled, []byte(tc.raw))
			if full != tc.full {
				t.Fatalf("DatasetFullQuery = %v, want %v", full, tc.full)
			}
			datasets := workItemFamilyPlanDatasets()
			for index := range datasets {
				if datasets[index].Key == canonicalWorkItemsDataset {
					datasets[index].FullQuery = full
				}
			}
			depth := 90
			units, err := BuildScheduledPlan(PlannerInput{
				OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: workItemWindowNow,
				IntegrationDepthDays: &depth,
				Sources:              []PlanSource{{ID: "source", ExternalID: "external", Provider: tc.provider}},
				Datasets:             datasets,
			})
			if err != nil {
				t.Fatal(err)
			}
			unit := onlyWorkItemsUnit(t, tc.provider, units)
			oneWindow := unit.WindowEnd.Sub(*unit.WindowStart) == 90*24*time.Hour

			capDays, netAdvance := HeavyRatchet()
			behind := workItemWindowNow.AddDate(0, 0, -30)
			class := PlannedWindowClass(tc.provider, canonicalWorkItemsDataset, "medium", full)
			catchingUp := ComputeWatermarkLag(class, &behind, workItemWindowNow, capDays, netAdvance).CatchingUp
			if oneWindow == catchingUp {
				t.Fatalf("planned one window = %v but freshness catching up = %v", oneWindow, catchingUp)
			}
		})
	}
}
