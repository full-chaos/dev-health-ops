package syncadmin

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

const skippedDatasetsResult = `{"skipped_datasets":[{"dataset_key":"incidents","reason":"feature_disabled","message":"skipped: feature off"}]}`

// A run planned with a dataset left out (feature off) says so: the sync-run
// response serves the per-dataset reason from the stored result.
func TestSyncRunResponseServesTheSkippedDatasetsWithTheirReason(t *testing.T) {
	result := skippedDatasetsResult
	body, err := syncRunResponse(&syncRun{ID: uuid.Nil, Status: "planned", Result: &result})
	if err != nil {
		t.Fatal(err)
	}
	text := marshal(t, body)
	for _, want := range []string{`"skipped_datasets":[`, `"dataset_key":"incidents"`, `"reason":"feature_disabled"`, `"message":"skipped: feature off"`} {
		if !strings.Contains(text, want) {
			t.Errorf("the response lacks %s: %s", want, text)
		}
	}
}

// The job-run response of a planner-managed run merges the sync run's result,
// so the same reason shows on the sync status of the job run.
func TestJobRunResponseServesTheSkippedDatasetsOfThePlannerRun(t *testing.T) {
	result := skippedDatasetsResult
	planner := &syncRun{ID: uuid.Nil, Mode: "m", TriggeredBy: "t", Status: "planned", TotalUnits: 2, Result: &result}
	body, err := jobRunResponse(&jobRun{ID: uuid.Nil, JobID: uuid.Nil, TriggeredBy: "t"}, decode(t, `{"sync_run_id":"x"}`), planner, nil)
	if err != nil {
		t.Fatal(err)
	}
	text := marshal(t, body)
	for _, want := range []string{`"skipped_datasets":[`, `"dataset_key":"incidents"`, `"reason":"feature_disabled"`} {
		if !strings.Contains(text, want) {
			t.Errorf("the response lacks %s: %s", want, text)
		}
	}
}
