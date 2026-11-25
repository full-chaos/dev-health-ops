package liverow

import "testing"

// CHAOS-14616: the gate of the pooled sources keeps a key whose newest computed_at holds any live row.
func TestNewestAnyLivePredicateKeepsAKeyWithAnyLiveRowAtTheNewestStamp(t *testing.T) {
	t.Parallel()
	for _, table := range []string{
		"repo_metrics_daily", "deploy_metrics_daily", "incident_metrics_daily",
		"testops_pipeline_metrics_daily", "testops_test_metrics_daily", "cicd_metrics_daily",
	} {
		for qualifier, prefix := range map[string]string{"": table + ".", "q": "q."} {
			want := "(tupleElement(max((" + prefix + "computed_at, 1 - " + prefix + "retracted)), 2) = 1)"
			if got := NewestAnyLivePredicate(table, qualifier); got != want {
				t.Errorf("NewestAnyLivePredicate(%q, %q) = %q, want %q", table, qualifier, got, want)
			}
		}
	}
}

func TestNewestAnyLivePredicatePanicsForATableWithNoRetractedColumn(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"work_item_state_durations_daily", "work_item_completion_daily", "no_such_daily"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("NewestAnyLivePredicate(%q) did not panic", table)
				}
			}()
			NewestAnyLivePredicate(table, "")
		}()
	}
}
