package daily

import (
	"context"
	"testing"
)

// The finalize of a run of the testops window lane (a tw- generation) computes nothing: the run computes the two families that read
// the trailing window, and the retraction of stale keys recomputes the work-item tables from the stored items (it writes their
// rows), which is a measure the run does not own. The run is still completed and its end still triggers the drain.
func TestTheFinalizeOfATestopsWindowRunRunsNoRetractionAndNoFamily(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	for _, generation := range []string{"tw-sync:00000000-0000-4000-8000-0000000000b1", "tw-drain:e:00000000-0000-4000-8000-0000000000b2"} {
		var order []string
		store := staleKeyRetractionStore()
		store.finalizeClaim.Run.Generation = generation
		store.run.Generation = generation
		retractor := &recordingRetractor{order: &order}
		handler := staleKeyRetractionFinalize(t, store, retractor, &order)
		drain := &recordingTouchedDrainer{}
		handler.SetTouchedDaysDrainer(drain)
		if err := handler.Work(context.Background(), finalizeExecution()); err != nil {
			t.Fatal(err)
		}
		if len(order) != 0 {
			t.Errorf("%s: the finalize ran %v, want nothing", generation, order)
		}
		if store.finalizeCompletions != 1 {
			t.Errorf("%s: completions = %d, want 1", generation, store.finalizeCompletions)
		}
		if len(drain.passes) != 1 {
			t.Errorf("%s: drain passes = %v, want 1 (the end of the run)", generation, drain.passes)
		}
	}
}
