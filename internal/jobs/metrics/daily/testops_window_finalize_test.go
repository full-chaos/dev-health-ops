package daily

import (
	"context"
	"errors"
	"slices"
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

// recordingNativeFamily records the families a partition pass computed.
type recordingNativeFamily struct {
	name     string
	computed *[]string
}

func (family recordingNativeFamily) ComputeFamily(context.Context, Run, Partition) (int, error) {
	*family.computed = append(*family.computed, family.name)
	return 1, nil
}

type windowZeroRowsObserver struct{ families []string }

func (observer *windowZeroRowsObserver) ObserveDailyMetricsFamilyZeroRowsWithSource(family string) error {
	observer.families = append(observer.families, family)
	return nil
}

// A partition of a testops window run computes the two families that read the trailing window and no other, in either phase.
func TestAPartitionOfATestopsWindowRunComputesOnlyTheWindowFamilies(t *testing.T) {
	for _, generation := range []string{"daily-v1", "tw-sync:00000000-0000-4000-8000-0000000000b1"} {
		var computed []string
		store := terminalPartitionStore()
		store.run.Generation = generation
		handler := terminalPartitionHandler(t, store)
		pre := map[string]NativeFamilyExecutor{}
		for _, name := range []string{"cicd", "testops_pipeline", "testops_test", "testops_risk"} {
			pre[name] = recordingNativeFamily{name: name, computed: &computed}
		}
		if err := handler.SetNativeFamilies(pre); err != nil {
			t.Fatal(err)
		}
		if err := handler.SetPostBridgeNativeFamilies(map[string]NativeFamilyExecutor{
			"compounding_risk": recordingNativeFamily{name: "compounding_risk", computed: &computed},
		}); err != nil {
			t.Fatal(err)
		}
		if err := handler.Work(context.Background(), executionAtAttempt(1, 5)); err != nil {
			t.Fatalf("%s: Work = %v", generation, err)
		}
		slices.Sort(computed)
		want := []string{"cicd", "compounding_risk", "testops_pipeline", "testops_risk", "testops_test"}
		if generation != "daily-v1" {
			want = []string{"testops_risk", "testops_test"}
		}
		if !slices.Equal(computed, want) || store.partitionCompletions != 1 {
			t.Errorf("%s: computed %v (completions %d), want %v and one completion", generation, computed, store.partitionCompletions, want)
		}
	}
}

// The zero-row check of a testops window run judges only the families the run computed: a day with CI source and no cicd row
// (today before the nightly run) must not fail the partition for good.
func TestTheZeroRowCheckOfATestopsWindowRunJudgesOnlyItsFamilies(t *testing.T) {
	for _, flagged := range [][]string{{"cicd", "deploy", "incident"}, {"cicd", "testops_risk"}} {
		store := terminalPartitionStore()
		store.run.Generation = "tw-drain:e:00000000-0000-4000-8000-0000000000b2"
		handler := terminalPartitionHandler(t, store)
		handler.SetSourceDataChecker(familiesSourceDataChecker{families: flagged})
		observer := &windowZeroRowsObserver{}
		handler.SetZeroRowsObserver(observer)
		err := handler.Work(context.Background(), executionAtAttempt(1, 5))
		if !slices.Contains(flagged, "testops_risk") {
			if err != nil || store.partitionCompletions != 1 || store.permanentFailures != 0 || len(observer.families) != 0 {
				t.Errorf("flagged %v: Work = %v, completions %d, permanent %d, observed %v; want a completed partition",
					flagged, err, store.partitionCompletions, store.permanentFailures, observer.families)
			}
			continue
		}
		if !errors.Is(err, ErrZeroRowsWithSourceData) || !slices.Equal(observer.families, []string{"testops_risk"}) {
			t.Errorf("flagged %v: Work = %v, observed %v; want the zero-row failure of testops_risk only", flagged, err, observer.families)
		}
	}
}
