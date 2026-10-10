package daily

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// recordingRetractor records the runs it is called with and the order of the
// calls against a finalize family.
type recordingRetractor struct {
	runs  []Run
	order *[]string
	err   error
}

func (retractor *recordingRetractor) RetractStaleKeys(_ context.Context, run Run) (int, error) {
	retractor.runs = append(retractor.runs, run)
	*retractor.order = append(*retractor.order, "retraction")
	return 0, retractor.err
}

type orderedFinalizeFamily struct{ order *[]string }

func (family orderedFinalizeFamily) ComputeFinalizeFamily(context.Context, Run) (int, error) {
	*family.order = append(*family.order, "family")
	return 0, nil
}

func staleKeyRetractionFinalize(t *testing.T, store *fakeStore, retractor StaleKeyRetractor, order *[]string) *FinalizeHandler {
	t.Helper()
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": orderedFinalizeFamily{order: order},
	}); err != nil {
		t.Fatal(err)
	}
	if retractor != nil {
		handler.SetStaleKeyRetractor(retractor)
	}
	return handler
}

func staleKeyRetractionStore() *fakeStore {
	return &fakeStore{
		// The claim of a finalize carries no repository list; the stored run
		// does.
		finalizeClaim: &FinalizeClaim{
			Run:           Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
			Token:         "00000000-0000-4000-8000-000000000004",
			LeaseDuration: time.Second,
		},
		run: Run{
			ID: testRunID, OrganizationID: testOrgID,
			DiscoveredRepoIDs: []RepositoryID{"00000000-0000-4000-8000-0000000000a1", "00000000-0000-4000-8000-0000000000a2"},
		},
	}
}

// The retraction of the stale team keys runs once for a run, before the
// finalize families, with the repositories of every partition of the run.
func TestTheFinalizeRetractsStaleKeysOnceBeforeTheFamilies(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	var order []string
	store := staleKeyRetractionStore()
	retractor := &recordingRetractor{order: &order}
	handler := staleKeyRetractionFinalize(t, store, retractor, &order)
	if !handler.HasStaleKeyRetractor() {
		t.Fatal("the handler reports no retractor")
	}
	if err := handler.Work(context.Background(), finalizeExecution()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"retraction", "family"}) {
		t.Fatalf("order = %v, want the retraction once and then the family", order)
	}
	if len(retractor.runs) != 1 || retractor.runs[0].ID != testRunID || retractor.runs[0].OrganizationID != testOrgID ||
		!reflect.DeepEqual(retractor.runs[0].DiscoveredRepoIDs, store.run.DiscoveredRepoIDs) {
		t.Fatalf("the retraction got %+v, want the claimed run with the repositories of its partitions", retractor.runs)
	}
	if store.finalizeCompletions != 1 {
		t.Fatalf("completions = %d, want 1", store.finalizeCompletions)
	}
}

// A failed retraction fails the finalize, which is tried again. No family runs
// on the tables that still hold the stale keys, and the run is not completed.
func TestAFailedRetractionFailsTheFinalizeAndRunsNoFamily(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	failure := errors.New("clickhouse: connection reset")
	var order []string
	store := staleKeyRetractionStore()
	handler := staleKeyRetractionFinalize(t, store, &recordingRetractor{order: &order, err: failure}, &order)
	err := handler.Work(context.Background(), finalizeExecution())
	if err == nil || !errors.Is(err, failure) || !errors.Is(err, ErrNativeFinalizeFamilyFailed) {
		t.Fatalf("Work = %v, want the failure of the retraction as a failed finalize", err)
	}
	if !reflect.DeepEqual(order, []string{"retraction"}) || store.finalizeCompletions != 0 {
		t.Fatalf("order = %v, completions = %d; want the retraction only and no completion", order, store.finalizeCompletions)
	}
}

// The repositories of the run are read from the store. A failed read fails the
// finalize: a retraction with no repository would supersede nothing and the
// run would end as complete.
func TestAFailedReadOfTheRunsRepositoriesFailsTheFinalize(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	var order []string
	store := staleKeyRetractionStore()
	store.loadErr = ErrUnavailable
	handler := staleKeyRetractionFinalize(t, store, &recordingRetractor{order: &order}, &order)
	err := handler.Work(context.Background(), finalizeExecution())
	if err == nil || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Work = %v, want the failed read of the run", err)
	}
	if len(order) != 0 || store.finalizeCompletions != 0 {
		t.Fatalf("order = %v, completions = %d; want no retraction, no family and no completion", order, store.finalizeCompletions)
	}
}

// A handler with no retractor skips the step and runs its families: the unit
// tests of the handler build it that way. The worker attaches one
// (workerservice, newDrainingDailyFinalizeHandler).
func TestAFinalizeWithNoRetractorRunsItsFamilies(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	var order []string
	store := staleKeyRetractionStore()
	handler := staleKeyRetractionFinalize(t, store, nil, &order)
	if handler.HasStaleKeyRetractor() {
		t.Fatal("the handler reports a retractor")
	}
	if err := handler.Work(context.Background(), finalizeExecution()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"family"}) {
		t.Fatalf("order = %v, want the family only", order)
	}
}

func TestRunStaleKeyRetractorRefusesWhatItCannotRun(t *testing.T) {
	if _, err := NewRunStaleKeyRetractor(nil); !errors.Is(err, errRunStaleKeyRetractorUnavailable) {
		t.Fatalf("a retractor with no connection was built: %v", err)
	}
	var none *RunStaleKeyRetractor
	if _, err := none.RetractStaleKeys(context.Background(), Run{}); !errors.Is(err, errRunStaleKeyRetractorUnavailable) {
		t.Fatalf("a nil retractor ran: %v", err)
	}
	retractor := &RunStaleKeyRetractor{conn: &governanceQueryRecorder{}, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	for name, run := range map[string]Run{
		"no organization": {TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)},
		"no target day":   {OrganizationID: "org"},
	} {
		if _, err := retractor.RetractStaleKeys(context.Background(), run); !errors.Is(err, ErrInvalidState) {
			t.Errorf("%s: err = %v, want ErrInvalidState", name, err)
		}
	}
}
