package daily

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

type recordingOrgActivity struct {
	refreshed           []string
	fills               int
	refreshErr, fillErr error
	// block makes Refresh wait for its context to end (a ClickHouse that does not answer).
	block bool
	// panics makes Refresh panic.
	panics bool
}

func (r *recordingOrgActivity) Refresh(ctx context.Context, orgID uuid.UUID) error {
	r.refreshed = append(r.refreshed, orgID.String())
	if r.panics {
		panic("refresh panicked")
	}
	if r.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.refreshErr
}

func (r *recordingOrgActivity) RefreshMissing(_ context.Context, limit int) (int, int, error) {
	if limit != 25 {
		return 0, 0, errors.New("unexpected batch")
	}
	r.fills++
	return 0, 0, r.fillErr
}

// The end of a finalize that completed refreshes the organization's activity
// projection once and fills the organizations that have no row; a finalize that
// failed or whose completion write failed refreshes nothing, and a failing
// refresh never fails the finalize.
func TestACompletedFinalizeRefreshesTheOrgActivityProjection(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	cases := []struct {
		name                 string
		familyErr            error
		attempt, maxAttempts int
		completionErr        error
		refreshErr, fillErr  error
		wantRefreshed        []string
		wantFills            int
		wantWorkErr          bool
	}{
		{name: "the run completed", attempt: 1, maxAttempts: 4, wantRefreshed: []string{testOrgID}, wantFills: 1},
		{name: "the refresh failed: the finalize still succeeds", attempt: 1, maxAttempts: 4, refreshErr: errors.New("boom"), wantRefreshed: []string{testOrgID}, wantFills: 1},
		{name: "the fill failed: the finalize still succeeds", attempt: 1, maxAttempts: 4, fillErr: errors.New("boom"), wantRefreshed: []string{testOrgID}, wantFills: 1},
		{name: "a family failed", familyErr: errors.New("boom"), attempt: 4, maxAttempts: 4},
		{name: "the completion write failed", attempt: 1, maxAttempts: 4, completionErr: ErrUnavailable},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := finalizeStoreWithClaim()
			store.completionErr = test.completionErr
			handler, err := NewFinalizeHandler(store)
			if err != nil {
				t.Fatal(err)
			}
			if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
				"ic_finalize": &stubFinalizeFamily{err: test.familyErr},
			}); err != nil {
				t.Fatal(err)
			}
			refresher := &recordingOrgActivity{refreshErr: test.refreshErr, fillErr: test.fillErr}
			handler.SetOrgActivityRefresher(refresher)
			execution := finalizeExecutionFor(testRunID)
			execution.Attempt = test.attempt
			execution.Definition.MaxAttempts = test.maxAttempts
			workErr := handler.Work(context.Background(), execution)
			if !reflect.DeepEqual(refresher.refreshed, test.wantRefreshed) || refresher.fills != test.wantFills {
				t.Fatalf("refreshed %v fills %d, want %v and %d (work error %v)", refresher.refreshed, refresher.fills, test.wantRefreshed, test.wantFills, workErr)
			}
			if test.wantRefreshed != nil && workErr != nil {
				t.Fatalf("a failing refresh failed the finalize: %v", workErr)
			}
		})
	}
}

// A refresh that never answers, or panics, cannot fail or delay the finalize: it is cut by its own timeout, the
// completion is already written, and the drain pass has already started.
func TestAHungOrPanickingOrgActivityRefreshNeitherFailsNorDelaysTheFinalize(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	for name, refresher := range map[string]*recordingOrgActivity{
		"hung":     {block: true},
		"panicked": {panics: true},
	} {
		t.Run(name, func(t *testing.T) {
			store := finalizeStoreWithClaim()
			handler, err := NewFinalizeHandler(store)
			if err != nil {
				t.Fatal(err)
			}
			if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": &stubFinalizeFamily{}}); err != nil {
				t.Fatal(err)
			}
			drainer := &recordingTouchedDrainer{}
			handler.SetTouchedDaysDrainer(drainer)
			handler.SetOrgActivityRefresher(refresher)
			handler.orgActivityTimeoutOverride = 200 * time.Millisecond
			execution := finalizeExecutionFor(testRunID)
			execution.Attempt, execution.Definition.MaxAttempts = 1, 4
			done := make(chan error, 1)
			go func() { done <- handler.Work(context.Background(), execution) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Work = %v, want nil: the finalize completed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the finalize is still waiting for the refresh after 5s")
			}
			if len(drainer.passes) != 1 {
				t.Fatalf("drain passes = %v, want the pass that follows the end of the run", drainer.passes)
			}
			if len(refresher.refreshed) != 1 {
				t.Fatalf("refresh calls = %v, want 1", refresher.refreshed)
			}
		})
	}
}
