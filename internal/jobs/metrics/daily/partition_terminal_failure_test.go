package daily

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// A partition that ends for good must be written failed_permanent with a bounded
// reason. A 'failed' partition is re-claimed only by a live River job of that
// partition (ClaimPartition), and the dispatch key is per run while the
// partition key is permanent, so once the job is cancelled or discarded nothing
// ever runs it again: left 'failed' with no reason, the run stays 'running'
// with no signal. failed_permanent is the state the blocked-run marker, its
// counter and the daily-redrive reset already read.

func terminalPartitionStore() *fakeStore {
	return &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition: Partition{ID: testPartitionID, RunID: testRunID},
			Token:     "00000000-0000-4000-8000-000000000003", LeaseDuration: 30 * time.Millisecond,
		},
		run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: "daily-v1", Status: "running"},
	}
}

func terminalPartitionHandler(t *testing.T, store *fakeStore) *PartitionHandler {
	t.Helper()
	handler, err := NewPartitionHandler(store, fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func executionAtAttempt(attempt, maxAttempts int) *jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs] {
	execution := partitionExecution()
	execution.Attempt, execution.Definition.MaxAttempts = attempt, maxAttempts
	return execution
}

func requireTerminalPartition(t *testing.T, store *fakeStore, wantReason string) {
	t.Helper()
	if store.permanentFailures != 1 || store.permanentFailureReason != wantReason {
		t.Fatalf("FailPartitionPermanently calls=%d reason=%q, want 1/%q", store.permanentFailures, store.permanentFailureReason, wantReason)
	}
	if store.partitionReleases != 0 || store.releasesWithReason != 0 {
		t.Fatalf("a partition that ended for good was also released (%d plain, %d with reason): it would be left 'failed'",
			store.partitionReleases, store.releasesWithReason)
	}
}

// C1: the zero-row check cancels the partition for good.
func TestPartitionZeroRowsEndsFailedPermanentWithItsReason(t *testing.T) {
	store := terminalPartitionStore()
	handler := terminalPartitionHandler(t, store)
	handler.SetSourceDataChecker(familiesSourceDataChecker{families: []string{"cicd"}})

	err := handler.Work(context.Background(), executionAtAttempt(1, 5))
	if !errors.Is(err, ErrZeroRowsWithSourceData) {
		t.Fatalf("Work error = %v, want ErrZeroRowsWithSourceData", err)
	}
	requireTerminalPartition(t, store, "zero_rows_with_source_data")
}

// The other deterministic exits after a claim, each a permanent invalid_state.
func TestPartitionInvalidStateExitsEndFailedPermanent(t *testing.T) {
	for name, build := range map[string]func(store *fakeStore, handler *PartitionHandler){
		"load run is invalid": func(store *fakeStore, _ *PartitionHandler) { store.loadErr = ErrInvalidState },
		"run belongs to another organization": func(store *fakeStore, _ *PartitionHandler) {
			store.run.OrganizationID = "00000000-0000-4000-8000-0000000000ff"
		},
		"source check invalid state": func(_ *fakeStore, handler *PartitionHandler) {
			handler.SetSourceDataChecker(errorSourceDataChecker{err: ErrInvalidState})
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := terminalPartitionStore()
			handler := terminalPartitionHandler(t, store)
			build(store, handler)
			if err := handler.Work(context.Background(), executionAtAttempt(1, 5)); err == nil {
				t.Fatal("Work succeeded")
			}
			requireTerminalPartition(t, store, "invalid_state")
		})
	}
}

// C2: a retryable error is released for the next attempt, and written
// failed_permanent only on the attempt River will not follow with another.
func TestPartitionRetryableErrorEndsFailedPermanentOnTheLastAttempt(t *testing.T) {
	for name, build := range map[string]func(store *fakeStore, handler *PartitionHandler){
		"complete fails": func(store *fakeStore, _ *PartitionHandler) { store.completionErr = errors.New("write failed") },
		"source check fails": func(_ *fakeStore, handler *PartitionHandler) {
			handler.SetSourceDataChecker(errorSourceDataChecker{err: errors.New("clickhouse unavailable")})
		},
		"load run fails": func(store *fakeStore, _ *PartitionHandler) { store.loadErr = errors.New("postgres unavailable") },
	} {
		t.Run(name+"/last attempt", func(t *testing.T) {
			store := terminalPartitionStore()
			handler := terminalPartitionHandler(t, store)
			build(store, handler)
			if err := handler.Work(context.Background(), executionAtAttempt(5, 5)); err == nil {
				t.Fatal("Work succeeded")
			}
			requireTerminalPartition(t, store, "attempts_exhausted")
		})
		t.Run(name+"/earlier attempt", func(t *testing.T) {
			store := terminalPartitionStore()
			handler := terminalPartitionHandler(t, store)
			build(store, handler)
			if err := handler.Work(context.Background(), executionAtAttempt(4, 5)); err == nil {
				t.Fatal("Work succeeded")
			}
			if store.permanentFailures != 0 || store.partitionReleases != 1 {
				t.Fatalf("attempt 4 of 5: permanent=%d releases=%d, want 0/1 (the retry must find a 'failed' row to claim)",
					store.permanentFailures, store.partitionReleases)
			}
		})
	}
}

// A job whose MaxAttempts is unset is never read as being on its last attempt.
func TestPartitionWithNoAttemptLimitIsNeverOnItsLastAttempt(t *testing.T) {
	store := terminalPartitionStore()
	store.completionErr = errors.New("write failed")
	handler := terminalPartitionHandler(t, store)
	if err := handler.Work(context.Background(), executionAtAttempt(0, 0)); err == nil {
		t.Fatal("Work succeeded")
	}
	if store.permanentFailures != 0 || store.partitionReleases != 1 {
		t.Fatalf("permanent=%d releases=%d, want 0/1", store.permanentFailures, store.partitionReleases)
	}
}

// A family-incomplete release keeps its own reason when it is the last attempt.
func TestPartitionFamilyIncompleteKeepsItsReasonOnTheLastAttempt(t *testing.T) {
	store := terminalPartitionStore()
	handler := terminalPartitionHandler(t, store)
	if err := handler.SetNativeFamilies(map[string]NativeFamilyExecutor{"team_wellbeing": nil}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), executionAtAttempt(5, 5)); !errors.Is(err, ErrPreBridgeFamilyIncomplete) {
		t.Fatalf("Work error = %v, want ErrPreBridgeFamilyIncomplete", err)
	}
	requireTerminalPartition(t, store, jobruntime.ReasonPreBridgeFamilyIncomplete.String())

	earlier := terminalPartitionStore()
	handler = terminalPartitionHandler(t, earlier)
	if err := handler.SetNativeFamilies(map[string]NativeFamilyExecutor{"team_wellbeing": nil}); err != nil {
		t.Fatal(err)
	}
	_ = handler.Work(context.Background(), executionAtAttempt(2, 5))
	if earlier.permanentFailures != 0 || earlier.releasesWithReason != 1 {
		t.Fatalf("attempt 2 of 5: permanent=%d releasesWithReason=%d, want 0/1", earlier.permanentFailures, earlier.releasesWithReason)
	}
}

// The post_bridge twin of the above.
func TestPartitionPostBridgeFamilyIncompleteKeepsItsReasonOnTheLastAttempt(t *testing.T) {
	failing := func(store *fakeStore) *PartitionHandler {
		handler := terminalPartitionHandler(t, store)
		handler.SetPostBridgeNativeFamilies(map[string]NativeFamilyExecutor{
			"work_item_state": &fakeNativeFamilyExecutor{err: errors.New("transient clickhouse failure")},
		})
		return handler
	}
	store := terminalPartitionStore()
	if err := failing(store).Work(context.Background(), executionAtAttempt(5, 5)); !errors.Is(err, ErrPostBridgeFamilyIncomplete) {
		t.Fatalf("Work error = %v, want ErrPostBridgeFamilyIncomplete", err)
	}
	requireTerminalPartition(t, store, jobruntime.ReasonPostBridgeFamilyIncomplete.String())

	earlier := terminalPartitionStore()
	_ = failing(earlier).Work(context.Background(), executionAtAttempt(2, 5))
	if earlier.permanentFailures != 0 || earlier.releasesWithReason != 1 {
		t.Fatalf("attempt 2 of 5: permanent=%d releasesWithReason=%d, want 0/1", earlier.permanentFailures, earlier.releasesWithReason)
	}
}

// The terminal write finds another holder: this attempt does not own the row, so it
// neither releases it (that would clobber the holder or write 'failed' with no
// reason) nor fails; it leaves one WARN naming the partition and the attempt.
func TestPartitionTerminalWriteThatLostItsLeaseIsLoggedAndLeftAlone(t *testing.T) {
	store := terminalPartitionStore()
	store.permanentFailureErr = ErrLeaseLost
	handler := terminalPartitionHandler(t, store)
	handler.SetSourceDataChecker(familiesSourceDataChecker{families: []string{"cicd"}})
	var logs bytes.Buffer
	execution := executionAtAttempt(2, 5)
	execution.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	err := handler.Work(context.Background(), execution)
	if !errors.Is(err, ErrZeroRowsWithSourceData) {
		t.Fatalf("Work error = %v, want the original ErrZeroRowsWithSourceData unchanged", err)
	}
	if store.permanentFailures != 1 || store.partitionReleases != 0 || store.releasesWithReason != 0 {
		t.Fatalf("permanent=%d releases=%d withReason=%d, want 1/0/0: a lost lease must not fall back to a release",
			store.permanentFailures, store.partitionReleases, store.releasesWithReason)
	}
	text := logs.String()
	if strings.Count(text, `"level":"WARN"`) != 1 || !strings.Contains(text, testPartitionID) || !strings.Contains(text, `"attempt":2`) {
		t.Fatalf("want exactly one WARN naming the partition and attempt 2, got:\n%s", text)
	}
}

// A partition of a run that already ended blocks nothing and the marker reads running
// runs only, so it is released as before.
func TestPartitionOfAnEndedRunIsReleasedNotFailedPermanent(t *testing.T) {
	store := terminalPartitionStore()
	store.run.Status = "failed"
	handler := terminalPartitionHandler(t, store)
	if err := handler.Work(context.Background(), executionAtAttempt(1, 5)); err == nil {
		t.Fatal("Work succeeded")
	}
	if store.permanentFailures != 0 || store.partitionReleases != 1 {
		t.Fatalf("permanent=%d releases=%d, want 0/1", store.permanentFailures, store.partitionReleases)
	}
}

// C3: the reasons a partition is ended with are in the vocabulary the Postgres store
// validates. A real store refuses a reason outside it with ErrInvalidState, and the
// fake store used above accepts any string, so this is the only place a missing
// entry shows. (ReleasePartitionWithReason checks the same set before any query.)
func TestPartitionTerminalReasonsAreInThePostgresVocabulary(t *testing.T) {
	store := &PostgresStore{}
	for _, reason := range []string{
		"zero_rows_with_source_data", "invalid_state", "attempts_exhausted", "terminal_unclassified",
	} {
		err := store.ReleasePartitionWithReason(context.Background(), PartitionClaim{}, reason)
		if errors.Is(err, ErrInvalidState) || !errors.Is(err, ErrUnavailable) {
			t.Errorf("reason %q: err = %v, want ErrUnavailable (the zero-value store's own guard, after the vocabulary check passed)", reason, err)
		}
	}
}
