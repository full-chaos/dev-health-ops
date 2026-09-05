package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// recordingFinalizeCompatibility captures the skip list the bridge was handed,
// which is the whole observable output of the mechanism at this level.
type recordingFinalizeCompatibility struct {
	fakeCompatibility
	sawSkip   []string
	callCount int
}

func (compatibility *recordingFinalizeCompatibility) Finalize(_ context.Context, _ Run, skipFamilies []string) error {
	compatibility.callCount++
	compatibility.sawSkip = skipFamilies
	return nil
}

type stubFinalizeFamily struct {
	rows  int
	err   error
	calls int
}

func (family *stubFinalizeFamily) ComputeFinalizeFamily(context.Context, Run) (int, error) {
	family.calls++
	return family.rows, family.err
}

func finalizeExecutionFor(runID string) *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(testOrgID),
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
		},
		Args: jobruntime.DailyMetricsFinalizeArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload]{
				OrganizationID: pointer(testOrgID),
				Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
				Payload:        jobcontract.DailyMetricsFinalizePayload{RunID: runID},
			},
		},
	}
}

func finalizeStoreWithClaim() *fakeStore {
	return &fakeStore{
		run: Run{ID: testRunID, OrganizationID: testOrgID, Status: "running"},
		finalizeClaim: &FinalizeClaim{
			Run:           Run{ID: testRunID, OrganizationID: testOrgID, Status: "running"},
			Token:         "token",
			LeaseDuration: 30 * time.Millisecond,
		},
	}
}

// A registered finalize family that SUCCEEDS must appear in the skip list the
// bridge receives -- that skip is the only thing stopping Python recomputing
// and silently superseding the rows Go just wrote.
func TestSucceedingNativeFinalizeFamilyIsSkippedByTheBridge(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	family := &stubFinalizeFamily{rows: 7}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
		t.Fatal(err)
	}

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success", err)
	}
	if family.calls != 1 {
		t.Fatalf("family calls = %d, want 1", family.calls)
	}
	if len(compatibility.sawSkip) != 1 || compatibility.sawSkip[0] != "ic_finalize" {
		t.Fatalf("bridge saw skip=%v, want [ic_finalize] -- without it Python "+
			"recomputes the family and its rows supersede the native ones", compatibility.sawSkip)
	}
}

// FAIL-OPEN, carried unchanged from CHAOS-4276's partition-side ruling: a
// family that errors is LEFT OUT of the skip list, so the bridge computes it
// exactly as before. The finalize itself must still succeed -- one family
// degrading to Python must not fail the run.
// REPLACES TestFailingNativeFinalizeFamilyFallsBackToTheBridge, which asserted
// the OPPOSITE and was correct only under the fail-open policy #2241 r2
// Findings 1 and 2 removed. Its reasoning -- "a failed family must not be
// skipped, or its rows are written by nobody" -- is answered by the redrive:
// the rows are written by the NEXT attempt, not by Python.
//
// Letting the bridge cover it was the hazard, because a family that failed on
// this attempt may have succeeded on a previous one.
func TestFailingNativeFinalizeFamilyFailsTheAttemptInsteadOfFallingBack(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	family := &stubFinalizeFamily{err: errors.New("clickhouse hiccup")}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
		t.Fatal(err)
	}

	workErr := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if workErr == nil {
		t.Fatal("Work succeeded after a native family failed -- the run completes and " +
			"is never redriven, and the family's rows are written by nobody")
	}
	if !errors.Is(workErr, ErrNativeFinalizeFamilyFailed) {
		t.Fatalf("err = %v, want it to wrap ErrNativeFinalizeFamilyFailed", workErr)
	}
	if compatibility.callCount != 0 {
		t.Fatalf("bridge calls = %d, want 0 -- Python must never compute a family "+
			"registered as native, or a retry can reintroduce it as a second writer",
			compatibility.callCount)
	}
}

// The default is inert: no registered families means the bridge is called
// exactly as it was before this capability existed.
func TestFinalizeWithoutNativeFamiliesIsUnchanged(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatal(err)
	}
	if compatibility.callCount != 1 || len(compatibility.sawSkip) != 0 {
		t.Fatalf("calls=%d skip=%v, want 1 and empty", compatibility.callCount, compatibility.sawSkip)
	}
}

// Deterministic order, and specifically pythonRecognisedFinalizeFamilies'
// DECLARED order rather than sort.Strings(names) -- the two are chosen to
// DISAGREE here ("zeta" declared before "alpha") so a regression back to a
// lexical sort cannot pass this test by accident the way it could if the
// declared and sorted orders happened to coincide.
//
// Why it has to be declared order, not just A deterministic one:
// computeNativeFinalizeFamilies marks every name from the current loop INDEX
// onward as refused when the run's context is cancelled mid-loop
// (nativeFinalizeFamilyNames[index:]) -- so which family runs first, and
// which families are still ahead of it when a cancellation lands, is a real
// operational decision, not an implementation detail a name's spelling should
// get to make.
func TestFinalizeFamiliesIterateInDeclaredOrderNotSortedName(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	// The registration guard only admits names the Python bridge gates on, and
	// deterministic ordering needs more than one name to be observable at all.
	// Widening the recognised set for the duration of this test is the honest
	// way to get there: inventing three production family names would make the
	// guard's own test lie about what Python understands.
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"zeta", "alpha", "mid"}

	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"alpha": &stubFinalizeFamily{}, "mid": &stubFinalizeFamily{}, "zeta": &stubFinalizeFamily{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatal(err)
	}
	want := []string{"zeta", "alpha", "mid"} // declared order -- sorted would be alpha, mid, zeta
	if len(compatibility.sawSkip) != len(want) {
		t.Fatalf("skip=%v, want %v", compatibility.sawSkip, want)
	}
	for i, name := range want {
		if compatibility.sawSkip[i] != name {
			t.Fatalf("skip=%v, want %v (declared order, not sorted)", compatibility.sawSkip, want)
		}
	}
}

// CHAOS-5141: the REAL production pythonRecognisedFinalizeFamilies, not a
// synthetic fixture like the declared-order test above. team_cognitive_load
// reads user_metrics_daily rows ic_finalize writes for the SAME run, so
// ic_finalize must iterate first -- computeNativeFinalizeFamilies now walks
// this list in its own declared order (TestFinalizeFamiliesIterateInDeclaredOrderNotSortedName
// proves the mechanism), so the ordering guarantee is only as good as this
// list's actual element order. A future edit that appends
// TeamCognitiveLoadFamilyName before "ic_finalize" (or reorders them) would
// compile and pass every other test here while silently making
// team_cognitive_load read a partial/stale user_metrics_daily on every run.
func TestICFinalizePrecedesTeamCognitiveLoadInDeclaredOrder(t *testing.T) {
	icIndex, teamCognitiveLoadIndex := -1, -1
	for index, name := range pythonRecognisedFinalizeFamilies {
		switch name {
		case ICFinalizeFamilyName:
			icIndex = index
		case TeamCognitiveLoadFamilyName:
			teamCognitiveLoadIndex = index
		}
	}
	if icIndex == -1 || teamCognitiveLoadIndex == -1 {
		t.Fatalf("pythonRecognisedFinalizeFamilies=%v missing ic_finalize or team_cognitive_load "+
			"-- this test cannot assert an ordering between two names it cannot find",
			pythonRecognisedFinalizeFamilies)
	}
	if icIndex >= teamCognitiveLoadIndex {
		t.Fatalf("pythonRecognisedFinalizeFamilies=%v has ic_finalize at index %d and "+
			"team_cognitive_load at index %d -- ic_finalize must iterate FIRST, since "+
			"team_cognitive_load reads user_metrics_daily rows ic_finalize writes in the same run",
			pythonRecognisedFinalizeFamilies, icIndex, teamCognitiveLoadIndex)
	}
}

type recordingFinalizeObserver struct {
	calls []string
	rows  map[string]int
	err   error
}

func (observer *recordingFinalizeObserver) ObserveDailyMetricsNativeFamily(
	family string, outcome jobruntime.DailyMetricsNativeFamilyOutcome, rowsWritten int, _ time.Duration,
) error {
	if observer.rows == nil {
		observer.rows = map[string]int{}
	}
	observer.calls = append(observer.calls, family+":"+string(outcome))
	observer.rows[family] = rowsWritten
	return observer.err
}

// CHAOS-4290 shipped the mechanism with fail-open and NO counter, which its own
// RISK-NOTES admitted meant a family failing every run degraded to Python
// invisibly. This is that gap closed: a REFUSED outcome must be reported even
// though the finalize still succeeds.
func TestFailingNativeFinalizeFamilyIsReportedRefused(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingFinalizeObserver{}
	handler.SetNativeFinalizeFamilyObserver(observer)
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{err: errors.New("clickhouse hiccup")},
	}); err != nil {
		t.Fatal(err)
	}

	// The attempt FAILS now (#2241 r2 Findings 1 and 2) instead of degrading to
	// Python. This test survived the forward-merge textually while asserting the
	// opposite semantics -- a clean auto-merge is not a semantic merge.
	workErr := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if workErr == nil {
		t.Fatal("Work succeeded after a native family failed -- the run completes " +
			"and is never redriven")
	}
	if !errors.Is(workErr, ErrNativeFinalizeFamilyFailed) {
		t.Fatalf("err = %v, want it to wrap ErrNativeFinalizeFamilyFailed", workErr)
	}
	// The counter is still the point, and still the reason this test exists: a
	// family that redrives forever with no counter is exactly as invisible as
	// one that silently degraded to Python.
	if len(observer.calls) != 1 || observer.calls[0] != "ic_finalize:refused" {
		t.Fatalf("observed %v, want [ic_finalize:refused]", observer.calls)
	}
	if compatibility.callCount != 0 {
		t.Fatalf("bridge calls = %d, want 0 -- Python must never compute a family "+
			"registered as native", compatibility.callCount)
	}
}

// A succeeding family reports computed WITH its row count, so the series can
// distinguish "ran and wrote nothing" from "did not run".
func TestSucceedingNativeFinalizeFamilyIsReportedComputedWithRows(t *testing.T) {
	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store, &recordingFinalizeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingFinalizeObserver{}
	handler.SetNativeFinalizeFamilyObserver(observer)
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{rows: 42},
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatal(err)
	}
	if len(observer.calls) != 1 || observer.calls[0] != "ic_finalize:computed" {
		t.Fatalf("observed %v, want [ic_finalize:computed]", observer.calls)
	}
	if observer.rows["ic_finalize"] != 42 {
		t.Fatalf("rows = %d, want 42 -- a computed outcome with no row count cannot "+
			"distinguish 'wrote nothing' from 'did not run'", observer.rows["ic_finalize"])
	}
}

// An observer that itself errors must not fail the job, matching every other
// observer in this package.
//
// CHAOS-5151: this fixture already existed and PROVED HALF the point --
// "still succeeds" -- but never checked the other half: the observer's error
// used to be discarded outright (`_ = ...ObserveDailyMetricsNativeFamily(...)`),
// so a family whose telemetry silently failed to record was AS INVISIBLE as
// one that was never registered at all, and the only sign anything went wrong
// would have been a counter that quietly never moved. Captures slog.Default()
// (the same fallback the finalize-failure log uses) for the duration of the
// test to assert the underlying error and identifiers actually land somewhere
// an operator can read.
func TestFinalizeSucceedsWhenTheObserverFails(t *testing.T) {
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(previous)

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store, &recordingFinalizeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetNativeFinalizeFamilyObserver(&recordingFinalizeObserver{err: errors.New("telemetry down")})
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{rows: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success despite the observer failure", err)
	}

	line := captured.String()
	if line == "" {
		t.Fatal("the observer's error was not logged -- a family whose telemetry " +
			"silently fails to record is exactly as invisible as one never registered")
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.Split(line, "\n")[0])), &record); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, line)
	}
	if got, _ := record["error"].(string); !strings.Contains(got, "telemetry down") {
		t.Errorf("logged error = %q, want it to contain the underlying cause", got)
	}
	for _, field := range []string{"run_id", "organization_id", "target_day", "family"} {
		if _, present := record[field]; !present {
			t.Errorf("log line is missing %q; got keys %v", field, keysOf(record))
		}
	}
}

// restoreRecognisedFinalizeFamilies puts the production set back. Taken as an
// argument rather than read inside, so the deferred call captures the value at
// defer time and a test cannot accidentally restore a set another test left
// behind.
func restoreRecognisedFinalizeFamilies(original []string) {
	pythonRecognisedFinalizeFamilies = original
}

// CHAOS-5151. SetNativeFinalizeFamilies validated only the NAME, not the
// executor value -- a nil executor for a recognised name was accepted,
// registered into nativeFinalizeFamilyNames, and would then vanish silently
// in computeNativeFinalizeFamilies's `if executor == nil { continue }`
// WITHOUT being added to skipFamilies. Python's bridge would compute that
// family too: the exact two-writer hazard the name-validation a few lines up
// exists to prevent, reached through a nil value instead of a typo'd string.
func TestSetNativeFinalizeFamiliesRejectsNilExecutor(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": nil,
	})
	if err == nil {
		t.Fatal("SetNativeFinalizeFamilies accepted a nil executor for a recognised name")
	}
	if !errors.Is(err, ErrUnknownFinalizeFamily) {
		t.Fatalf("error = %v, want ErrUnknownFinalizeFamily", err)
	}
	// All-or-nothing, matching the name-rejection path just above it: a
	// rejected registration must not leave the handler in a half-registered
	// state believing the family is native.
	if len(handler.nativeFinalizeFamilyNames) != 0 {
		t.Fatalf("nativeFinalizeFamilyNames = %v, want empty after a rejected registration",
			handler.nativeFinalizeFamilyNames)
	}
}
