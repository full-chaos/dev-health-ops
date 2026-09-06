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

// restoreRecognisedFinalizeFamilies puts the production set back. Taken as an
// argument rather than read inside, so the deferred call captures the value at
// defer time and a test cannot accidentally restore a set another test left
// behind.
func restoreRecognisedFinalizeFamilies(original []string) {
	pythonRecognisedFinalizeFamilies = original
}

// A registered finalize family that SUCCEEDS runs exactly once, and every
// recognised family must be registered for Work to succeed at all (CHAOS-3092
// PR-A': no bridge left to cover an unregistered one) -- narrowed to just this
// one name for the test's duration, the same pattern every test below uses.
func TestSucceedingNativeFinalizeFamilyRunsExactlyOnce(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
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
}

// NO FAIL-OPEN, AT ALL (#2241 r2 Findings 1 and 2; team-lead's ruling). A
// family that errors fails the WHOLE attempt -- there is no bridge left to
// cover it even if one existed, and CHAOS-3092 PR-A' removed the bridge call
// entirely regardless.
func TestFailingNativeFinalizeFamilyFailsTheAttempt(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
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
}

// CHAOS-3092 PR-A': with the compatibility bridge deleted, a recognised
// finalize family with NO registered executor at all is no longer an inert
// no-op (there is nothing left to fall open to) -- it must fail the attempt
// loudly, the same as a family that ran and failed. This REPLACES
// TestFinalizeWithoutNativeFamiliesIsUnchanged, which asserted the OPPOSITE
// (the bridge quietly covering everything) under the now-deleted bridge.
func TestFinalizeWithoutNativeFamiliesFailsLoud(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	workErr := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if workErr == nil {
		t.Fatal("Work succeeded with zero registered finalize families -- with no " +
			"compatibility bridge left, nothing would ever write ic_finalize's rows")
	}
	if !errors.Is(workErr, ErrFinalizeFamilyIncomplete) {
		t.Fatalf("err = %v, want it to wrap ErrFinalizeFamilyIncomplete", workErr)
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
// (pythonRecognisedFinalizeFamilies[index:]) -- so which family runs first,
// and which families are still ahead of it when a cancellation lands, is a
// real operational decision, not an implementation detail a name's spelling
// should get to make. Order is observed via the recording observer's call
// sequence now that there is no bridge skip-list to inspect.
func TestFinalizeFamiliesIterateInDeclaredOrderNotSortedName(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	// The registration guard only admits recognised names, and deterministic
	// ordering needs more than one name to be observable at all. Widening the
	// recognised set for the duration of this test is the honest way to get
	// there: inventing three production family names would make the guard's
	// own test lie about what is actually recognised.
	pythonRecognisedFinalizeFamilies = []string{"zeta", "alpha", "mid"}

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingFinalizeObserver{}
	handler.SetNativeFinalizeFamilyObserver(observer)
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"alpha": &stubFinalizeFamily{}, "mid": &stubFinalizeFamily{}, "zeta": &stubFinalizeFamily{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatal(err)
	}
	want := []string{"zeta:computed", "alpha:computed", "mid:computed"} // declared order -- sorted would be alpha, mid, zeta
	if len(observer.calls) != len(want) {
		t.Fatalf("observed=%v, want %v", observer.calls, want)
	}
	for i, call := range want {
		if observer.calls[i] != call {
			t.Fatalf("observed=%v, want %v (declared order, not sorted)", observer.calls, want)
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
// though the run itself now fails (CHAOS-3092 PR-A' removed the bridge
// entirely, so there is no "still succeeds" half left to assert).
func TestFailingNativeFinalizeFamilyIsReportedRefused(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
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
}

// A succeeding family reports computed WITH its row count, so the series can
// distinguish "ran and wrote nothing" from "did not run".
func TestSucceedingNativeFinalizeFamilyIsReportedComputedWithRows(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
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
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(previous)

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
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

// CHAOS-5151. SetNativeFinalizeFamilies validated only the NAME, not the
// executor value -- a nil executor for a recognised name was accepted and
// registered, then would vanish silently in computeNativeFinalizeFamilies's
// old `if executor == nil { continue }` shape. This is the FIRST of two
// defenses against that class now: registration itself refuses a nil value
// outright (this test); computeNativeFinalizeFamilies's own
// ErrFinalizeFamilyIncomplete check (TestFinalizeWithoutNativeFamiliesFailsLoud)
// is the second, for a name that was never registered at all rather than
// registered with nil.
func TestSetNativeFinalizeFamiliesRejectsNilExecutor(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
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

// TestRegisteringAnUnrecognisedFinalizeFamilyIsRefused moved here from the
// now-deleted finalize_family_gate_agreement_test.go (CHAOS-3092 PR-A': that
// file's other three tests were the Python-source-scan agreement checks,
// which have nothing left to agree with once the bridge is gone; this one
// tests SetNativeFinalizeFamilies's own name-validation, unrelated to the
// bridge, and stays valid unchanged). The guard has to REFUSE, not merely
// warn: a dropped-but-registered family would be the same two-writer bug
// wearing a different hat (now: the same "nobody writes this family" bug).
func TestRegisteringAnUnrecognisedFinalizeFamilyIsRefused(t *testing.T) {
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim())
	if err != nil {
		t.Fatal(err)
	}
	err = handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalise": &stubFinalizeFamily{},
	})
	if err == nil {
		t.Fatal("registering the typo'd name succeeded; nothing recognises it as a real family")
	}
	// Registration is all-or-nothing: a refused call must leave NOTHING behind,
	// or the caller believes a family is native when it is not.
	if len(handler.nativeFinalizeFamilyNames) != 0 {
		t.Fatalf("refused registration still recorded %v", handler.nativeFinalizeFamilyNames)
	}
}

// The three tests below close the coverage gap the r3 class-sweep found
// (CHAOS-4290): releaseFinalize's error was discarded at three call sites in
// Work, all fixed to log via finalizeExecutionLogger/finalizeLogger, but none
// of the three logging branches had ANY test forcing ReleaseFinalize itself
// to fail -- go tool cover showed finalizeExecutionLogger at 0.0%. Each test
// below drives Work down one of the three call sites and asserts the log
// line, mirroring TestFinalizeSucceedsWhenTheObserverFails' capture pattern.

// TestFinalizeLogsAReleaseFailureFromTheMismatchGuard hits the first call
// site (Work's claim/run mismatch guard, before any native family runs).
func TestFinalizeLogsAReleaseFailureFromTheMismatchGuard(t *testing.T) {
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(previous)

	store := finalizeStoreWithClaim()
	store.releaseFinalizeErr = errors.New("release down")
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}

	// A different org than the claim's own triggers the mismatch guard
	// (claim.Run.OrganizationID != *execution.OrganizationID) without
	// touching any native family.
	execution := finalizeExecutionFor(testRunID)
	mismatchedOrg := "org-does-not-match"
	execution.OrganizationID = &mismatchedOrg
	execution.Envelope.OrganizationID = &mismatchedOrg
	execution.Args.EnvelopeArgs.OrganizationID = &mismatchedOrg

	workErr := handler.Work(context.Background(), execution)
	if !errors.Is(workErr, ErrInvalidState) {
		t.Fatalf("Work = %v, want it to wrap ErrInvalidState", workErr)
	}
	if store.finalizeReleases != 1 {
		t.Fatalf("finalizeReleases = %d, want 1 -- the mismatch guard must still attempt a release", store.finalizeReleases)
	}
	assertFinalizeReleaseFailureLogged(t, captured.String(), "release down")
}

// TestFinalizeLogsAReleaseFailureAfterANativeFamilyFailure hits the second
// call site: a native family fails on an attempt that is NOT the final one,
// so Work releases (rather than terminalizes) the claim for a future retry.
func TestFinalizeLogsAReleaseFailureAfterANativeFamilyFailure(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(previous)

	store := finalizeStoreWithClaim()
	store.releaseFinalizeErr = errors.New("release down")
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	family := &stubFinalizeFamily{err: errors.New("clickhouse hiccup")}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
		t.Fatal(err)
	}

	execution := finalizeExecutionFor(testRunID)
	execution.Attempt = 0
	execution.Definition.MaxAttempts = 3 // attempt (0) < max (3): retryable, not terminal.

	workErr := handler.Work(context.Background(), execution)
	if !errors.Is(workErr, ErrNativeFinalizeFamilyFailed) {
		t.Fatalf("Work = %v, want it to wrap ErrNativeFinalizeFamilyFailed", workErr)
	}
	if store.failedFinalizePermanently != 0 {
		t.Fatalf("failedFinalizePermanently = %d, want 0 -- attempt 0 of 3 must release, not terminalize",
			store.failedFinalizePermanently)
	}
	if store.finalizeReleases != 1 {
		t.Fatalf("finalizeReleases = %d, want 1", store.finalizeReleases)
	}
	assertFinalizeReleaseFailureLogged(t, captured.String(), "release down")
}

// TestFinalizeLogsAReleaseFailureAfterACompleteFinalizeFailure hits the third
// call site: every native family succeeds, but CompleteFinalize itself
// fails, so Work releases the claim it can no longer mark complete. No
// finalize family is registered here on purpose -- CHAOS-3092 PR-A' widens
// the recognised set to EMPTY for this test specifically, so
// computeNativeFinalizeFamilies has nothing to iterate and succeeds
// trivially, keeping this test's actual subject (CompleteFinalize's own
// failure path) isolated from the unrelated ErrFinalizeFamilyIncomplete gate.
func TestFinalizeLogsAReleaseFailureAfterACompleteFinalizeFailure(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{}

	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(previous)

	store := finalizeStoreWithClaim()
	store.releaseFinalizeErr = errors.New("release down")
	store.completionErr = errors.New("completion down")
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}

	workErr := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if workErr == nil {
		t.Fatal("Work succeeded despite CompleteFinalize failing")
	}
	if store.finalizeReleases != 1 {
		t.Fatalf("finalizeReleases = %d, want 1", store.finalizeReleases)
	}
	assertFinalizeReleaseFailureLogged(t, captured.String(), "release down")
	assertFinalizeReleaseFailureLogged(t, captured.String(), "completion down")
}

// TestFinalizeLogsACompleteFinalizeFailureEvenWhenReleaseSucceeds is the r3
// finding: CompleteFinalize's OWN error was never logged, only a SUBSEQUENT
// release failure -- so the common case (completion fails, release
// succeeds) left no log at all naming why completion failed. Isolated from
// the test above, which always failed both calls and so could not tell
// completion's own log line apart from coincidentally passing because
// release's failure was logged instead. Same empty-recognised-set shape as
// the test above, for the same reason.
func TestFinalizeLogsACompleteFinalizeFailureEvenWhenReleaseSucceeds(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{}

	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(previous)

	store := finalizeStoreWithClaim()
	store.completionErr = errors.New("completion down")
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}

	workErr := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if workErr == nil {
		t.Fatal("Work succeeded despite CompleteFinalize failing")
	}
	if store.finalizeReleases != 1 {
		t.Fatalf("finalizeReleases = %d, want 1", store.finalizeReleases)
	}
	assertFinalizeReleaseFailureLogged(t, captured.String(), "completion down")
}

// assertFinalizeReleaseFailureLogged is the three tests' shared assertion:
// exactly the shape TestFinalizeSucceedsWhenTheObserverFails already
// verifies for the telemetry-observer class of swallowed error, applied here
// to the store-release class instead.
func assertFinalizeReleaseFailureLogged(t *testing.T, logOutput, wantErrSubstring string) {
	t.Helper()
	if logOutput == "" {
		t.Fatal("ReleaseFinalize's error was not logged -- a release failure here is " +
			"exactly as invisible as the terminal-write failure r2 finding #1 fixed")
	}
	// The release-failure log line is not necessarily the FIRST line: the
	// native-family-failure or CompleteFinalize-failure test scenarios log
	// their OWN cause first via the existing "daily finalize failed" line,
	// and the release-failure line follows it -- find the line naming this
	// test's own error, rather than assume line 1.
	var record map[string]any
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(logOutput), "\n") {
		var candidate map[string]any
		if err := json.Unmarshal([]byte(line), &candidate); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		if got, _ := candidate["error"].(string); strings.Contains(got, wantErrSubstring) {
			record = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no log line contained error %q; got:\n%s", wantErrSubstring, logOutput)
	}
	for _, field := range []string{"run_id", "organization_id", "target_day"} {
		if _, present := record[field]; !present {
			t.Errorf("log line is missing %q; got keys %v", field, keysOf(record))
		}
	}
}
