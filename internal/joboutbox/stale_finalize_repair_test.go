package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river/rivertype"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// scriptedStaleFinalizeRedriver records the call order and fails the runs it
// is told to fail.
type scriptedStaleFinalizeRedriver struct {
	mu    sync.Mutex
	calls []string
	fail  map[string]error
	// before runs inside each call, with the call's context.
	before func(ctx context.Context, runID string) error
}

func (redriver *scriptedStaleFinalizeRedriver) RedriveStaleFinalize(ctx context.Context, runID, _ string) (bool, error) {
	redriver.mu.Lock()
	redriver.calls = append(redriver.calls, runID)
	err := redriver.fail[runID]
	before := redriver.before
	redriver.mu.Unlock()
	if before != nil {
		if beforeErr := before(ctx, runID); beforeErr != nil {
			return false, beforeErr
		}
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (redriver *scriptedStaleFinalizeRedriver) takeCalls() []string {
	redriver.mu.Lock()
	defer redriver.mu.Unlock()
	calls := redriver.calls
	redriver.calls = nil
	return calls
}

// keysetSurvey emulates statement A over a fixed, ordered candidate list: the
// candidates after the cursor, at most limit of them.
type keysetSurvey struct {
	mu      sync.Mutex
	runs    []staleFinalizeCandidate
	levels  *StaleFinalizeLevels
	fail    []StaleFinalizeFailure
	calls   int
	cursors []*staleFinalizeCursor
}

func (survey *keysetSurvey) read(_ context.Context, _ time.Time, limit int, cursor *staleFinalizeCursor) staleFinalizeSurvey {
	survey.mu.Lock()
	defer survey.mu.Unlock()
	survey.calls++
	survey.cursors = append(survey.cursors, cursor)
	if survey.fail != nil {
		return staleFinalizeSurvey{failures: survey.fail}
	}
	out := []staleFinalizeCandidate{}
	for _, run := range survey.runs {
		if cursor != nil && !(run.updatedAt.After(cursor.updatedAt) ||
			(run.updatedAt.Equal(cursor.updatedAt) && run.runID > cursor.runID)) {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, run)
	}
	return staleFinalizeSurvey{candidates: out, levels: survey.levels}
}

func staleRuns(n int) []staleFinalizeCandidate {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	runs := make([]staleFinalizeCandidate, 0, n)
	for i := 0; i < n; i++ {
		runs = append(runs, staleFinalizeCandidate{
			runID:     "00000000-0000-4000-8000-0000000000" + string(rune('a'+i/10)) + string(rune('0'+i%10)),
			updatedAt: base.Add(time.Duration(i) * time.Minute),
		})
	}
	return runs
}

func newScriptedStage(survey *keysetSurvey, redriver StaleFinalizeRedriver) *staleFinalizeStage {
	return &staleFinalizeStage{
		survey:   survey.read,
		redriver: redriver,
		clock:    time.Now,
		newNonce: func() string { return "nonce" },
	}
}

var staleFinalizeTestNow = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

// F5: one failed run never stops the pass. Run 1's redrive is refused with
// ErrStaleFinalizeInvalidState; run 2 is redriven in the SAME pass.
func TestStaleFinalizeOneFailedRunDoesNotStopThePass(t *testing.T) {
	runs := staleRuns(2)
	survey := &keysetSurvey{runs: runs}
	redriver := &scriptedStaleFinalizeRedriver{fail: map[string]error{runs[0].runID: ErrStaleFinalizeInvalidState}}
	pass := newScriptedStage(survey, redriver).step(context.Background(), staleFinalizeTestNow, 10)
	if !reflect.DeepEqual(pass.redriven, []string{runs[1].runID}) {
		t.Fatalf("redriven = %v, want run 2 only", pass.redriven)
	}
	want := []StaleFinalizeFailure{{Step: StaleFinalizeStepRedrive, Class: StaleFinalizeClassInvalidState}}
	if !reflect.DeepEqual(pass.failures, want) {
		t.Fatalf("failures = %+v, want %+v", pass.failures, want)
	}
	if pass.deferred != 0 {
		t.Fatalf("deferred = %d, want 0", pass.deferred)
	}
}

// F6: the keyset cursor. With limit L, the L oldest candidates fail in every
// pass and one newer run is healthy: the healthy run is redriven in pass 2,
// and the failing runs are tried again in pass 3 after the cursor wraps.
func TestStaleFinalizeFailingOldestRunsDoNotHideANewerOne(t *testing.T) {
	const limit = 3
	runs := staleRuns(limit + 1)
	fail := map[string]error{}
	for _, run := range runs[:limit] {
		fail[run.runID] = ErrStaleFinalizeInvalidState
	}
	survey := &keysetSurvey{runs: runs}
	redriver := &scriptedStaleFinalizeRedriver{fail: fail}
	stage := newScriptedStage(survey, redriver)
	ids := func(list []staleFinalizeCandidate) []string {
		out := []string{}
		for _, run := range list {
			out = append(out, run.runID)
		}
		return out
	}

	first := stage.step(context.Background(), staleFinalizeTestNow, limit)
	if got := redriver.takeCalls(); !reflect.DeepEqual(got, ids(runs[:limit])) || len(first.redriven) != 0 {
		t.Fatalf("pass 1 calls %v redriven %v, want the %d failing runs and nothing redriven", got, first.redriven, limit)
	}
	second := stage.step(context.Background(), staleFinalizeTestNow.Add(staleFinalizeInterval), limit)
	if got := redriver.takeCalls(); !reflect.DeepEqual(got, ids(runs[limit:])) ||
		!reflect.DeepEqual(second.redriven, ids(runs[limit:])) {
		t.Fatalf("pass 2 calls %v redriven %v, want the newer healthy run", got, second.redriven)
	}
	stage.step(context.Background(), staleFinalizeTestNow.Add(2*staleFinalizeInterval), limit)
	if got := redriver.takeCalls(); !reflect.DeepEqual(got, ids(runs[:limit])) {
		t.Fatalf("pass 3 calls %v, want the failing runs again after the cursor wrapped", got)
	}
	if survey.cursors[0] != nil || survey.cursors[1] == nil || survey.cursors[1].runID != runs[limit-1].runID || survey.cursors[2] != nil {
		t.Fatalf("cursors = %v, want nil, the last tried run, nil", survey.cursors)
	}
}

// The pass budget: each redrive takes 300 ms of pass time on a fake clock. The
// loop starts a run only while 200 ms + 50 ms remain, so it tries six and
// defers the rest without a failure. The deferred runs are the first of the
// next pass.
func TestStaleFinalizeDefersTheRestWhenTheBudgetEnds(t *testing.T) {
	runs := staleRuns(10)
	survey := &keysetSurvey{runs: runs}
	var mu sync.Mutex
	fake := staleFinalizeTestNow
	redriver := &scriptedStaleFinalizeRedriver{before: func(context.Context, string) error {
		mu.Lock()
		fake = fake.Add(300 * time.Millisecond)
		mu.Unlock()
		return nil
	}}
	stage := newScriptedStage(survey, redriver)
	stage.clock = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fake
	}
	pass := stage.step(context.Background(), staleFinalizeTestNow, 10)
	if len(pass.redriven) != 6 || pass.deferred != 4 || len(pass.failures) != 0 {
		t.Fatalf("redriven %d deferred %d failures %v, want 6, 4 and none", len(pass.redriven), pass.deferred, pass.failures)
	}
	redriver.takeCalls()
	stage.step(context.Background(), staleFinalizeTestNow.Add(staleFinalizeInterval), 10)
	if got := redriver.takeCalls(); len(got) == 0 || got[0] != runs[6].runID {
		t.Fatalf("pass 2 calls %v, want the first deferred run first", got)
	}
}

// Two deadline failures end the pass: each one closes a domain connection.
func TestStaleFinalizeTwoDeadlinesDeferTheRest(t *testing.T) {
	runs := staleRuns(5)
	survey := &keysetSurvey{runs: runs}
	redriver := &scriptedStaleFinalizeRedriver{before: func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ErrUnavailable
	}}
	pass := newScriptedStage(survey, redriver).step(context.Background(), staleFinalizeTestNow, 10)
	want := []StaleFinalizeFailure{
		{Step: StaleFinalizeStepRedrive, Class: StaleFinalizeClassDeadline},
		{Step: StaleFinalizeStepRedrive, Class: StaleFinalizeClassDeadline},
	}
	if !reflect.DeepEqual(pass.failures, want) || pass.deferred != 3 || len(redriver.takeCalls()) != 2 {
		t.Fatalf("failures %+v deferred %d, want two deadline failures and 3 deferred", pass.failures, pass.deferred)
	}
}

// The cadence: one pass per interval; a second step inside it reads nothing.
// A clock that stepped back by more than one interval runs again.
func TestStaleFinalizeRunsOncePerInterval(t *testing.T) {
	survey := &keysetSurvey{runs: staleRuns(1)}
	stage := newScriptedStage(survey, &scriptedStaleFinalizeRedriver{})
	stage.step(context.Background(), staleFinalizeTestNow, 10)
	if pass := stage.step(context.Background(), staleFinalizeTestNow.Add(staleFinalizeInterval-time.Second), 10); survey.calls != 1 || pass.levels != nil || pass.redriven != nil {
		t.Fatalf("a step inside the interval surveyed (calls %d, pass %+v)", survey.calls, pass)
	}
	stage.step(context.Background(), staleFinalizeTestNow.Add(staleFinalizeInterval), 10)
	if survey.calls != 2 {
		t.Fatalf("a step at the interval did not survey (calls %d)", survey.calls)
	}
	stage.step(context.Background(), staleFinalizeTestNow.Add(-2*staleFinalizeInterval), 10)
	if survey.calls != 3 {
		t.Fatalf("a clock stepped back past one interval held the stage (calls %d)", survey.calls)
	}
}

// A failed survey redrives nothing, keeps the cursor, and reports its failure.
func TestStaleFinalizeSurveyFailureRedrivesNothing(t *testing.T) {
	survey := &keysetSurvey{runs: staleRuns(3)}
	redriver := &scriptedStaleFinalizeRedriver{fail: map[string]error{staleRuns(1)[0].runID: ErrStaleFinalizeInvalidState}}
	stage := newScriptedStage(survey, redriver)
	stage.step(context.Background(), staleFinalizeTestNow, 1)
	kept := stage.cursor
	survey.fail = []StaleFinalizeFailure{{Step: StaleFinalizeStepSurvey, Class: StaleFinalizeClassDeadline}}
	redriver.takeCalls()
	pass := stage.step(context.Background(), staleFinalizeTestNow.Add(staleFinalizeInterval), 1)
	if len(redriver.takeCalls()) != 0 || !reflect.DeepEqual(pass.failures, survey.fail) || pass.levels != nil {
		t.Fatalf("pass after a failed survey = %+v", pass)
	}
	if kept == nil || stage.cursor != kept {
		t.Fatalf("cursor %v, want the kept cursor %v", stage.cursor, kept)
	}
}

func TestStaleFinalizeClassifiesRedriveAndSurveyErrors(t *testing.T) {
	for _, testCase := range []struct {
		err      error
		deadline bool
		want     string
	}{
		{ErrUnavailable, true, StaleFinalizeClassDeadline},
		{context.DeadlineExceeded, false, StaleFinalizeClassDeadline},
		{errors.Join(ErrStaleFinalizeInvalidState, errors.New("contract")), false, StaleFinalizeClassInvalidState},
		{ErrNotAuthorized, false, StaleFinalizeClassNotAuthorized},
		{ErrUnavailable, false, StaleFinalizeClassUnavailable},
	} {
		if got := classifyStaleFinalizeRedrive(testCase.err, testCase.deadline); got != testCase.want {
			t.Errorf("redrive %v deadline=%t = %s, want %s", testCase.err, testCase.deadline, got, testCase.want)
		}
	}
	for code, want := range map[string]string{
		"57014": StaleFinalizeClassDeadline, "55P03": StaleFinalizeClassDeadline,
		insufficientPrivilege: StaleFinalizeClassNotAuthorized, "08006": StaleFinalizeClassUnavailable,
	} {
		if got := classifyStaleFinalizeSurvey(context.Background(), &pgconn.PgError{Code: code}); got != want {
			t.Errorf("survey SQLSTATE %s = %s, want %s", code, got, want)
		}
	}
}

// The survey budget holds: the two statement timeouts fit the survey's client
// deadline, and the redrive phase that remains fits one per-run deadline plus
// the reserve.
func TestStaleFinalizeSurveyTimeoutsFitThePassBudget(t *testing.T) {
	candidates, levels, lock := 400*time.Millisecond, 250*time.Millisecond, 100*time.Millisecond
	for statement, want := range map[string]time.Duration{
		staleFinalizeCandidatesStatementTimeout: candidates,
		staleFinalizeLevelsStatementTimeout:     levels,
		staleFinalizeLockTimeout:                lock,
	} {
		if !strings.Contains(statement, "'"+want.String()+"'") {
			t.Fatalf("%q does not set %s", statement, want)
		}
	}
	if candidates+levels > staleFinalizeSurveyBudget || lock > candidates {
		t.Fatalf("survey timeouts %s + %s do not fit the survey deadline %s", candidates, levels, staleFinalizeSurveyBudget)
	}
	if staleFinalizeBudget-staleFinalizeSurveyBudget < staleFinalizeRunDeadline+staleFinalizeRunReserve {
		t.Fatal("the redrive phase cannot fit one run")
	}
}

// Done 3: the 24 h horizon stays above River's whole attempt budget for
// metrics.daily_finalize (4 x 900 s plus the longest backoff before each
// retry), so the stage never redrives beside a job River can still run.
func TestStaleFinalizeHorizonExceedsTheRetryBudget(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := registry.Descriptor(jobcontract.KindDailyMetricsFinalize)
	if !ok {
		t.Fatalf("registry has no %s", jobcontract.KindDailyMetricsFinalize)
	}
	if descriptor.MaxAttempts != 4 || descriptor.Timeout != 900*time.Second {
		t.Fatalf("descriptor budget %d x %s, the pin is 4 x 900 s: re-check the horizon", descriptor.MaxAttempts, descriptor.Timeout)
	}
	base := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	budget := time.Duration(descriptor.MaxAttempts) * descriptor.Timeout
	for attempt := 1; attempt < descriptor.MaxAttempts; attempt++ {
		var longest time.Duration
		for id := int64(1); id <= 1000; id++ {
			next := jobruntime.NextRetryAt(descriptor, &rivertype.JobRow{ID: id, Attempt: attempt, AttemptedAt: &base})
			if next.IsZero() {
				t.Fatalf("retry policy %q schedules no retry", descriptor.RetryPolicy)
			}
			if delay := next.Sub(base); delay > longest {
				longest = delay
			}
		}
		budget += longest
	}
	if StaleFinalizeHorizon <= budget {
		t.Fatalf("StaleFinalizeHorizon %s <= retry budget %s", StaleFinalizeHorizon, budget)
	}
}

// emptyRetiredKindRows answers observeRetiredKinds with no row.
func emptyRetiredKindRows(context.Context, string, ...any) (pgx.Rows, error) {
	return &fakeStrandSurveyRows{}, nil
}

// Fail-open through StrandRepair.Step: a stage failure is a counted failure in
// the result, and Step returns no error.
func TestStrandRepairStepCarriesTheStageAndNeverReturnsItsFailure(t *testing.T) {
	survey := &keysetSurvey{runs: staleRuns(2), levels: &StaleFinalizeLevels{NoPartitions: 2}}
	runs := staleRuns(2)
	repair := &StrandRepair{
		beginQueue:  func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		queryQueue:  emptyRetiredKindRows,
		queryDomain: func(context.Context, string, ...any) (pgx.Rows, error) { return nil, errors.New("unused") },
		client:      riverDeleteAdapter{},
		staleFinalize: newScriptedStage(survey, &scriptedStaleFinalizeRedriver{
			fail: map[string]error{runs[0].runID: ErrUnavailable},
		}),
	}
	result, err := repair.Step(context.Background(), staleFinalizeTestNow, 10)
	if err != nil {
		t.Fatalf("Step returned the stage failure: %v", err)
	}
	if !reflect.DeepEqual(result.StaleFinalizeRedriven, []string{runs[1].runID}) ||
		!reflect.DeepEqual(result.StaleFinalizeFailures, []StaleFinalizeFailure{{Step: StaleFinalizeStepRedrive, Class: StaleFinalizeClassUnavailable}}) ||
		result.StaleFinalizeLevels == nil || result.StaleFinalizeLevels.NoPartitions != 2 {
		t.Fatalf("result = %+v", result)
	}
}

// T3: Relay.stepRecovery copies the four stage fields, also on an error return.
func TestStepRecoveryCopiesTheStaleFinalizeFields(t *testing.T) {
	levels := &StaleFinalizeLevels{Live: 1, StrandShape: 2, Recent: 3, NoPartitions: 4, Truncated: true}
	failures := []StaleFinalizeFailure{{Step: StaleFinalizeStepLevels, Class: StaleFinalizeClassDeadline}}
	for _, stepErr := range []error{nil, ErrUnavailable} {
		strand := &fakeStrandRepair{result: StrandRepairResult{
			StaleFinalizeRedriven: []string{"r1"}, StaleFinalizeFailures: failures,
			StaleFinalizeDeferred: 5, StaleFinalizeLevels: levels,
		}, err: stepErr}
		relay := &Relay{repair: fakeTerminalRepair{}, strandRepair: strand}
		result, _ := relay.stepRecovery(context.Background(), time.Now(), 1)
		if !reflect.DeepEqual(result.StaleFinalizeRedriven, []string{"r1"}) || !reflect.DeepEqual(result.StaleFinalizeFailures, failures) ||
			result.StaleFinalizeDeferred != 5 || result.StaleFinalizeLevels != levels {
			t.Fatalf("err=%v: result = %+v", stepErr, result)
		}
	}
}

// The loop counts the stage, logs one WARN per failure and one INFO per
// redriven run, and WARNs on a level change only for no_partitions and
// strand_shape. A pass without levels keeps the gauges.
func TestStaleFinalizeLevelWarnsOnlyOnChange(t *testing.T) {
	clock := &testReconcilerClock{now: staleFinalizeTestNow}
	results := []StepResult{
		{StaleFinalizeLevels: &StaleFinalizeLevels{NoPartitions: 1, Recent: 2}},
		{StaleFinalizeLevels: &StaleFinalizeLevels{NoPartitions: 1, Recent: 5, Live: 3}},
		{StaleFinalizeLevels: &StaleFinalizeLevels{NoPartitions: 1, StrandShape: 2}, StaleFinalizeRedriven: []string{"r1"},
			StaleFinalizeFailures: []StaleFinalizeFailure{{Step: StaleFinalizeStepRedrive, Class: StaleFinalizeClassDeadline}}, StaleFinalizeDeferred: 3},
		{},
	}
	var buf bytes.Buffer
	stepper := loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		result := results[0]
		results = results[1:]
		return result, nil
	})
	loop, err := newReconcilerLoop(stepper, ReconcilerLoopConfig{
		PollInterval: minReconcilerPollInterval, Limit: 7,
		Registry: health.NewRegistry(time.Second), Logger: slog.New(slog.NewJSONHandler(&buf, nil)),
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	counts := func() (changed, failed, redriven int) {
		text := buf.String()
		return strings.Count(text, StaleFinalizeLevelChangedLogMessage),
			strings.Count(text, StaleFinalizeFailureLogMessage), strings.Count(text, StaleFinalizeRedrivenLogMessage)
	}
	for pass, want := range [][3]int{{1, 0, 0}, {1, 0, 0}, {2, 1, 1}, {2, 1, 1}} {
		if err := loop.step(context.Background(), clock.Now()); err != nil {
			t.Fatal(err)
		}
		if changed, failed, redriven := counts(); changed != want[0] || failed != want[1] || redriven != want[2] {
			t.Fatalf("after pass %d: %d level WARNs, %d failure WARNs, %d redriven INFOs; want %v\n%s", pass+1, changed, failed, redriven, want, buf.String())
		}
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_stale_finalize_redriven_total 1",
		"worker_outbox_reconciler_stale_finalize_errors_total 1",
		"worker_outbox_reconciler_stale_finalize_deferred_total 3",
		"worker_outbox_reconciler_stale_finalize_standing_no_partitions 1",
		"worker_outbox_reconciler_stale_finalize_standing_strand_shape 2",
		"worker_outbox_reconciler_stale_finalize_standing_recent 0",
		"worker_outbox_reconciler_stale_finalize_standing_live 0",
		"worker_outbox_reconciler_stale_finalize_standing_truncated 0",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
}

// stallingRollbackTx fails the survey's first statement, then stalls its
// rollback until the rollback's own context ends, as a connection that went
// silent at that instant would.
type stallingRollbackTx struct {
	pgx.Tx
	rollbackErr chan error
}

func (*stallingRollbackTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, ErrUnavailable
}

func (tx *stallingRollbackTx) Rollback(ctx context.Context) error {
	<-ctx.Done()
	tx.rollbackErr <- ctx.Err()
	return ctx.Err()
}

// R1: the survey's rollback has a deadline of its own, so a stalled rollback
// ends instead of holding the relay step. Without one, the rollback below
// waits for ever and the survey never returns.
func TestStaleFinalizeSurveyRollbackEndsOnItsOwnDeadline(t *testing.T) {
	tx := &stallingRollbackTx{rollbackErr: make(chan error, 1)}
	survey := surveyStaleFinalize(context.Background(),
		func(context.Context) (pgx.Tx, error) { return tx, nil },
		"SELECT 1", "SELECT 1", staleFinalizeTestNow, 10, nil)
	if err := <-tx.rollbackErr; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rollback ended with %v, want its own deadline", err)
	}
	want := []StaleFinalizeFailure{{Step: StaleFinalizeStepSurvey, Class: StaleFinalizeClassUnavailable}}
	if !reflect.DeepEqual(survey.failures, want) || survey.candidates != nil {
		t.Fatalf("survey = %+v, want one survey failure and no candidates", survey)
	}
}
