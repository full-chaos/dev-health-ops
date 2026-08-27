package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type materializerExec struct {
	sql  string
	args []any
}

type fakeMaterializerTx struct {
	pgx.Tx
	affected []int64
	failAt   int
	// failErrs overrides the generic injected failure at a given 1-based
	// exec index (matching failAt) with a specific error -- used to plant a
	// *pgconn.PgError and prove its SQLSTATE survives into the step error.
	failErrs  map[int]error
	execs     []materializerExec
	committed bool
	rolled    bool
	// runaway is what the CHAOS-4097 report read returns. The embedded nil
	// pgx.Tx would panic on Query, so the fake answers it explicitly: an
	// empty result is the healthy pass every pre-existing case here asserts.
	runaway []RunawayDispatchWakeup
	// runawayTotal lets a test make the SAMPLE smaller than the TRUE count,
	// which is the only way to catch a gauge fed from len(sample).
	runawayTotal int64
	runawayQuery string
	// Each of the three report statements gets its own injectable failure.
	// One shared "make it fail" switch would let a fix that only handled the
	// query error still pass, which is the shape of coverage this ticket is
	// about.
	failQuery    bool
	failScan     bool
	failRowsIter bool
	// discoveryReturning is what materializeDiscoverySQL's RETURNING clause
	// answers with (CHAOS-4357 round 2, codex P2): one bool per row the
	// statement touched, true iff that row was the stale-dispatched
	// recovery shape. nil (the default) is the healthy "nothing touched"
	// case every pre-existing case here asserts, matching runaway above.
	// Routed through the SAME Query method as the runaway report -- the two
	// statements are told apart by SQL text (materializeDiscoverySQL is the
	// only one with a RETURNING clause), the same way a real driver would
	// dispatch on the prepared statement, not on caller intent.
	discoveryReturning    []bool
	failDiscoveryQuery    bool
	failDiscoveryScan     bool
	failDiscoveryRowsIter bool
	discoveryQuery        string
	discoveryArgs         []any
}

func (tx *fakeMaterializerTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	// materializeDiscoverySQL is the only Query call with a RETURNING
	// clause; the runaway report is a plain SELECT. A real driver
	// dispatches on the prepared statement text the same way.
	if strings.Contains(sql, "RETURNING") {
		tx.discoveryQuery = sql
		tx.discoveryArgs = append([]any(nil), args...)
		if tx.failDiscoveryQuery {
			return nil, errors.New("injected discovery materialization query failure")
		}
		return &fakeDiscoveryRows{
			recovered: tx.discoveryReturning,
			failScan:  tx.failDiscoveryScan, failIter: tx.failDiscoveryRowsIter,
		}, nil
	}
	tx.runawayQuery = sql
	if tx.failQuery {
		return nil, errors.New("injected runaway report query failure")
	}
	total := tx.runawayTotal
	if total == 0 {
		total = int64(len(tx.runaway))
	}
	return &fakeRunawayRows{
		rows: tx.runaway, total: total,
		failScan: tx.failScan, failIter: tx.failRowsIter,
	}, nil
}

type fakeDiscoveryRows struct {
	pgx.Rows
	recovered []bool
	index     int
	failScan  bool
	failIter  bool
}

func (rows *fakeDiscoveryRows) Next() bool {
	rows.index++
	return rows.index <= len(rows.recovered)
}

func (rows *fakeDiscoveryRows) Scan(dest ...any) error {
	if rows.failScan {
		return errors.New("injected discovery materialization scan failure")
	}
	target, ok := dest[0].(*bool)
	if len(dest) != 1 || !ok {
		return errors.New("unexpected discovery materialization projection")
	}
	*target = rows.recovered[rows.index-1]
	return nil
}

func (rows *fakeDiscoveryRows) Err() error {
	if rows.failIter {
		return errors.New("injected discovery materialization iteration failure")
	}
	return nil
}

func (rows *fakeDiscoveryRows) Close() {}

type fakeRunawayRows struct {
	pgx.Rows
	rows     []RunawayDispatchWakeup
	total    int64
	index    int
	failScan bool
	failIter bool
}

func (rows *fakeRunawayRows) Next() bool {
	rows.index++
	return rows.index <= len(rows.rows)
}

func (rows *fakeRunawayRows) Scan(dest ...any) error {
	if rows.failScan {
		return errors.New("injected runaway report scan failure")
	}
	if len(dest) != 3 {
		return errors.New("unexpected runaway projection")
	}
	current := rows.rows[rows.index-1]
	id, ok := dest[0].(*string)
	if !ok {
		return errors.New("unexpected runaway sync_run_id target")
	}
	attempts, ok := dest[1].(*int64)
	if !ok {
		return errors.New("unexpected runaway attempts target")
	}
	total, ok := dest[2].(*int64)
	if !ok {
		return errors.New("unexpected runaway total target")
	}
	*id, *attempts = current.SyncRunID, current.Attempts
	// The window function repeats the FULL matching count on every row, which
	// is exactly the property the gauge depends on, so the fake reproduces it
	// rather than echoing len(rows).
	*total = rows.total
	return nil
}

func (rows *fakeRunawayRows) Err() error {
	if rows.failIter {
		return errors.New("injected runaway report iteration failure")
	}
	return nil
}

func (rows *fakeRunawayRows) Close() {}

func (tx *fakeMaterializerTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, materializerExec{sql: sql, args: append([]any(nil), args...)})
	if tx.failAt > 0 && len(tx.execs) == tx.failAt {
		if customErr, ok := tx.failErrs[tx.failAt]; ok {
			return pgconn.CommandTag{}, customErr
		}
		return pgconn.CommandTag{}, errors.New("injected materializer statement failure")
	}
	index := len(tx.execs) - 1
	affected := int64(0)
	if index < len(tx.affected) {
		affected = tx.affected[index]
	}
	return pgconn.NewCommandTag("INSERT 0 " + strconv.FormatInt(affected, 10)), nil
}

func (tx *fakeMaterializerTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (tx *fakeMaterializerTx) Rollback(context.Context) error {
	tx.rolled = true
	return nil
}

func TestMaterializerRunsOneBoundedTransportNeutralTransaction(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	cutoff := now.Add(-15 * time.Minute)
	// CHAOS-4357 round 2: discovery no longer runs through Exec (it RETURNs
	// per-row recovery flags, so it goes through Query instead) -- execs now
	// covers JIT-disable, dispatch, finalize, post_sync in that order.
	tx := &fakeMaterializerTx{
		affected:           []int64{0, 2, 1, 1},
		discoveryReturning: []bool{true, false},
	}
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
		return tx, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := materializer.Step(context.Background(), now, cutoff, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, MaterializerResult{Dispatch: 2, Finalize: 1, Discovery: 2, DiscoveryRearmed: 1, PostSync: 1}) {
		t.Fatalf("Step() result = %#v", result)
	}
	if !tx.committed || !tx.rolled || len(tx.execs) != 4 {
		t.Fatalf("transaction = committed:%t rolled:%t execs:%d", tx.committed, tx.rolled, len(tx.execs))
	}

	// CHAOS-4262: the transaction's very first statement disables JIT before
	// any materialization statement runs, and takes no arguments.
	if got := strings.TrimSpace(tx.execs[0].sql); got != "SET LOCAL jit = off" {
		t.Fatalf("first statement = %q, want the JIT-disable SET LOCAL", got)
	}
	if len(tx.execs[0].args) != 0 {
		t.Fatalf("JIT-disable statement carried arguments: %#v", tx.execs[0].args)
	}

	wantArgs := [][]any{
		{now, cutoff, 2},
		{now, 2},
		{now, 2},
	}
	for index, execution := range tx.execs[1:] {
		if !reflect.DeepEqual(execution.args, wantArgs[index]) {
			t.Fatalf("statement %d arguments = %#v, want %#v", index, execution.args, wantArgs[index])
		}
		upper := strings.ToUpper(execution.sql)
		for _, forbidden := range []string{
			"SYNC_DISPATCH_TRANSPORT_ROUTES",
			"CLAIM_TOKEN = GEN_RANDOM_UUID()",
			"ATTEMPTS = SYNC_DISPATCH_OUTBOX.ATTEMPTS + 1",
		} {
			if strings.Contains(upper, forbidden) {
				t.Fatalf("statement %d crosses transport boundary %q:\n%s", index, forbidden, execution.sql)
			}
		}
		if !strings.Contains(upper, "LIMIT $") ||
			!strings.Contains(upper, "ON CONFLICT (SYNC_RUN_ID, KIND)") {
			t.Fatalf("statement %d is not bounded/idempotent:\n%s", index, execution.sql)
		}
	}
	rearmSQL := strings.ToUpper(tx.execs[1].sql)
	for _, required := range []string{
		"EXCLUDED.AVAILABLE_AT < SYNC_DISPATCH_OUTBOX.AVAILABLE_AT",
		"SYNC_DISPATCH_OUTBOX.CLAIM_EXPIRES_AT > $1",
		"LAST_ERROR IS DISTINCT FROM 'FEATURE_DISABLED'",
		"DISPATCHED_TRANSPORT IS DISTINCT FROM 'RIVER'",
		"UNIT.STATUS = 'DISPATCHING'",
		"UNIT.STATUS = 'RETRYING'",
		"CLAIM_ROUTE_GENERATION = CASE",
		"DISPATCHED_ROUTE_GENERATION = CASE",
		"WHERE SYNC_DISPATCH_OUTBOX.STATUS <> 'PENDING'",
	} {
		if !strings.Contains(rearmSQL, required) {
			t.Fatalf("rearm SQL missing %q:\n%s", required, tx.execs[1].sql)
		}
	}
	postSyncUpper := strings.ToUpper(tx.execs[3].sql)
	if !strings.Contains(postSyncUpper, "LEFT JOIN PUBLIC.SYNC_DISPATCH_OUTBOX") ||
		!strings.Contains(postSyncUpper, "OUTBOX.ID IS NULL") ||
		!strings.Contains(postSyncUpper, "DO NOTHING") ||
		strings.Contains(postSyncUpper, "DO UPDATE") {
		t.Fatalf("post_sync must remain insert-only:\n%s", tx.execs[3].sql)
	}

	// CHAOS-4357 round 2 (codex P1): the discovery statement's own
	// stale-dispatched re-arm eligibility is gated by staleDispatchCutoff,
	// the same grace-period argument dispatch's own rearm uses, not just
	// now/limit.
	if !reflect.DeepEqual(tx.discoveryArgs, []any{now, 2, cutoff}) {
		t.Fatalf("discovery query args = %#v, want [now, limit, cutoff]", tx.discoveryArgs)
	}
	discoveryUpper := strings.ToUpper(tx.discoveryQuery)
	for _, required := range []string{
		"RETURNING",
		"STALE_DISPATCHED",
		"OUTBOX.DISPATCHED_AT <= $3",
		"LAST_ERROR IS DISTINCT FROM 'FEATURE_DISABLED'",
	} {
		if !strings.Contains(discoveryUpper, required) {
			t.Fatalf("discovery SQL missing %q:\n%s", required, tx.discoveryQuery)
		}
	}
}

func TestMaterializerStatementFailureRollsBackWholeStep(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	// materializeFailAt names which of the three Exec-based materialization
	// statements fails (1=dispatch .. 3=post_sync); the JIT-disable SET
	// LOCAL always runs first, so the actual failing exec index is
	// materializeFailAt+1. Discovery is no longer one of these -- it runs
	// through Query (RETURNING), not Exec (CHAOS-4357 round 2); its own
	// failure modes are covered by TestMaterializerDiscoveryQueryFailure*
	// below, mirroring the runaway report's three-part fault injection.
	wantSteps := []string{
		materializerStepDispatch,
		materializerStepFinalize,
		materializerStepPostSync,
	}
	for materializeFailAt := 1; materializeFailAt <= 3; materializeFailAt++ {
		t.Run(strconv.Itoa(materializeFailAt), func(t *testing.T) {
			tx := &fakeMaterializerTx{affected: []int64{0, 1, 1, 1}, failAt: materializeFailAt + 1}
			materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
				return tx, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := materializer.Step(context.Background(), now, now.Add(-time.Minute), 1)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Step() error = %v", err)
			}
			if wantStep := wantSteps[materializeFailAt-1]; MaterializerStepIdentity(err) != wantStep {
				t.Fatalf("MaterializerStepIdentity(err) = %q, want %q", MaterializerStepIdentity(err), wantStep)
			}
			if !reflect.DeepEqual(result, MaterializerResult{}) || tx.committed || !tx.rolled || len(tx.execs) != materializeFailAt+1 {
				t.Fatalf("failed step = result:%#v committed:%t rolled:%t execs:%d",
					result, tx.committed, tx.rolled, len(tx.execs))
			}
		})
	}
}

// TestMaterializerDiscoveryQueryFailureRollsBackWholeStep covers discovery's
// own three-part fault injection (CHAOS-4357 round 2), the same discipline
// TestMaterializerReportsWhenTheRunawayReportItselfFails already applies to
// the runaway report's own Query call: a shared "make it fail" switch would
// let a fix that only handled the query error still pass, which is exactly
// the coverage gap this ticket is about.
func TestMaterializerDiscoveryQueryFailureRollsBackWholeStep(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		fault func(*fakeMaterializerTx)
	}{
		{"query fails", func(tx *fakeMaterializerTx) { tx.failDiscoveryQuery = true }},
		{"scan fails", func(tx *fakeMaterializerTx) {
			tx.discoveryReturning = []bool{true}
			tx.failDiscoveryScan = true
		}},
		{"rows iteration fails", func(tx *fakeMaterializerTx) {
			tx.discoveryReturning = []bool{true}
			tx.failDiscoveryRowsIter = true
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tx := &fakeMaterializerTx{affected: []int64{0, 1, 1, 1}}
			testCase.fault(tx)
			materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
				return tx, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := materializer.Step(context.Background(), now, now.Add(-time.Minute), 1)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Step() error = %v", err)
			}
			if got := MaterializerStepIdentity(err); got != materializerStepDiscovery {
				t.Fatalf("MaterializerStepIdentity(err) = %q, want %q", got, materializerStepDiscovery)
			}
			// dispatch and finalize already committed their execs before
			// discovery's Query runs; only the transaction-level rollback
			// undoes them, which is what committed/rolled asserts.
			if !reflect.DeepEqual(result, MaterializerResult{}) || tx.committed || !tx.rolled {
				t.Fatalf("failed step = result:%#v committed:%t rolled:%t",
					result, tx.committed, tx.rolled)
			}
		})
	}
}

// CHAOS-4262: the JIT-disable statement itself is as fallible as any other
// exec on the transaction, and its failure must roll back cleanly and report
// its own step identity rather than being folded into the dispatch step.
func TestMaterializerJITDisableFailureRollsBackWholeStep(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	tx := &fakeMaterializerTx{failAt: 1}
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
		return tx, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := materializer.Step(context.Background(), now, now.Add(-time.Minute), 1)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Step() error = %v", err)
	}
	if got := MaterializerStepIdentity(err); got != materializerStepDisableJIT {
		t.Fatalf("MaterializerStepIdentity(err) = %q, want %q", got, materializerStepDisableJIT)
	}
	if !reflect.DeepEqual(result, MaterializerResult{}) || tx.committed || !tx.rolled || len(tx.execs) != 1 {
		t.Fatalf("failed step = result:%#v committed:%t rolled:%t execs:%d",
			result, tx.committed, tx.rolled, len(tx.execs))
	}
}

// CHAOS-4262: a canceled statement's SQLSTATE must survive into the returned
// error so a caller (pipeline telemetry) can tell a real 57014 cancellation
// apart from every other cause folded into the same ErrUnavailable
// classification -- without ever seeing the driver's own error text.
func TestMaterializerStepErrorCarriesSQLState(t *testing.T) {
	tx := &fakeMaterializerTx{
		failAt:   2,
		failErrs: map[int]error{2: &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"}},
	}
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) { return tx, nil })
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	_, err = materializer.Step(context.Background(), now, now.Add(-time.Minute), 1)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Step() error = %v", err)
	}
	if got := MaterializerStepSQLState(err); got != "57014" {
		t.Fatalf("MaterializerStepSQLState(err) = %q, want 57014", got)
	}
	if got := MaterializerStepIdentity(err); got != materializerStepDispatch {
		t.Fatalf("MaterializerStepIdentity(err) = %q, want %q", got, materializerStepDispatch)
	}
	// The driver's own message must never reach the returned error text.
	if strings.Contains(err.Error(), "canceling statement") {
		t.Fatalf("Step() error leaked driver text: %v", err)
	}
}

// TestMaterializerStepErrorClassifiesStageContextDeadline pins the CHAOS-4262
// codex review finding: pgx v5's context watcher answers a canceled stage
// context by tearing the connection down client-side and returning a
// wrapped context.DeadlineExceeded, NOT a *pgconn.PgError -- confirmed
// empirically against a real server, the exact cancellation that makes
// Postgres log "canceling statement due to user request" (sqlstate 57014)
// reaches the Go caller as a bare context.DeadlineExceeded. Since runStage
// bounds every stage with exactly this kind of context.WithTimeout, this is
// the ACTUAL production failure shape, not the server-side statement_timeout
// TestMaterializerStepErrorCarriesSQLState above exercises. Without this
// classification, dev_health_reconciler_stage_cancelled_total stays silent
// on the one failure it was built to surface.
func TestMaterializerStepErrorClassifiesStageContextDeadline(t *testing.T) {
	tx := &fakeMaterializerTx{
		failAt:   2,
		failErrs: map[int]error{2: fmt.Errorf("timeout: %w", context.DeadlineExceeded)},
	}
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) { return tx, nil })
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	_, err = materializer.Step(context.Background(), now, now.Add(-time.Minute), 1)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Step() error = %v", err)
	}
	if got := MaterializerStepSQLState(err); got != stageContextDeadlineLabel {
		t.Fatalf("MaterializerStepSQLState(err) = %q, want %q", got, stageContextDeadlineLabel)
	}
	if got := MaterializerStepIdentity(err); got != materializerStepDispatch {
		t.Fatalf("MaterializerStepIdentity(err) = %q, want %q", got, materializerStepDispatch)
	}
}

func TestMaterializerRejectsInvalidBoundariesBeforeOpeningTransaction(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	begins := 0
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
		begins++
		return &fakeMaterializerTx{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		ctx    context.Context
		now    time.Time
		cutoff time.Time
		limit  int
	}{
		{"nil context", nil, now, now.Add(-time.Minute), 1},
		{"zero now", context.Background(), time.Time{}, now.Add(-time.Minute), 1},
		{"zero cutoff", context.Background(), now, time.Time{}, 1},
		{"future cutoff", context.Background(), now, now.Add(time.Second), 1},
		{"zero limit", context.Background(), now, now.Add(-time.Minute), 0},
		{"over limit", context.Background(), now, now.Add(-time.Minute), maximumStepLimit + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := materializer.Step(test.ctx, test.now, test.cutoff, test.limit); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("Step() error = %v", err)
			}
		})
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := materializer.Step(cancelled, now, now.Add(-time.Minute), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Step() error = %v", err)
	}
	if begins != 0 {
		t.Fatalf("invalid calls opened %d transactions", begins)
	}
}

// A DETECTOR THAT BREAKS MUST NOT REPORT CLEAN (adversarial review finding).
//
// An earlier cut swallowed the report's error so a read fault could not lose
// the materialization. The instinct was right and the trade was wrong: an
// empty Runaway then means either "no run is looping" or "the statement that
// would have told you did not run", and those demand opposite responses from
// an operator. A permission or schema fault would have reproduced exactly the
// silence CHAOS-4097 exists to end.
//
// All three statements are covered separately, because a fix that handled only
// the query error would pass a single shared case.
func TestMaterializerReportsWhenTheRunawayReportItselfFails(t *testing.T) {
	now := time.Date(2026, time.August, 22, 18, 0, 0, 0, time.UTC)
	cutoff := now.Add(-15 * time.Minute)
	for _, testCase := range []struct {
		name     string
		mutate   func(*fakeMaterializerTx)
		wantStep string
	}{
		{"query fails", func(tx *fakeMaterializerTx) { tx.failQuery = true }, runawayReportStepQuery},
		{"scan fails", func(tx *fakeMaterializerTx) {
			tx.runaway = []RunawayDispatchWakeup{{SyncRunID: "run", Attempts: 5000}}
			tx.failScan = true
		}, runawayReportStepScan},
		{"iteration fails", func(tx *fakeMaterializerTx) { tx.failRowsIter = true }, runawayReportStepRows},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tx := &fakeMaterializerTx{affected: []int64{0, 1, 0, 0, 0}}
			testCase.mutate(tx)
			materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
				return tx, nil
			})
			if err != nil {
				t.Fatal(err)
			}

			result, err := materializer.Step(context.Background(), now, cutoff, 2)
			// The materialization must SURVIVE a broken report -- losing a
			// dispatch wakeup because a diagnostic read failed would be a
			// worse trade than the silence.
			if err != nil {
				t.Fatalf("a failed report took the materialization down with it: %v", err)
			}
			if !tx.committed || result.Dispatch != 1 {
				t.Fatalf("materialization did not commit: committed=%t result=%#v", tx.committed, result)
			}
			// ...and it must SAY it failed.
			if result.RunawayReportStep != testCase.wantStep {
				t.Fatalf("RunawayReportStep = %q, want %q: an empty report that does "+
					"not admit it failed is indistinguishable from a healthy one",
					result.RunawayReportStep, testCase.wantStep)
			}
			if len(result.Runaway) != 0 || result.RunawayTruncated {
				t.Fatalf("a failed report returned findings: %#v", result)
			}
		})
	}
}

// NON-VACUITY: a healthy pass must leave the step empty, or the pipeline would
// log a broken detector on every tick and the signal would be worthless.
func TestMaterializerLeavesTheReportStepEmptyOnAHealthyPass(t *testing.T) {
	now := time.Date(2026, time.August, 22, 18, 0, 0, 0, time.UTC)
	tx := &fakeMaterializerTx{affected: []int64{0, 1, 0, 0, 0}}
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) { return tx, nil })
	if err != nil {
		t.Fatal(err)
	}
	result, err := materializer.Step(context.Background(), now, now.Add(-time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.RunawayReportStep != "" {
		t.Fatalf("RunawayReportStep = %q on a healthy pass", result.RunawayReportStep)
	}
}

// THE GAUGE MUST NOT BE THE SAMPLE SIZE (adversarial review finding).
//
// The report is deliberately capped at runawayDispatchScan rows so one pass
// cannot emit an unbounded burst of log lines. Feeding the metric from that
// capped slice would have reported 20 for CHAOS-4093's 83 stuck runs — an
// incident more than four times the size the dashboard showed. Understating
// scope is the specific way a scope metric fails, and it fails silently.
//
// The fake supplies a true count LARGER than the sample precisely so a
// regression to len(Runaway) cannot pass.
func TestMaterializerReportsTheExactRunawayTotalNotTheSampleSize(t *testing.T) {
	now := time.Date(2026, time.August, 22, 18, 0, 0, 0, time.UTC)
	sample := make([]RunawayDispatchWakeup, 0, runawayDispatchScan+1)
	for index := 0; index < runawayDispatchScan+1; index++ {
		sample = append(sample, RunawayDispatchWakeup{
			SyncRunID: fmt.Sprintf("run-%02d", index),
			Attempts:  int64(runawayDispatchAttempts + index),
		})
	}
	tx := &fakeMaterializerTx{
		affected: []int64{0, 1, 0, 0, 0},
		runaway:  sample,
		// 83 is CHAOS-4093's real stuck-run count.
		runawayTotal: 83,
	}
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) { return tx, nil })
	if err != nil {
		t.Fatal(err)
	}
	result, err := materializer.Step(context.Background(), now, now.Add(-time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.RunawayTotal != 83 {
		t.Fatalf("RunawayTotal = %d, want the exact 83 over the threshold; a gauge "+
			"fed from the capped sample would report %d and understate the incident",
			result.RunawayTotal, len(result.Runaway))
	}
	// The sample stays capped — the total is what grows, not the log line.
	if len(result.Runaway) != runawayDispatchScan || !result.RunawayTruncated {
		t.Fatalf("sample = %d rows truncated=%t, want %d and a truncation flag",
			len(result.Runaway), result.RunawayTruncated, runawayDispatchScan)
	}
}
