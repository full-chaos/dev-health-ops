package syncreconciler

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeLeaseRepairRows struct {
	pgx.Rows
	candidates []expiredLeaseCandidate
	index      int
	err        error
}

// wantLinearExpiredLeaseRetrySurfaces is an independent recovery acceptance
// oracle. Production selects its list from workitemcontract; keeping this
// literal here ensures removing a retry-safety tag cannot be hidden by changing
// both producer and expectation together.
func wantLinearExpiredLeaseRetrySurfaces() []string {
	return []string{
		"ai_attribution", "estimate_coverage_metrics_daily",
		"investment_classifications_daily", "investment_metrics_daily",
		"issue_type_metrics_daily", "sprints", "work_item_cycle_times",
		"work_item_dependencies", "work_item_interactions",
		"work_item_metrics_daily", "work_item_reopen_events",
		"work_item_state_durations_daily", "work_item_team_attributions",
		"work_item_transitions", "work_item_user_metrics_daily", "work_items",
	}
}

func (rows *fakeLeaseRepairRows) Next() bool { return rows.index < len(rows.candidates) }

func (rows *fakeLeaseRepairRows) Scan(dest ...any) error {
	if rows.index >= len(rows.candidates) {
		return errors.New("scan past rows")
	}
	candidate := rows.candidates[rows.index]
	rows.index++
	values := []any{
		candidate.id,
		candidate.syncRunID,
		candidate.leaseOwner,
		candidate.provider,
		candidate.mode,
		candidate.datasetKey,
		candidate.orgID,
		candidate.costClass,
		candidate.retryCount,
	}
	for index, destination := range dest {
		switch typed := destination.(type) {
		case *string:
			*typed = values[index].(string)
		case *int64:
			*typed = values[index].(int64)
		default:
			return errors.New("unexpected scan destination")
		}
	}
	return nil
}

func (rows *fakeLeaseRepairRows) Err() error { return rows.err }
func (*fakeLeaseRepairRows) Close()          {}

type fakeLeaseRepairTx struct {
	pgx.Tx
	candidates  []expiredLeaseCandidate
	querySQL    string
	queryArgs   []any
	queryRowSQL []string
	execSQL     []string
	execArgs    [][]any
	affected    []int64
	execErr     error
	commitErr   error
	commit      bool
	rollback    bool
	// callLog is a SINGLE ordered log spanning both Exec and QueryRow calls
	// (unlike execSQL/queryRowSQL above, which are each internally ordered
	// but say nothing about how the two interleave). CHAOS-4586 round 3
	// pins the lock-order contract on this: every unit row write
	// ("exec:unit:...") must precede its own candidate's run lock
	// ("queryrow:runlock:...", from syncrunrollup.Bump), matching every
	// single-run terminal writer elsewhere, and across candidates the run
	// locks must land in ascending sync_run_id order regardless of the
	// candidates' own input order -- see
	// TestLeaseRepairStepLocksUnitsBeforeRunsAndVisitsRunsInAscendingOrder.
	callLog []string
}

func (tx *fakeLeaseRepairTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.querySQL = sql
	tx.queryArgs = args
	return &fakeLeaseRepairRows{candidates: append([]expiredLeaseCandidate(nil), tx.candidates...)}, nil
}

func (tx *fakeLeaseRepairTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execSQL = append(tx.execSQL, sql)
	tx.execArgs = append(tx.execArgs, args)
	switch {
	case strings.Contains(sql, "pg_advisory_xact_lock"):
		tx.callLog = append(tx.callLog, "exec:advisory")
	case len(args) > 0:
		if id, ok := args[0].(string); ok {
			tx.callLog = append(tx.callLog, "exec:unit:"+id)
		} else {
			tx.callLog = append(tx.callLog, "exec:unit")
		}
	default:
		tx.callLog = append(tx.callLog, "exec:unit")
	}
	if tx.execErr != nil {
		return pgconn.CommandTag{}, tx.execErr
	}
	index := len(tx.execSQL) - 1
	affected := int64(1)
	if index < len(tx.affected) {
		affected = tx.affected[index]
	}
	return pgconn.NewCommandTag("UPDATE " + strconv.FormatInt(affected, 10)), nil
}

// fakeLeaseRepairRow stubs the two QueryRow calls syncrunrollup.Bump makes
// (CHAOS-4586): the lock-first `SELECT 1 ... FOR UPDATE` (an int) and the
// rollup recompute's RETURNING (three ints). Filling every *int destination
// with 0 is sufficient for this fake -- no test here asserts on the
// resulting rollup counters, only that Bump does not fail the Step.
type fakeLeaseRepairRow struct{}

func (*fakeLeaseRepairRow) Scan(dest ...any) error {
	for _, destination := range dest {
		if typed, ok := destination.(*int); ok {
			*typed = 0
		}
	}
	return nil
}

func (tx *fakeLeaseRepairTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.queryRowSQL = append(tx.queryRowSQL, sql)
	kind := "recompute"
	switch {
	case strings.Contains(sql, "public.sync_run_units") && strings.Contains(sql, "FOR UPDATE"):
		// lockSyncRunUnitsAscending's pre-lock (codex round 4, P1) -- distinct
		// from the run lock below despite both being a bare "SELECT 1 ...
		// FOR UPDATE": this one targets sync_run_units, not sync_runs.
		kind = "unitlock"
	case strings.Contains(sql, "public.sync_runs") && strings.Contains(sql, "FOR UPDATE"):
		kind = "runlock"
	}
	if len(args) > 0 {
		if id, ok := args[0].(string); ok {
			tx.callLog = append(tx.callLog, "queryrow:"+kind+":"+id)
		} else {
			tx.callLog = append(tx.callLog, "queryrow:"+kind)
		}
	} else {
		tx.callLog = append(tx.callLog, "queryrow:"+kind)
	}
	return &fakeLeaseRepairRow{}
}

func (tx *fakeLeaseRepairTx) Commit(context.Context) error {
	tx.commit = true
	return tx.commitErr
}

func (tx *fakeLeaseRepairTx) Rollback(context.Context) error {
	tx.rollback = true
	return nil
}

func repairCandidate(provider, mode, dataset string, retries int64) expiredLeaseCandidate {
	return expiredLeaseCandidate{
		id:         candidateID1,
		syncRunID:  "00000000-0000-4000-8000-000000003301",
		leaseOwner: "worker-a",
		provider:   provider,
		mode:       mode,
		datasetKey: dataset,
		orgID:      "org-a",
		costClass:  "standard",
		retryCount: retries,
	}
}

func TestExpiredLeaseDecisionIsFailClosedOutsideExactLinearBackfillSurface(t *testing.T) {
	tests := []struct {
		name      string
		candidate expiredLeaseCandidate
		want      expiredLeaseDecision
	}{
		{"eligible below ceiling", repairCandidate("linear", "backfill", "work_items", 0), expiredLeaseDecision{retry: true}},
		{"eligible at ceiling", repairCandidate("linear", "backfill", "work_items", 1), expiredLeaseDecision{exhausted: true}},
		{"wrong provider", repairCandidate("github", "backfill", "work_items", 0), expiredLeaseDecision{}},
		{"wrong mode", repairCandidate("linear", "incremental", "work_items", 0), expiredLeaseDecision{}},
		{"wrong dataset", repairCandidate("linear", "backfill", "repositories", 0), expiredLeaseDecision{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := decideExpiredLeaseRepair(test.candidate, DefaultLeaseRepairConfig()); got != test.want {
				t.Fatalf("decideExpiredLeaseRepair() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestLeaseRepairConfigDefaultsAndBucketHashMatchPythonContract(t *testing.T) {
	defaults := DefaultLeaseRepairConfig()
	if defaults.MaximumRetries != 1 || defaults.RetryBackoff != time.Minute || !defaults.valid() {
		t.Fatalf("default config = %#v", defaults)
	}
	if leaseRepairBucketAdvisoryID("org-a", "linear", "standard") != 3882165252103971925 {
		t.Fatalf("bucket advisory id diverged from Python SHA-256 contract")
	}
	if _, err := newLeaseRepairWithConfig(func(context.Context) (pgx.Tx, error) { return nil, nil }, LeaseRepairConfig{MaximumRetries: -1}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("negative retry config error = %v", err)
	}
	if got := decideExpiredLeaseRepair(repairCandidate("linear", "backfill", "work_items", 2), LeaseRepairConfig{MaximumRetries: 3}); !got.retry || got.exhausted {
		t.Fatalf("operator config decision = %#v", got)
	}
}

func TestLeaseRepairStepUsesCASAndRollsBackOnFault(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeLeaseRepairTx{
		candidates: []expiredLeaseCandidate{
			repairCandidate("linear", "backfill", "work_items", 0),
			func() expiredLeaseCandidate {
				candidate := repairCandidate("github", "backfill", "work_items", 0)
				candidate.id = candidateID2
				return candidate
			}(),
		},
	}
	repair, err := newLeaseRepair(func(context.Context) (pgx.Tx, error) { return tx, nil })
	if err != nil {
		t.Fatal(err)
	}
	result, err := repair.Step(context.Background(), now, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result != (LeaseRepairResult{Selected: 2, Retried: 1, Failed: 1}) || !tx.commit {
		t.Fatalf("Step() = %#v, commit=%t", result, tx.commit)
	}
	upperSelect := strings.ToUpper(tx.querySQL)
	for _, required := range []string{
		"RUN.STATUS NOT IN ('SUCCESS', 'PARTIAL_FAILED', 'FAILED')",
		"RUN.ORG_ID = UNIT.ORG_ID",
		"ORDER BY UNIT.LEASE_EXPIRES_AT, UNIT.ID",
		"LIMIT $2",
	} {
		if !strings.Contains(upperSelect, required) {
			t.Fatalf("selection SQL missing %q:\n%s", required, tx.querySQL)
		}
	}
	if len(tx.execSQL) != 4 || tx.execSQL[0] != "SELECT pg_advisory_xact_lock($1)" ||
		tx.execSQL[1] != "SELECT pg_advisory_xact_lock($1)" ||
		!strings.Contains(tx.execSQL[2], "rate_limit_deferrals = 0") ||
		!strings.Contains(tx.execSQL[2], "unit.lease_owner = $2") ||
		!strings.Contains(tx.execSQL[3], "'error_category', $4::text") {
		t.Fatalf("write SQL = %v", tx.execSQL)
	}
	if got, want := tx.execArgs[2][4], wantLinearExpiredLeaseRetrySurfaces(); !reflect.DeepEqual(got, want) {
		t.Fatalf("retry surfaces = %#v, want %#v", got, want)
	}

	faultTx := &fakeLeaseRepairTx{candidates: []expiredLeaseCandidate{repairCandidate("linear", "backfill", "work_items", 0)}, execErr: errors.New("injected write fault")}
	faultRepair, err := newLeaseRepair(func(context.Context) (pgx.Tx, error) { return faultTx, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := faultRepair.Step(context.Background(), now, 1); !errors.Is(err, ErrUnavailable) || faultTx.commit || !faultTx.rollback {
		t.Fatalf("fault Step() err=%v commit=%t rollback=%t", err, faultTx.commit, faultTx.rollback)
	}
}

// leaseCandidateForRun builds a candidate ineligible for the Linear-backfill
// retry surface (provider "github"), so decideExpiredLeaseRepair always
// takes the fail branch: one Exec (the unit write) followed by one Bump
// call (two QueryRows: the run lock, then the recompute).
func leaseCandidateForRun(unitID, syncRunID string) expiredLeaseCandidate {
	return expiredLeaseCandidate{
		id:         unitID,
		syncRunID:  syncRunID,
		leaseOwner: "worker-a",
		provider:   "github",
		mode:       "backfill",
		datasetKey: "work_items",
		orgID:      "org-a",
		costClass:  "standard",
		retryCount: 0,
	}
}

// TestLeaseRepairStepLocksUnitsBeforeRunsAndVisitsRunsInAscendingOrder pins
// the lock-order contract codex found two successive violations of
// (CHAOS-4586):
//
//   - Round 3: round 2's fix pre-locked every candidate's run before any
//     unit write, inverting the order against every single-run terminal
//     writer elsewhere (which all lock their sync_run_units row first, then
//     call syncrunrollup.Bump) -- an ABBA deadlock waiting to happen. Fixed
//     by sorting candidates by ascending sync_run_id and keeping the
//     existing unit-write-then-Bump sequence per candidate.
//   - Round 4: that per-candidate sequence still lets a SECOND candidate in
//     the SAME run write its unit row WHILE this transaction already holds
//     that run's lock (from the first candidate's Bump call, which holds
//     the lock for the rest of the transaction) -- run-before-unit for that
//     one candidate, the same inversion round 3 fixed, just reachable a
//     different way. Fixed by lockSyncRunUnitsAscending: every candidate's
//     unit row locked up front, before this transaction ever touches a run
//     lock.
//
// This test feeds FOUR candidates spanning three runs -- one run
// (...003) has TWO candidates, to actually exercise the round-4 scenario,
// not just the round-3 one -- in a deliberately scrambled order, and
// asserts every half of the contract from the real interleaved call
// sequence, not from inspecting source text: ALL unit locks land first, in
// ascending unit-id order, before any write or run lock; every candidate's
// own unit write still precedes its own run lock; and the run locks never
// regress to an EARLIER run than one already visited.
func TestLeaseRepairStepLocksUnitsBeforeRunsAndVisitsRunsInAscendingOrder(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	const (
		run1 = "00000000-0000-4000-8000-000000000001"
		run2 = "00000000-0000-4000-8000-000000000002"
		run3 = "00000000-0000-4000-8000-000000000003"
	)
	candidates := []expiredLeaseCandidate{
		leaseCandidateForRun("00000000-0000-4000-8000-000000000014", run3),
		leaseCandidateForRun("00000000-0000-4000-8000-000000000011", run1),
		leaseCandidateForRun("00000000-0000-4000-8000-000000000015", run3), // second candidate in run3
		leaseCandidateForRun("00000000-0000-4000-8000-000000000012", run2),
	}
	tx := &fakeLeaseRepairTx{candidates: candidates}
	repair, err := newLeaseRepair(func(context.Context) (pgx.Tx, error) { return tx, nil })
	if err != nil {
		t.Fatal(err)
	}
	result, err := repair.Step(context.Background(), now, len(candidates))
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != len(candidates) {
		t.Fatalf("Failed = %d, want %d (result=%#v)", result.Failed, len(candidates), result)
	}

	var events []string
	for _, entry := range tx.callLog {
		if entry == "exec:advisory" {
			continue
		}
		events = append(events, entry)
	}
	if len(events) != len(candidates)+3*len(candidates) {
		t.Fatalf("callLog (advisory locks excluded) = %v, want %d entries", events, len(candidates)+3*len(candidates))
	}

	// Phase 0: every candidate's unit row locked, ascending by unit id,
	// before ANYTHING else (round 4).
	wantUnitLockOrder := []string{
		"00000000-0000-4000-8000-000000000011",
		"00000000-0000-4000-8000-000000000012",
		"00000000-0000-4000-8000-000000000014",
		"00000000-0000-4000-8000-000000000015",
	}
	var gotUnitLockOrder []string
	for _, entry := range events[:len(candidates)] {
		if !strings.HasPrefix(entry, "queryrow:unitlock:") {
			t.Fatalf("phase-0 event %q is not a unit lock -- every unit must be locked before any write or run lock", entry)
		}
		gotUnitLockOrder = append(gotUnitLockOrder, strings.TrimPrefix(entry, "queryrow:unitlock:"))
	}
	if !reflect.DeepEqual(gotUnitLockOrder, wantUnitLockOrder) {
		t.Fatalf("phase-0 unit-lock order = %v, want ascending %v", gotUnitLockOrder, wantUnitLockOrder)
	}

	// Phase 2: per-candidate unit-write-then-Bump, in ascending sync_run_id
	// order (round 3) -- the pre-locked units above make each candidate's
	// own unit write here a no-op re-lock, never a fresh acquisition.
	phase2 := events[len(candidates):]
	var runLockOrder []string
	for i := range candidates {
		unitEvent, lockEvent, recomputeEvent := phase2[3*i], phase2[3*i+1], phase2[3*i+2]
		if !strings.HasPrefix(unitEvent, "exec:unit:") {
			t.Fatalf("phase-2 event %d = %q, want the unit write to come first for each candidate", 3*i, unitEvent)
		}
		if !strings.HasPrefix(lockEvent, "queryrow:runlock:") {
			t.Fatalf("phase-2 event %d = %q, want the run lock to come right after its candidate's unit write", 3*i+1, lockEvent)
		}
		if !strings.HasPrefix(recomputeEvent, "queryrow:recompute:") {
			t.Fatalf("phase-2 event %d = %q, want the rollup recompute right after the run lock", 3*i+2, recomputeEvent)
		}
		runLockOrder = append(runLockOrder, strings.TrimPrefix(lockEvent, "queryrow:runlock:"))
	}
	want := []string{run1, run2, run3, run3}
	if !reflect.DeepEqual(runLockOrder, want) {
		t.Fatalf("run-lock visit order = %v, want %v (never regressing to an earlier run; candidates were fed in a scrambled order on purpose)", runLockOrder, want)
	}
}

func TestLeaseRepairRejectsInvalidBoundsWithoutBeginning(t *testing.T) {
	called := false
	repair, err := newLeaseRepair(func(context.Context) (pgx.Tx, error) {
		called = true
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, leaseRepairMaximumLimit + 1} {
		if _, err := repair.Step(context.Background(), time.Now(), limit); !errors.Is(err, ErrInvalidConfiguration) || called {
			t.Fatalf("limit %d error = %v begin=%t", limit, err, called)
		}
	}
}
