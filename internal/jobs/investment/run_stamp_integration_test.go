//go:build integration

package investment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
)

// CHAOS-14228: two runs of one org that read the worker clock in the same
// millisecond must still store two versions of a unit, and the later-STARTED
// run must be the row every reader serves, before and after a merge.

// attachRunStamp gives the executor the production run stamp on the domain
// pool and returns the stamps it issued so far.
func attachRunStamp(executor *NativeExecutor, pool *pgxpool.Pool) func() []time.Time {
	stamper, err := NewRunStamper(pool, testLogger())
	if err != nil {
		panic(err)
	}
	recorder := &recordedRunStamp{inner: stamper}
	executor.SetRunStamp(recorder)
	return recorder.issued
}

type recordedRunStamp struct {
	inner  RunStamp
	mu     sync.Mutex
	stamps []time.Time
}

func (r *recordedRunStamp) Stamp(ctx context.Context, orgID string) (time.Time, error) {
	stamp, err := r.inner.Stamp(ctx, orgID)
	if err == nil {
		r.mu.Lock()
		r.stamps = append(r.stamps, stamp)
		r.mu.Unlock()
	}
	return stamp, err
}

func (r *recordedRunStamp) issued() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.stamps...)
}

// insertArm is how the server takes the runs' inserts.
type insertArm struct {
	name     string
	settings clickhouse.Settings
	// collapses: concurrent inserts can share one part, and optimize_on_insert
	// then keeps one row per unit of that part, so a run's rows can be gone
	// before any merge.
	collapses bool
}

var insertArms = []insertArm{
	{name: "sync_insert", settings: clickhouse.Settings{"async_insert": 0}},
	{name: "async_insert", settings: clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 1, "optimize_on_insert": 1}, collapses: true},
}

func eachInsertArm(t *testing.T, cell func(t *testing.T, arm insertArm)) {
	for _, arm := range insertArms {
		t.Run(arm.name, func(t *testing.T) { cell(t, arm) })
	}
}

// pinnedWorkerInstant is the worker-clock instant a venue set up at setup
// pins: an hour before it, so no PostgreSQL stamp taken at or after setup can
// equal it.
func pinnedWorkerInstant(setup time.Time) time.Time {
	return setup.Add(-time.Hour).Truncate(time.Millisecond)
}

type runStampVenue struct {
	h      *shadowHarness
	pg     domainroledb.Venue
	arm    insertArm
	at     time.Time
	scope  string
	mu     sync.Mutex
	issued []func() []time.Time
}

func newRunStampVenue(t *testing.T, arm insertArm) *runStampVenue {
	t.Helper()
	for _, name := range []string{"LLM_PROVIDER", "TYPESAFE_API_KEY", "INVESTMENT_SHADOW_PROVIDER", "INVESTMENT_SHADOW_ORG_IDS",
		"TYPESAFE_MODEL", "LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL", "LLM_MODEL_OPENAI", envLLMBatchMode, envLLMBatchTimeout} {
		t.Setenv(name, "")
	}
	h := newShadowHarness(t)
	pg := domainroledb.Start(t, h.ctx, "runstamp")
	pg.RequireReady(t, h.ctx)
	// A background merge collapses the versions under test; optimizeFinal
	// starts merges again.
	if err := h.conn.Exec(h.ctx, `SYSTEM STOP MERGES work_unit_investments`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	return &runStampVenue{
		h:   h,
		pg:  pg,
		arm: arm,
		// Every run reads this instant from the worker clock.
		at: pinnedWorkerInstant(now),
		scope: `{"force":true,"from_date":"` + h.windowStart.Format("2006-01-02") +
			`","to_date":"` + h.windowEnd.Format("2006-01-02") + `"}`,
	}
}

// executor is one worker replica: its own executor and writer, the shared
// pinned clock, and provider as its LLM.
func (v *runStampVenue) executor(t *testing.T, provider categorize.Provider) *NativeExecutor {
	t.Helper()
	writer, err := chwrite.NewWriter(v.h.conn)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewNativeExecutor(v.h.reader, writer, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	executor.now = func() time.Time { return v.at }
	executor.newProvider = func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
		return provider, categorize.ProviderKindMock, nil
	}
	executor.newShadow = nil
	issued := attachRunStamp(executor, v.pg.Domain)
	v.mu.Lock()
	v.issued = append(v.issued, issued)
	v.mu.Unlock()
	return executor
}

// issuedStamps is every stamp the executors of the venue issued.
func (v *runStampVenue) issuedStamps() []time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	var all []time.Time
	for _, issued := range v.issued {
		all = append(all, issued()...)
	}
	return all
}

func (v *runStampVenue) execute(ctx context.Context, executor *NativeExecutor, requestID string) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(v.arm.settings))
	_, err := executor.Execute(ctx, workgraph.Claim{Request: workgraph.Request{
		ID: requestID, OrganizationID: hierarchyCascadeTestOrg, Kind: workgraph.KindMaterialize, Scope: []byte(v.scope),
	}, Token: "run-stamp-claim"})
	return err
}

// runIDs is every distinct run id stored so far.
func (v *runStampVenue) runIDs(t *testing.T) []string {
	t.Helper()
	rows, err := v.h.conn.Query(v.h.ctx, `SELECT DISTINCT categorization_run_id FROM work_unit_investments ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func without(ids []string, drop string) string {
	for _, id := range ids {
		if id != drop {
			return id
		}
	}
	return ""
}

type unitPick struct {
	unit, ops, goReader, acr string
	rows, versions, runs     uint64
}

// picks reads, per unit, the run id each reader form serves: ops
// (latestrow.OrderKey, the merge order), dev-health-go (bare argMax by
// computed_at) and acr after CHAOS-7323 (computed_at, then the larger run id).
func (v *runStampVenue) picks(t *testing.T) []unitPick {
	t.Helper()
	rows, err := v.h.conn.Query(v.h.ctx, fmt.Sprintf(`
SELECT work_unit_id,
       %s,
       argMax(categorization_run_id, computed_at),
       argMax(categorization_run_id, (computed_at, categorization_run_id)),
       count(),
       uniqExact(computed_at),
       uniqExact(categorization_run_id)
FROM work_unit_investments
WHERE org_id = ?
GROUP BY org_id, work_unit_id
ORDER BY work_unit_id`, latestrow.ArgMax("categorization_run_id")),
		hierarchyCascadeTestOrg)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []unitPick
	for rows.Next() {
		var p unitPick
		if err := rows.Scan(&p.unit, &p.ops, &p.goReader, &p.acr, &p.rows, &p.versions, &p.runs); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("no unit was written")
	}
	return out
}

// requireServed checks every unit: one version per stored run (wantRuns of
// them, 0 = as many as are stored), and every reader form serves winner.
func (v *runStampVenue) requireServed(t *testing.T, stage string, wantRuns uint64, winner string) {
	t.Helper()
	for _, p := range v.picks(t) {
		if wantRuns != 0 && p.runs != wantRuns {
			t.Errorf("%s: unit %s: %d runs stored, want %d", stage, p.unit, p.runs, wantRuns)
		}
		if p.versions != p.runs {
			t.Errorf("%s: unit %s: %d rows of %d runs hold %d distinct computed_at: runs share a version", stage, p.unit, p.rows, p.runs, p.versions)
		}
		if p.ops != winner || p.goReader != winner || p.acr != winner {
			t.Errorf("%s: unit %s: served run ops=%s go=%s acr=%s, want the later-started run %s", stage, p.unit, p.ops, p.goReader, p.acr, winner)
		}
	}
}

// requireNewestStampKept: the newest stamp issued is the version every unit
// keeps, and one run holds it.
func (v *runStampVenue) requireNewestStampKept(t *testing.T, stage string) {
	t.Helper()
	issued := v.issuedStamps()
	if len(issued) == 0 {
		t.Errorf("%s: no run stamp was issued", stage)
		return
	}
	newest := issued[0]
	for _, stamp := range issued[1:] {
		if stamp.After(newest) {
			newest = stamp
		}
	}
	var units, atNewest, runsAtNewest uint64
	if err := v.h.conn.QueryRow(v.h.ctx, `
SELECT uniqExact(work_unit_id), uniqExactIf(work_unit_id, computed_at = toDateTime64(?, 3, 'UTC')),
       uniqExactIf(categorization_run_id, computed_at = toDateTime64(?, 3, 'UTC'))
FROM work_unit_investments`, newest.UTC().Format("2006-01-02 15:04:05.000"), newest.UTC().Format("2006-01-02 15:04:05.000")).Scan(&units, &atNewest, &runsAtNewest); err != nil {
		t.Fatal(err)
	}
	if atNewest != units || runsAtNewest != 1 {
		t.Errorf("%s: newest stamp %s: %d of %d units keep it, %d runs hold it; want every unit, one run", stage, newest.Format(time.RFC3339Nano), atNewest, units, runsAtNewest)
	}
}

func (v *runStampVenue) optimizeFinal(t *testing.T) {
	t.Helper()
	for _, statement := range []string{`SYSTEM START MERGES work_unit_investments`, `OPTIMIZE TABLE work_unit_investments FINAL`} {
		if err := v.h.conn.Exec(v.h.ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

// requireStampedRun fails when a run's rows carry the worker clock: the run
// took no stamp.
func (v *runStampVenue) requireStampedRun(t *testing.T, runID string) {
	t.Helper()
	var atWorkerClock uint64
	if err := v.h.conn.QueryRow(v.h.ctx,
		`SELECT countIf(computed_at = toDateTime64(?, 3, 'UTC')) FROM work_unit_investments WHERE categorization_run_id = ?`,
		v.at.Format("2006-01-02 15:04:05.000"), runID).Scan(&atWorkerClock); err != nil {
		t.Fatal(err)
	}
	if atWorkerClock != 0 {
		t.Errorf("run %s: %d rows carry the worker clock %s: the run took no per-org stamp", runID, atWorkerClock, v.at.Format(time.RFC3339Nano))
	}
}

// requireNoDenials: the domain role ran every statement of the stamp.
func (v *runStampVenue) requireNoDenials(t *testing.T) {
	t.Helper()
	if denied := v.pg.Denials.All(); len(denied) != 0 {
		t.Errorf("the domain role was refused: %q", denied)
	}
}

// advisoryLocks counts the advisory locks held in the venue right now.
func (v *runStampVenue) advisoryLocks(t *testing.T) int64 {
	t.Helper()
	var held int64
	if err := v.pg.Admin.QueryRow(v.h.ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	return held
}

// gatedProvider holds every completion until release is closed, so its run
// takes its stamp, then waits inside categorization.
type gatedProvider struct {
	categorize.MockProvider
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
}

func newGatedProvider() *gatedProvider {
	return &gatedProvider{entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *gatedProvider) Complete(ctx context.Context, request categorize.CompletionRequest) (categorize.CompletionResult, error) {
	p.enteredOnce.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return categorize.CompletionResult{}, ctx.Err()
	}
	return p.MockProvider.Complete(ctx, request)
}

// Winner written LAST: run A starts and writes, then run B starts and writes.
// Both read the same worker-clock instant.
func TestRunStampLaterStartedRunWinsWrittenLast(t *testing.T) {
	eachInsertArm(t, func(t *testing.T, arm insertArm) {
		v := newRunStampVenue(t, arm)

		if err := v.execute(v.h.ctx, v.executor(t, categorize.MockProvider{}), "run-stamp-a"); err != nil {
			t.Fatal(err)
		}
		ids := v.runIDs(t)
		if len(ids) != 1 {
			t.Fatalf("after run A: run ids %q", ids)
		}
		runA := ids[0]
		if err := v.execute(v.h.ctx, v.executor(t, categorize.MockProvider{}), "run-stamp-b"); err != nil {
			t.Fatal(err)
		}
		runB := without(v.runIDs(t), runA)
		if runB == "" {
			t.Fatal("run B wrote no row")
		}

		v.requireStampedRun(t, runA)
		v.requireStampedRun(t, runB)
		v.requireServed(t, "before merge", 2, runB)
		v.requireNewestStampKept(t, "before merge")
		v.optimizeFinal(t)
		v.requireServed(t, "after OPTIMIZE FINAL", 1, runB)
		v.requireNewestStampKept(t, "after OPTIMIZE FINAL")
		v.requireNoDenials(t)
	})
}

// Winner written FIRST: run A starts (takes its stamp) and waits inside
// categorization; run B starts and writes; then A writes. A stamp lock held
// across A's run would block B here, so B's run has a deadline. While A
// waits, no advisory lock is held: the lock covers the stamp only.
func TestRunStampLaterStartedRunWinsWrittenFirst(t *testing.T) {
	eachInsertArm(t, func(t *testing.T, arm insertArm) {
		v := newRunStampVenue(t, arm)

		gated := newGatedProvider()
		runADone := make(chan error, 1)
		executorA := v.executor(t, gated)
		go func() { runADone <- v.execute(v.h.ctx, executorA, "run-stamp-a") }()
		select {
		case <-gated.entered:
		case err := <-runADone:
			t.Fatalf("run A ended before categorization: %v", err)
		case <-time.After(2 * time.Minute):
			t.Fatal("run A never reached categorization")
		}

		if held := v.advisoryLocks(t); held != 0 {
			t.Errorf("run A holds %d advisory locks during categorization: the stamp lock must end before the run", held)
		}
		bCtx, cancel := context.WithTimeout(v.h.ctx, time.Minute)
		err := v.execute(bCtx, v.executor(t, categorize.MockProvider{}), "run-stamp-b")
		cancel()
		if err != nil {
			close(gated.release)
			<-runADone
			t.Fatalf("run B (started while A waits): %v", err)
		}
		ids := v.runIDs(t)
		if len(ids) != 1 {
			close(gated.release)
			<-runADone
			t.Fatalf("after run B: run ids %q", ids)
		}
		runB := ids[0]

		close(gated.release)
		if err := <-runADone; err != nil {
			t.Fatalf("run A: %v", err)
		}
		runA := without(v.runIDs(t), runB)
		if runA == "" {
			t.Fatal("run A wrote no row")
		}

		v.requireStampedRun(t, runA)
		v.requireStampedRun(t, runB)
		v.requireServed(t, "before merge", 2, runB)
		v.requireNewestStampKept(t, "before merge")
		v.optimizeFinal(t)
		v.requireServed(t, "after OPTIMIZE FINAL", 1, runB)
		v.requireNewestStampKept(t, "after OPTIMIZE FINAL")
		v.requireNoDenials(t)
	})
}

// Concurrent runs of one org, all on the same worker-clock instant, each on
// its own replica: no two stored runs share a version, the newest stamp is the
// row every unit keeps, and every reader serves it, before and after a merge.
// With async inserts the runs' inserts can share one part, so fewer runs than
// were started may be stored.
func TestRunStampConcurrentRunsStoreDistinctVersions(t *testing.T) {
	eachInsertArm(t, func(t *testing.T, arm insertArm) {
		v := newRunStampVenue(t, arm)
		const runs = 6

		executors := make([]*NativeExecutor, runs)
		for i := range executors {
			executors[i] = v.executor(t, categorize.MockProvider{})
		}
		start := make(chan struct{})
		errs := make(chan error, runs)
		var wg sync.WaitGroup
		for i, executor := range executors {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs <- v.execute(v.h.ctx, executor, fmt.Sprintf("run-stamp-%d", i))
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		ids := v.runIDs(t)
		if len(ids) == 0 || len(ids) > runs || (!arm.collapses && len(ids) != runs) {
			t.Fatalf("run ids %q, want %d (async insert: 1..%d)", ids, runs, runs)
		}
		for _, id := range ids {
			v.requireStampedRun(t, id)
		}

		var newest string
		var runsAtNewest uint64
		if err := v.h.conn.QueryRow(v.h.ctx, `
SELECT any(categorization_run_id), uniqExact(categorization_run_id) FROM work_unit_investments
WHERE computed_at = (SELECT max(computed_at) FROM work_unit_investments)`).Scan(&newest, &runsAtNewest); err != nil {
			t.Fatal(err)
		}
		if runsAtNewest != 1 {
			t.Errorf("%d runs share the newest version: no single later-started run", runsAtNewest)
		}
		wantRuns := uint64(0)
		if !arm.collapses {
			wantRuns = runs
		}
		v.requireServed(t, "before merge", wantRuns, newest)
		v.requireNewestStampKept(t, "before merge")
		v.optimizeFinal(t)
		v.requireServed(t, "after OPTIMIZE FINAL", 1, newest)
		v.requireNewestStampKept(t, "after OPTIMIZE FINAL")
		v.requireNoDenials(t)
	})
}

// Callers of one org that stamp at the same instant each get their own
// millisecond, as the prod domain role; another org takes the same lock key
// only by hash collision. No advisory lock outlives a stamp.
func TestRunStampsAreDistinctPerOrgUnderContention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	pg := domainroledb.Start(t, ctx, "runstampload")
	pg.RequireReady(t, ctx)
	stamper, err := NewRunStamper(pg.Domain, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	const rounds, callers = 20, 4
	var all []time.Time
	for round := range rounds {
		start := make(chan struct{})
		got := make([]time.Time, callers)
		errs := make([]error, callers)
		var wg sync.WaitGroup
		for caller := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				got[caller], errs[caller] = stamper.Stamp(ctx, hierarchyCascadeTestOrg)
			}()
		}
		close(start)
		wg.Wait()
		for caller, err := range errs {
			if err != nil {
				t.Fatalf("round %d caller %d: %v", round, caller, err)
			}
		}
		all = append(all, got...)
	}
	seen := map[time.Time]int{}
	for _, stamp := range all {
		if stamp.Truncate(time.Millisecond) != stamp {
			t.Errorf("stamp %s is finer than a millisecond", stamp.Format(time.RFC3339Nano))
		}
		seen[stamp]++
	}
	if len(seen) != len(all) {
		t.Errorf("%d stamps hold %d distinct milliseconds: callers of one org shared a stamp", len(all), len(seen))
	}
	var held int64
	if err := pg.Admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Errorf("%d advisory locks held after every stamp returned", held)
	}
	if denied := pg.Denials.All(); len(denied) != 0 {
		t.Errorf("the domain role was refused: %q", denied)
	}
}

// Census: the run stamp's first lock key differs from every other two-key
// advisory namespace in the repo (internal/jobs/metrics/remaining/
// manual_recompute.go:168-170, manual_backfill.go:392-394, postgres.go:347-348
// and :1056-1057, internal/jobs/metrics/daily/manual_run.go:197-198,
// internal/syncdispatchruntime/touched_days_drain.go:515 and :519).
func TestRunStampLockNamespaceCollidesWithNoOtherTwoKeyLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	pg := domainroledb.Start(t, ctx, "runstampkey")
	var ours int32
	if err := pg.Domain.QueryRow(ctx, `SELECT hashtext($1)`, RunStampLockNamespace).Scan(&ours); err != nil {
		t.Fatal(err)
	}
	others := map[string]int32{"touchedDrainLockNamespace": 8846, "touchedCheckLockNamespace": 8847}
	for _, name := range []string{"remaining_metrics_manual_backfill_day", "remaining_metrics_dora_day", "remaining_metrics_fresh_scope", "daily_metrics_manual_day"} {
		var key int32
		if err := pg.Domain.QueryRow(ctx, `SELECT hashtext($1)`, name).Scan(&key); err != nil {
			t.Fatal(err)
		}
		others[name] = key
	}
	for name, key := range others {
		if key == ours {
			t.Errorf("run stamp namespace %q collides with %s (%d)", RunStampLockNamespace, name, key)
		}
	}
}

// A PostgreSQL clock that never leaves the stamped millisecond: the stamp is
// refused with an error and a log line naming it, and the run writes nothing.
func TestRunStampHeldClockFailsTheRunAndWritesNothing(t *testing.T) {
	v := newRunStampVenue(t, insertArms[0])
	executor := v.executor(t, categorize.MockProvider{})
	logs := &syncBuffer{}
	stamper, err := NewRunStamper(v.pg.Domain, debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	stamper.clockLeftSQL = `SELECT false FROM pg_sleep(0.001) WHERE $1::timestamptz IS NOT NULL`
	executor.SetRunStamp(stamper)

	err = v.execute(v.h.ctx, executor, "run-stamp-held")
	if !errors.Is(err, ErrRunStampClockHeld) || !errors.Is(err, workgraph.ErrUnavailable) {
		t.Fatalf("run with a held clock: err = %v, want ErrRunStampClockHeld as dependency_unavailable", err)
	}
	logged := logs.String()
	if !strings.Contains(logged, "investment run stamp refused") || !strings.Contains(logged, "stamp=") || !strings.Contains(logged, "org_id="+hierarchyCascadeTestOrg) {
		t.Errorf("no log line names the refused stamp: %q", logged)
	}
	if rows := v.h.count(t, `SELECT count() FROM work_unit_investments`); rows != 0 {
		t.Errorf("the refused run wrote %d investment rows", rows)
	}
	if held := v.advisoryLocks(t); held != 0 {
		t.Errorf("%d advisory locks held after the refused stamp", held)
	}
}

// setupInStampMillisecond re-pins the venue as if it had been set up in the
// millisecond of the run's own stamp: the CI flake of run 44619068634.
type setupInStampMillisecond struct {
	inner RunStamp
	v     *runStampVenue
}

func (s *setupInStampMillisecond) Stamp(ctx context.Context, orgID string) (time.Time, error) {
	stamp, err := s.inner.Stamp(ctx, orgID)
	if err == nil {
		s.v.at = pinnedWorkerInstant(stamp)
	}
	return stamp, err
}

// A venue set up in the same millisecond as the run's stamp: the run took its
// own stamp, so the worker-clock check must not fire.
func TestRunStampVenueSetUpInTheStampMillisecondKeepsTheRunStamped(t *testing.T) {
	v := newRunStampVenue(t, insertArms[0])
	executor := v.executor(t, categorize.MockProvider{})
	executor.SetRunStamp(&setupInStampMillisecond{inner: executor.runStamp, v: v})
	if err := v.execute(v.h.ctx, executor, "run-stamp-setup-ms"); err != nil {
		t.Fatal(err)
	}
	ids := v.runIDs(t)
	if len(ids) != 1 {
		t.Fatalf("run ids %q", ids)
	}
	issued := v.issuedStamps()
	if len(issued) != 1 || !issued[0].Truncate(time.Millisecond).Equal(issued[0]) {
		t.Fatalf("issued stamps %v, want one millisecond stamp", issued)
	}
	v.requireStampedRun(t, ids[0])
}
