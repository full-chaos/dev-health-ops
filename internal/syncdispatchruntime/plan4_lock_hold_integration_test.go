//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14250 acp lock-1 rule 3 (INFO, no assert on a duration): the bucket-lock hold of one dispatch pass and of one
// sweep at queue depth 350, and the commit-path lock wait of a unit writer queued behind a sweep. A pgx QueryTracer
// timestamps, per connection, the bucket's advisory-lock statement (pg_try_advisory_xact_lock or
// pg_advisory_xact_lock with the bucket's key) and the transaction's COMMIT: hold = COMMIT end - lock statement end;
// wait = the blocking lock statement's own duration.

type b4TraceKey struct{}

type b4Query struct {
	sql   string
	args  []any
	start time.Time
}

type b4ConnState struct {
	lockEnd time.Time
	locked  bool
}

// b4Tracer is a pgx.QueryTracer: bucket-lock holds and waits, and the SQLSTATE of every failed statement.
type b4Tracer struct {
	mu      sync.Mutex
	lockArg string
	conns   map[*pgx.Conn]*b4ConnState
	holds   []time.Duration
	waits   []time.Duration
	codes   map[string]int
	samples map[string]string
}

func newB4Tracer(bucket syncbucket.Bucket) *b4Tracer {
	return &b4Tracer{lockArg: fmt.Sprint(syncbucket.LockKey(bucket)), conns: map[*pgx.Conn]*b4ConnState{},
		codes: map[string]int{}, samples: map[string]string{}}
}

func (tracer *b4Tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, b4TraceKey{}, &b4Query{sql: data.SQL, args: data.Args, start: time.Now()})
}

func (tracer *b4Tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	end := time.Now()
	query, ok := ctx.Value(b4TraceKey{}).(*b4Query)
	if !ok {
		return
	}
	statement := strings.ToLower(strings.TrimSpace(query.sql))
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if data.Err != nil {
		code := "go_error"
		var pgErr *pgconn.PgError
		if errors.As(data.Err, &pgErr) {
			code = pgErr.Code
		}
		tracer.codes[code]++
		if _, seen := tracer.samples[code]; !seen {
			tracer.samples[code] = fmt.Sprintf("%.120s -> %v", strings.Join(strings.Fields(query.sql), " "), data.Err)
		}
	}
	state := tracer.conns[conn]
	switch {
	case statement == "begin" || strings.HasPrefix(statement, "begin "):
		tracer.conns[conn] = &b4ConnState{}
	case strings.Contains(statement, "advisory_xact_lock(") && data.Err == nil && len(query.args) == 1 && fmt.Sprint(query.args[0]) == tracer.lockArg:
		if state == nil {
			state = &b4ConnState{}
			tracer.conns[conn] = state
		}
		state.lockEnd, state.locked = end, true
		if !strings.Contains(statement, "pg_try_advisory_xact_lock") {
			tracer.waits = append(tracer.waits, end.Sub(query.start))
		}
	case statement == "commit":
		if state != nil && state.locked {
			tracer.holds = append(tracer.holds, end.Sub(state.lockEnd))
		}
		delete(tracer.conns, conn)
	case statement == "rollback":
		delete(tracer.conns, conn)
	}
}

func (tracer *b4Tracer) reset() {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.holds, tracer.waits = nil, nil
}

// take returns and clears the holds and waits recorded since the last reset.
func (tracer *b4Tracer) take() (holds, waits []time.Duration) {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	holds, waits = tracer.holds, tracer.waits
	tracer.holds, tracer.waits = nil, nil
	return holds, waits
}

func (tracer *b4Tracer) codeCounts() (map[string]int, map[string]string) {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	codes, samples := map[string]int{}, map[string]string{}
	for code, count := range tracer.codes {
		codes[code] = count
	}
	for code, sample := range tracer.samples {
		samples[code] = sample
	}
	return codes, samples
}

// b4TracedPool is a second pool on the cell's migrated database, with tracer and maxConns connections.
func b4TracedPool(t *testing.T, ctx context.Context, base *pgxpool.Pool, tracer pgx.QueryTracer, maxConns int32) *pgxpool.Pool {
	t.Helper()
	config := base.Config()
	config.MaxConns = maxConns
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func b4Max(values []time.Duration) time.Duration {
	var top time.Duration
	for _, value := range values {
		top = max(top, value)
	}
	return top
}

// b4EndUnitWrite is a unit writer's write: the unit's success, the rollup bump and the finalize arm.
func b4EndUnitWrite(ctx context.Context, unitID, runID string, now time.Time) func(pgx.Tx) error {
	return func(tx pgx.Tx) error {
		command, err := tx.Exec(ctx, `UPDATE sync_run_units SET status = 'success', lease_owner = NULL, lease_expires_at = NULL, updated_at = $2
WHERE id = $1::uuid AND status = 'running'`, unitID, now)
		if err != nil {
			return err
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("unit %s was not running", unitID)
		}
		if _, _, _, err := syncrunrollup.Bump(ctx, tx, runID); err != nil {
			return err
		}
		return syncrunrollup.ArmFinalize(ctx, tx, runID, rqOrg, now)
	}
}

// b4InsertRunningUnit adds a running unit with a live lease to runID in the medium bucket.
func b4InsertRunningUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID, runID string, now time.Time) {
	t.Helper()
	lease, owner := now.Add(20*time.Hour), "worker-14250"
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: unitID, RunID: runID, OrgID: rqOrg, IntegrationID: rqIntegration,
		SourceID: rqSource, Provider: "github", DatasetKey: "commits", CostClass: "medium", Mode: "incremental", UpdatedAt: &now,
		Status: syncRunUnitStatusRunning, LeaseExpiresAt: &lease, LeaseOwner: &owner})
}

func TestB4BucketLockHoldIsMeasured(t *testing.T) {
	const depth, repeats = 350, 5
	withRearmQueuePool(t, func(ctx context.Context, base *pgxpool.Pool) {
		bucket := syncbucket.Bucket{OrgID: rqOrg, Provider: "github", CostClass: "medium"}
		tracer := newB4Tracer(bucket)
		pool := b4TracedPool(t, ctx, base, tracer, 8)
		defer pool.Close()
		sim, _ := newP3Sim(t, ctx, pool)
		// One unit of the holder runs: the bucket (cap 2) has one free slot, so every pick reads the heads.
		holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
		far := sim.now.Add(50 * time.Minute)
		for i := 1; i <= depth; i++ {
			sim.seed(rqRunSpec{index: i, createdAt: sim.now.Add(-30*time.Hour + time.Duration(i)*time.Second), medium: 1, wakeAt: &far})
		}
		if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
			t.Fatal(err)
		}
		if capValue := syncbucket.Cap("medium"); capValue != 2 {
			t.Fatalf("premise: medium cap = %d, want 2", capValue)
		}
		clearReservations := func() {
			if _, err := pool.Exec(ctx, `DELETE FROM sync_dispatch_bucket_reservations`); err != nil {
				t.Fatal(err)
			}
		}
		youngest := rqID(1, depth, 0)

		var passHolds []time.Duration
		for k := 0; k < repeats; k++ {
			clearReservations()
			tracer.reset()
			p4PassOnly(t, sim, youngest)
			holds, _ := tracer.take()
			if len(holds) != 1 {
				t.Fatalf("premise: pass %d recorded %d bucket-locked commits, want 1", k, len(holds))
			}
			_, reserved, err := rqReservation(ctx, pool, rqOrg, "github", "medium", rqID(1, 1, 0))
			if err != nil {
				t.Fatal(err)
			}
			if !reserved {
				t.Fatalf("premise: pass %d of the youngest run did not reserve the free slot for the oldest waiter", k)
			}
			passHolds = append(passHolds, holds[0])
		}

		var sweepHolds []time.Duration
		for k := 0; k < repeats; k++ {
			clearReservations()
			tracer.reset()
			result, err := runBucketSweep(ctx, pool, dispatchBucket{orgID: rqOrg, provider: "github", costClass: "medium"},
				func() time.Time { return sim.now })
			if err != nil {
				t.Fatal(err)
			}
			holds, _ := tracer.take()
			if len(holds) != 1 || len(result.woken) != 1 {
				t.Fatalf("premise: sweep %d recorded %d bucket-locked commits and woke %v, want 1 and 1", k, len(holds), result.woken)
			}
			sweepHolds = append(sweepHolds, holds[0])
		}

		// Commit-path wait: a unit writer (p4WriterRelease, LockCommitPath first) reaches the bucket lock while a sweep's
		// transaction holds it (pickBucketSlots done, not committed); the sweep commits once the writer is seen waiting.
		noSweep := newP3Sweeps(func() time.Time { return sim.now })
		slot := syncrunrollup.SlotFree{RunID: holder, OrgID: rqOrg, Provider: "github", CostClass: "medium"}
		var queuedWaits, freeWaits []time.Duration
		for k := 0; k < repeats; k++ {
			for _, queued := range []bool{true, false} {
				unitID := rqID(3, 0, 100+2*k)
				if !queued {
					unitID = rqID(3, 0, 101+2*k)
				}
				b4InsertRunningUnit(t, ctx, pool, unitID, holder, sim.now)
				clearReservations()
				before := syncbucket.LockWaitCount(syncbucket.LockWaitPathProviderUnit)
				var sweepTx pgx.Tx
				if queued {
					var err error
					if sweepTx, err = pool.Begin(ctx); err != nil {
						t.Fatal(err)
					}
					if _, _, _, _, err := pickBucketSlots(ctx, sweepTx, dispatchBucket{orgID: rqOrg, provider: "github", costClass: "medium"}, sim.now); err != nil {
						t.Fatal(err)
					}
				}
				tracer.reset()
				done := make(chan error, 1)
				go func() {
					done <- p4WriterRelease(ctx, pool, noSweep, slot, sim.now, syncrunrollup.DispatchWakePathProviderUnit,
						b4EndUnitWrite(ctx, unitID, holder, sim.now))
				}()
				if queued {
					deadline := time.Now().Add(10 * time.Second)
					for {
						var waiting int
						if err := base.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&waiting); err != nil {
							t.Fatal(err)
						}
						if waiting > 0 {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("premise: the writer never queued on the bucket lock the sweep holds")
						}
					}
					if err := sweepTx.Commit(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if err := <-done; err != nil {
					t.Fatalf("writer: %v", err)
				}
				_, waits := tracer.take()
				if len(waits) != 1 {
					t.Fatalf("premise: the writer recorded %d commit-path lock statements, want 1", len(waits))
				}
				if got := syncbucket.LockWaitCount(syncbucket.LockWaitPathProviderUnit) - before; got != 1 {
					t.Fatalf("premise: LockWaitCount(provider_unit) moved by %d, want 1", got)
				}
				if queued {
					queuedWaits = append(queuedWaits, waits[0])
				} else {
					freeWaits = append(freeWaits, waits[0])
				}
			}
		}
		passHold, sweepHold := b4Max(passHolds), b4Max(sweepHolds)
		t.Logf("INFO depth %d, max over %d: pass hold=%s, sweep hold=%s, commit-path wait=%s (queued behind a sweep's commit), %s (lock free)",
			depth, repeats, passHold, sweepHold, b4Max(queuedWaits), b4Max(freeWaits))
		t.Logf("INFO pass holds %v; sweep holds %v; queued waits %v; free waits %v", passHolds, sweepHolds, queuedWaits, freeWaits)
		t.Logf("INFO syncbucket.LockWaitCount(%s) = %d", syncbucket.LockWaitPathProviderUnit, syncbucket.LockWaitCount(syncbucket.LockWaitPathProviderUnit))
		for _, hold := range []struct {
			name  string
			value time.Duration
		}{{"pass hold", passHold}, {"sweep hold", sweepHold}} {
			if hold.value > 50*time.Millisecond {
				t.Logf("REPORT: %s %s > 50ms at depth %d", hold.name, hold.value, depth)
			}
		}
	})
}
