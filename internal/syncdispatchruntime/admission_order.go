package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
)

// runOrder is the admission position of a run (CHAOS-8880): runs of one bucket are admitted oldest first on the
// total order (createdAt, id). backfill marks a run that never holds back a non-backfill run.
type runOrder struct {
	id        string
	createdAt time.Time
	backfill  bool
	// mode and triggeredBy are the stored columns backfill is derived from; readRunOrder fills them for the log.
	mode        string
	triggeredBy string
}

// olderRun is one open run of a bucket, older than the deciding run, that still has units waiting for a slot.
// progressAt is the newest sign that its dispatch makes progress; waiting is how many of its units wait in the bucket.
type olderRun struct {
	runOrder
	progressAt time.Time
	waiting    int
}

// olderRunsReadLimit bounds the older runs one bucket decision reads. Past it the decision holds back for the oldest
// ones only, which is never fewer slots than today's (none).
const olderRunsReadLimit = 16

// admissionLivenessBound is how long an older run may show no progress and still hold slots back. A run whose pass is
// blocked (a paused route, a reference discovery that never ends) stops holding newer runs after it.
func admissionLivenessBound() time.Duration {
	return 2 * redispatchCountdownMax(redispatchCountdown())
}

// runBefore is the admission order: createdAt, then id, so two runs never hold each other back.
func runBefore(left, right runOrder) bool {
	if !left.createdAt.Equal(right.createdAt) {
		return left.createdAt.Before(right.createdAt)
	}
	return left.id < right.id
}

// holdBackFor returns how many of freeSlots the deciding run leaves to older live runs of the bucket, and the
// oldest run it held slots for (empty when it held none). Policy B': a backfill never holds back a non-backfill run;
// any other older run holds back every newer run, backfills included.
func holdBackFor(self runOrder, older []olderRun, freeSlots int, now time.Time, bound time.Duration) (int, string) {
	if freeSlots <= 0 {
		return 0, ""
	}
	held := 0
	var oldest *runOrder
	for index := range older {
		run := older[index]
		if run.waiting <= 0 || !runBefore(run.runOrder, self) || (run.backfill && !self.backfill) {
			continue
		}
		if now.Sub(run.progressAt) >= bound {
			continue
		}
		held += run.waiting
		if oldest == nil || runBefore(run.runOrder, *oldest) {
			oldest = &older[index].runOrder
		}
	}
	if held == 0 {
		return 0, ""
	}
	return min(held, freeSlots), oldest.id
}

// wakeHeldBackRuns arms one dispatch wakeup per distinct run the pass held slots back for, after the pass committed.
// The wakeup row is deduped per run by the outbox key, so passes of several newer runs leave one row.
func wakeHeldBackRuns(ctx context.Context, db syncrunrollup.Execer, runIDs []string, now time.Time) int {
	woken := 0
	seen := make(map[string]bool, len(runIDs))
	for _, runID := range runIDs {
		if seen[runID] {
			continue
		}
		seen[runID] = true
		syncrunrollup.WakeDispatch(ctx, db, runID, now, syncrunrollup.DispatchWakePathHoldBack)
		woken++
	}
	return woken
}

// readRunOrder reads the deciding run's admission position in the guard transaction.
func readRunOrder(ctx context.Context, tx pgx.Tx, runID string) (runOrder, error) {
	order := runOrder{id: runID}
	err := tx.QueryRow(ctx, `
SELECT created_at, (triggered_by = 'backfill' OR mode = 'backfill'), mode, triggered_by
FROM public.sync_runs
WHERE id = $1::uuid`, runID).Scan(&order.createdAt, &order.backfill, &order.mode, &order.triggeredBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return runOrder{}, fmt.Errorf("%w: read run order: sync run %s not found", ErrDiscoveryTransientFailure, runID)
	}
	if err != nil {
		return runOrder{}, fmt.Errorf("%w: read run order: %w", ErrDiscoveryTransientFailure, err)
	}
	return order, nil
}

// olderRunsInBucketSQL lists the open runs of the org, older than the deciding run, that have a unit waiting in the
// bucket and are live, oldest first. The backfill rule is applied here as well as in holdBackFor so a crowd of older
// backfills cannot fill the LIMIT. SQL proves liveness, so progress_at is returned as now ($7).
//
// Waiting: planned, or retrying and due, in the bucket -- the units a pass of that run would claim now. They are read
// from the bucket's own index (ix_sync_run_units_bucket_status_lease: org, provider, cost class, status) and grouped
// by run, so the bucket and due filters run before any limit: a run whose bucket units lie behind any number of
// units of other buckets still reads as waiting (CHAOS-14250 r1b P1-2), and a run's backlog in other buckets is never
// read (CHAOS-8880 F1).
//
// Live: one bounded probe per progress sign inside the window ($9 seconds before now), each an EXISTS that stops at
// its first row:
//   - created: the run itself was created inside the window;
//   - terminal: the run's first terminal write is inside the window. Every terminal unit write arms the run's
//     finalize_sync_run outbox row in its own transaction (syncrunrollup.ArmFinalize; the dispatch pass's own
//     terminal paths through armFinalizeSyncRunWakeup), and only that INSERT sets created_at. updated_at is NOT
//     read: the relay rewrites it on claim, dispatch and publish-failure backoff, so a row stuck in backoff would
//     read as live for ever. One probe on uq_sync_dispatch_outbox_run_kind; later terminal writes count only
//     through the other signs;
//   - heartbeat: a running unit sent a heartbeat;
//   - lease: a running unit holds a live lease;
//   - dispatching: a unit was claimed (dispatching) inside the window;
//   - swept: the bucket's sync.bucket_sweep picked the run for a free slot inside the window -- the mark the sweep
//     writes into sync_runs.result (bucketSweepWokenKey, CHAOS-14250), so another run's pass leaves that slot to it.
//     A run whose pass never runs (paused route) stops holding newer runs once the mark is older than the bound; a
//     run the sweep never picks has no mark.
//
// The unit probes read only the run's running or dispatching units (ix_sync_run_units_run_status), which the
// concurrency caps keep few; a run's planned backlog and its finished units are never read.
//
// Cost: the bucket's waiting units of the org (one index range per status), then the candidate runs' running and
// dispatching units. A finished history and other buckets' backlogs are never read.
var olderRunsInBucketSQL = `
WITH waiting AS MATERIALIZED (
    SELECT unit.sync_run_id, LEAST(count(*), $8)::int AS n
    FROM public.sync_run_units AS unit
    WHERE unit.org_id = $1
        AND unit.provider = $2
        AND unit.cost_class = $3
        AND (unit.status = 'planned' OR (unit.status = 'retrying' AND unit.available_at <= $7))
    GROUP BY unit.sync_run_id
)
SELECT run.id::text, run.created_at, (run.triggered_by = 'backfill' OR run.mode = 'backfill'), $7::timestamptz, waiting.n
FROM waiting
JOIN public.sync_runs AS run ON run.id = waiting.sync_run_id
WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
    AND run.org_id = $1
    AND (run.created_at, run.id) < ($4::timestamptz, $5::uuid)
    AND ($6::boolean OR NOT (run.triggered_by = 'backfill' OR run.mode = 'backfill'))
    AND waiting.n > 0
    AND (
        run.created_at > $7::timestamptz - make_interval(secs => $9::double precision)
        OR EXISTS (
            SELECT 1 FROM public.sync_dispatch_outbox AS outbox
            WHERE outbox.sync_run_id = run.id AND outbox.kind = 'finalize_sync_run'
                AND outbox.created_at > $7::timestamptz - make_interval(secs => $9::double precision)
            LIMIT 1
        )
        OR EXISTS (
            SELECT 1 FROM public.sync_run_units AS unit
            WHERE unit.sync_run_id = run.id AND unit.status = 'running'
                AND unit.last_heartbeat_at > $7::timestamptz - make_interval(secs => $9::double precision)
            LIMIT 1
        )
        OR EXISTS (
            SELECT 1 FROM public.sync_run_units AS unit
            WHERE unit.sync_run_id = run.id AND unit.status = 'running'
                AND unit.lease_expires_at > $7::timestamptz
            LIMIT 1
        )
        OR EXISTS (
            SELECT 1 FROM public.sync_run_units AS unit
            WHERE unit.sync_run_id = run.id AND unit.status = 'dispatching'
                AND unit.updated_at > $7::timestamptz - make_interval(secs => $9::double precision)
            LIMIT 1
        )
        OR (
            jsonb_typeof(run.result::jsonb -> '` + bucketSweepWokenKey + `') = 'number'
            AND (run.result::jsonb ->> '` + bucketSweepWokenKey + `')::double precision
                > extract(epoch FROM $7::timestamptz) - $9::double precision
        )
    )
ORDER BY run.created_at, run.id
LIMIT $10`

// readOlderRuns runs olderRunsInBucketSQL for one bucket in the guard transaction, after the bucket locks.
func readOlderRuns(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, self runOrder, now time.Time) ([]olderRun, error) {
	rows, err := tx.Query(ctx, olderRunsInBucketSQL,
		bucket.orgID, bucket.provider, bucket.costClass,
		self.createdAt, self.id, self.backfill, now,
		concurrencyCapForCostClass(bucket.costClass), admissionLivenessBound().Seconds(), olderRunsReadLimit)
	if err != nil {
		return nil, fmt.Errorf("%w: load older runs in bucket: %w", ErrDiscoveryTransientFailure, err)
	}
	defer rows.Close()
	var older []olderRun
	for rows.Next() {
		var run olderRun
		if err := rows.Scan(&run.id, &run.createdAt, &run.backfill, &run.progressAt, &run.waiting); err != nil {
			return nil, fmt.Errorf("%w: scan older run in bucket: %w", ErrDiscoveryTransientFailure, err)
		}
		older = append(older, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate older runs in bucket: %w", ErrDiscoveryTransientFailure, err)
	}
	return older, nil
}
