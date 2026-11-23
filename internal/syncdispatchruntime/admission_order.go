package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
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
// Waiting: planned, or retrying and due -- the units a pass of that run would claim now. Only the first $11 planned or
// retrying units of a run are looked at (ix_sync_run_units_run_status); a run whose bucket units lie past them reads
// as not waiting, which holds back nothing (the behavior before CHAOS-8880), never too much.
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
//   - wake armed (CHAOS-14394): the run's dispatch_sync_run outbox row is pending, has no last_error and is due inside
//     the window, before or after now. A run left unclaimed by the bucket cap or by hold-back re-arms that row on every
//     pass (schedule_redispatch.go), so a run that waits for a slot stays live though it never had a unit claimed.
//     Own refusals do not show here: they end as terminal units, retrying units due later (not waiting) or no pending
//     dispatch row (discovery block, paused route, publish failure sets last_error). available_at, never updated_at
//     (the relay rewrites it). One probe on uq_sync_dispatch_outbox_run_kind.
//
// The unit probes read only the run's running or dispatching units (ix_sync_run_units_run_status), which the
// concurrency caps keep few; a run's planned backlog and its finished units are never read.
//
// Cost: the runs come from ix_sync_runs_active_candidates (non-terminal runs only), so a finished history is never
// read. Row visits per run: at most $11 waiting candidates plus its running and dispatching units.
const olderRunsInBucketSQL = `
SELECT run.id::text, run.created_at, (run.triggered_by = 'backfill' OR run.mode = 'backfill'), $7::timestamptz, waiting.n
FROM public.sync_runs AS run
CROSS JOIN LATERAL (
    SELECT count(*)::int AS n
    FROM (
        SELECT 1
        FROM (
            SELECT unit.provider, unit.cost_class, unit.status, unit.available_at
            FROM public.sync_run_units AS unit
            WHERE unit.sync_run_id = run.id
                AND unit.status IN ('planned', 'retrying')
            LIMIT $11
        ) AS seen
        WHERE seen.provider = $2
            AND seen.cost_class = $3
            AND (seen.status = 'planned' OR seen.available_at <= $7)
        LIMIT $8
    ) AS due
) AS waiting
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
        OR EXISTS (
            SELECT 1 FROM public.sync_dispatch_outbox AS wake
            WHERE wake.sync_run_id = run.id AND wake.kind = 'dispatch_sync_run'
                AND wake.status = 'pending' AND wake.last_error IS NULL
                AND wake.available_at > $7::timestamptz - make_interval(secs => $9::double precision)
                AND wake.available_at <= $7::timestamptz + make_interval(secs => $9::double precision)
            LIMIT 1
        )
    )
ORDER BY run.created_at, run.id
LIMIT $10`

// olderRunWaitingScanLimit bounds the planned or retrying units read per older run to find its waiting units in one
// bucket.
const olderRunWaitingScanLimit = 256

// readOlderRuns runs olderRunsInBucketSQL for one bucket in the guard transaction, after the bucket locks.
func readOlderRuns(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, self runOrder, now time.Time) ([]olderRun, error) {
	rows, err := tx.Query(ctx, olderRunsInBucketSQL,
		bucket.orgID, bucket.provider, bucket.costClass,
		self.createdAt, self.id, self.backfill, now,
		concurrencyCapForCostClass(bucket.costClass), admissionLivenessBound().Seconds(), olderRunsReadLimit, olderRunWaitingScanLimit)
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

// heldBackSignal names why the oldest run a bucket decision held slots for was live (CHAOS-14403), for the Info
// bucket_decision line. The first six are the liveness probes of olderRunsInBucketSQL in the order
// pickHeldBackSignal takes the first true one.
type heldBackSignal string

const (
	heldBackSignalNone            heldBackSignal = "none"
	heldBackSignalRecentRun       heldBackSignal = "recent_run"
	heldBackSignalFinalizePending heldBackSignal = "finalize_pending"
	heldBackSignalUnitHeartbeat   heldBackSignal = "unit_heartbeat"
	heldBackSignalUnitLease       heldBackSignal = "unit_lease"
	heldBackSignalUnitDispatching heldBackSignal = "unit_dispatching"
	heldBackSignalWakeArmed       heldBackSignal = "wake_armed"
	// heldBackSignalRecheckedNone: no probe was true on the recheck (the run's state moved after olderRunsInBucketSQL).
	heldBackSignalRecheckedNone heldBackSignal = "rechecked_none"
	heldBackSignalReadFailed    heldBackSignal = "read_failed"
)

// heldBackSignalOrder is the order pickHeldBackSignal reads heldBackSignalSQL's columns in.
var heldBackSignalOrder = [6]heldBackSignal{
	heldBackSignalRecentRun, heldBackSignalFinalizePending, heldBackSignalUnitHeartbeat,
	heldBackSignalUnitLease, heldBackSignalUnitDispatching, heldBackSignalWakeArmed,
}

func (signal heldBackSignal) label() synclog.Label {
	switch signal {
	case heldBackSignalNone:
		return synclog.LabelHeldBackNone
	case heldBackSignalRecentRun:
		return synclog.LabelHeldBackRecentRun
	case heldBackSignalFinalizePending:
		return synclog.LabelHeldBackFinalizePending
	case heldBackSignalUnitHeartbeat:
		return synclog.LabelHeldBackUnitHeartbeat
	case heldBackSignalUnitLease:
		return synclog.LabelHeldBackUnitLease
	case heldBackSignalUnitDispatching:
		return synclog.LabelHeldBackUnitDispatching
	case heldBackSignalWakeArmed:
		return synclog.LabelHeldBackWakeArmed
	case heldBackSignalRecheckedNone:
		return synclog.LabelHeldBackRecheckedNone
	case heldBackSignalReadFailed:
		return synclog.LabelHeldBackReadFailed
	}
	return synclog.LabelInvalid
}

func pickHeldBackSignal(probes [6]bool) heldBackSignal {
	for index, matched := range probes {
		if matched {
			return heldBackSignalOrder[index]
		}
	}
	return heldBackSignalRecheckedNone
}

// heldBackSignalSQL is the liveness test of olderRunsInBucketSQL for ONE run, each probe its own column, in
// heldBackSignalOrder. The probe text is olderRunsInBucketSQL's, with $7 (now) written $2 and $9 (the bound in
// seconds) written $3; TestHeldBackSignalProbesAreTheOlderRunsProbes pins that. A variable so the tests can plant a
// statement error; production never changes it.
var heldBackSignalSQL = `
SELECT
    run.created_at > $2::timestamptz - make_interval(secs => $3::double precision),
    EXISTS (
            SELECT 1 FROM public.sync_dispatch_outbox AS outbox
            WHERE outbox.sync_run_id = run.id AND outbox.kind = 'finalize_sync_run'
                AND outbox.created_at > $2::timestamptz - make_interval(secs => $3::double precision)
            LIMIT 1
        ),
    EXISTS (
            SELECT 1 FROM public.sync_run_units AS unit
            WHERE unit.sync_run_id = run.id AND unit.status = 'running'
                AND unit.last_heartbeat_at > $2::timestamptz - make_interval(secs => $3::double precision)
            LIMIT 1
        ),
    EXISTS (
            SELECT 1 FROM public.sync_run_units AS unit
            WHERE unit.sync_run_id = run.id AND unit.status = 'running'
                AND unit.lease_expires_at > $2::timestamptz
            LIMIT 1
        ),
    EXISTS (
            SELECT 1 FROM public.sync_run_units AS unit
            WHERE unit.sync_run_id = run.id AND unit.status = 'dispatching'
                AND unit.updated_at > $2::timestamptz - make_interval(secs => $3::double precision)
            LIMIT 1
        ),
    EXISTS (
            SELECT 1 FROM public.sync_dispatch_outbox AS wake
            WHERE wake.sync_run_id = run.id AND wake.kind = 'dispatch_sync_run'
                AND wake.status = 'pending' AND wake.last_error IS NULL
                AND wake.available_at > $2::timestamptz - make_interval(secs => $3::double precision)
                AND wake.available_at <= $2::timestamptz + make_interval(secs => $3::double precision)
            LIMIT 1
        )
FROM public.sync_runs AS run
WHERE run.id = $1::uuid`

// heldBackSignalStatementTimeout bounds the recheck, which runs while the guard holds the bucket locks. Its probes stop
// at their first row on one run, so it normally takes well under a millisecond.
const heldBackSignalStatementTimeout = "200ms"

// heldBackSignalSavepoint is the savepoint readHeldBackSignal opens and always ends before it returns.
const heldBackSignalSavepoint = "held_back_signal"

// readHeldBackSignal rechecks, after the decision is made, which liveness probe of the held-for run matches, with
// readOlderRuns' now and bound. It runs in the guard transaction; under READ COMMITTED each statement takes its own
// snapshot, so it can see a later state than olderRunsInBucketSQL did (rechecked_none). It is for the log line only:
// it reads in its own savepoint with a transaction-local statement_timeout, then ALWAYS rolls back to the savepoint
// (which undoes an error, a timeout and the SET LOCAL) and releases it, so the pass goes on in no subtransaction. A
// failed or timed-out read, or a failed ROLLBACK TO or RELEASE, gives read_failed; a run gone since the first read
// gives rechecked_none.
func readHeldBackSignal(ctx context.Context, tx pgx.Tx, runID string, now time.Time) heldBackSignal {
	if _, err := tx.Exec(ctx, "SAVEPOINT "+heldBackSignalSavepoint); err != nil {
		return heldBackSignalReadFailed
	}
	var probes [6]bool
	_, err := tx.Exec(ctx, `SELECT set_config('statement_timeout', $1, true)`, heldBackSignalStatementTimeout)
	if err == nil {
		err = tx.QueryRow(ctx, heldBackSignalSQL, runID, now, admissionLivenessBound().Seconds()).
			Scan(&probes[0], &probes[1], &probes[2], &probes[3], &probes[4], &probes[5])
	}
	if _, rollbackErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+heldBackSignalSavepoint); rollbackErr != nil {
		return heldBackSignalReadFailed
	}
	if _, releaseErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+heldBackSignalSavepoint); releaseErr != nil {
		return heldBackSignalReadFailed
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return heldBackSignalReadFailed
	}
	return pickHeldBackSignal(probes)
}

// heldBackRunPrefix is the first 8 characters of a held-for run id for the Info line, or none.
func heldBackRunPrefix(runID string) string {
	if runID == "" {
		return string(heldBackSignalNone)
	}
	return runID[:min(len(runID), 8)]
}
