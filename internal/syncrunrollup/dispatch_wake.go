package syncrunrollup

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// OutboxWakeupOnConflictSQL is the ON CONFLICT tail of the canonical
// sync_dispatch_outbox wakeup upsert (Python's upsert_outbox_wakeup): a row
// that exists moves to 'pending' at the EARLIER of its own and the new
// available_at, an expired claim is cleared, a live claim is kept, the
// dispatched-transport columns are reset, and a row parked at 'dispatched' with
// last_error='feature_disabled' is left completely untouched.
//
// It is the tail only, so that a producer whose INSERT half differs (a VALUES
// row, or a guarded INSERT ... SELECT) still shares one copy of the merge rule.
// It reads nothing but EXCLUDED, so it carries no parameters of its own.
const OutboxWakeupOnConflictSQL = `ON CONFLICT (sync_run_id, kind) DO UPDATE
SET status = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.status
        ELSE 'pending'
    END,
    available_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.available_at
        ELSE LEAST(public.sync_dispatch_outbox.available_at, EXCLUDED.available_at)
    END,
    dispatched_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_at
        ELSE NULL
    END,
    last_error = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.last_error
        ELSE NULL
    END,
    claim_token = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_token
        ELSE NULL
    END,
    claim_expires_at = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_expires_at
        ELSE NULL
    END,
    claim_transport = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_transport
        ELSE NULL
    END,
    claim_route_generation = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_route_generation
        ELSE NULL
    END,
    dispatched_transport = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_transport
        ELSE NULL
    END,
    dispatched_route_generation = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_route_generation
        ELSE NULL
    END,
    transport_job_id = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.transport_job_id
        ELSE NULL
    END,
    updated_at = EXCLUDED.updated_at`

// WakeDispatchSQL arms the run's dispatch_sync_run wakeup at $3 when a unit of
// the run has just left its concurrency slot (CHAOS-10013).
//
// # Why
//
// Dispatch is one pass per RUN. A pass claims units up to the cost-class cap of
// each (org, provider, cost class) bucket and re-arms itself for the units it
// held back at now + SYNC_DISPATCH_REDISPATCH_COUNTDOWN. Nothing else woke it:
// a unit's terminal commit re-armed only finalize_sync_run. So a medium bucket
// (cap 2) drained at two units per countdown however fast the units ran --
// measured 1.7-2.0 units/min on prod and bigboy with a 60 s countdown, a
// 330-unit github run taking 169 minutes for about one second of work per unit.
// This statement is the missing edge: the unit's slot is free, so the run's
// next pass is due now. The countdown stays as the safety net for a wake that
// never lands.
//
// # The guard
//
// The row is armed only while the run is open AND has a unit still waiting for
// a slot ('planned', or 'retrying' whose backoff the pass itself honours).
// Without the guard the last units of every run would each buy a pass that can
// find nothing to claim. A run with nothing waiting gets no row and no update:
// finalize_sync_run, which the same commit already armed, is what closes it.
//
// # Dedupe
//
// One row per (sync_run_id, kind) by the outbox's own unique key, merged by
// OutboxWakeupOnConflictSQL: a burst of completions in one run moves the SAME
// row to the earliest of their instants, it never adds rows.
const WakeDispatchSQL = `
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    created_at, updated_at
)
SELECT $1::uuid, run.org_id, run.id, 'dispatch_sync_run', 'pending',
    $3::timestamptz, 0, $3::timestamptz, $3::timestamptz
FROM public.sync_runs AS run
WHERE run.id = $2::uuid
  AND run.status NOT IN ('success', 'partial_failed', 'failed')
  AND EXISTS (
      SELECT 1
      FROM public.sync_run_units AS unit
      WHERE unit.sync_run_id = run.id
        AND unit.status IN ('planned', 'retrying')
  )
` + OutboxWakeupOnConflictSQL

// Execer is the one method WakeDispatch needs: *pgxpool.Pool and pgx.Tx both
// satisfy it.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// Wake paths: which writer freed the slot.
const (
	DispatchWakePathProviderUnit       = "provider_unit"
	DispatchWakePathLeaseRepair        = "lease_repair"
	DispatchWakePathUnreclaimableSweep = "unreclaimable_sweep"
	DispatchWakePathRouteUnavailable   = "route_unavailable"
	DispatchWakePathDispatchPass       = "dispatch_pass"
)

// DispatchWakeTimeout bounds one wake statement. The wake is best effort and
// runs on the caller's goroutine after its commit, so a slow or saturated
// outbox (the reconciler's materializer has met ctx deadlines on this table)
// must cost the caller at most this long; the countdown covers the miss.
const DispatchWakeTimeout = 5 * time.Second

// Wake outcomes.
const (
	DispatchWakeOutcomeArmed      = "armed"
	DispatchWakeOutcomeNotWaiting = "not_waiting"
	DispatchWakeOutcomeFailed     = "failed"
)

// WakeDispatch runs WakeDispatchSQL as its OWN statement and reports whether a
// wakeup was armed.
//
// Call it AFTER the transaction that freed the slot has committed, never inside
// it. The unit commit holds the unit row, the sync_runs row (Bump's lock-first
// step) and the finalize outbox row; adding the dispatch outbox row to that set
// would be a fourth lock on the hottest write path, against the relay and the
// reconciler's materializer that lock dispatch rows in their own order. Outside
// the transaction the wake can only be late, never wrong, and late is what the
// countdown already covers. For the same reason it returns no error: a failed
// wake is logged and counted, and the caller's commit stands.
func WakeDispatch(ctx context.Context, db Execer, syncRunID string, now time.Time, path string) bool {
	wakeCtx, cancel := context.WithTimeout(ctx, DispatchWakeTimeout)
	defer cancel()
	command, err := db.Exec(wakeCtx, WakeDispatchSQL, uuid.New(), syncRunID, now.UTC())
	if err != nil {
		dispatchWakeCounts.record(path, DispatchWakeOutcomeFailed)
		synclog.Default().Warn(ctx, synclog.MsgDispatchSyncRunUnitCompletionWakeFailed,
			synclog.Run(synclog.ParseID(syncRunID)),
			synclog.Text(synclog.KeyPath, synclog.ParseLabel(path)),
			synclog.Failure(err),
		)
		return false
	}
	if command.RowsAffected() == 0 {
		dispatchWakeCounts.record(path, DispatchWakeOutcomeNotWaiting)
		return false
	}
	dispatchWakeCounts.record(path, DispatchWakeOutcomeArmed)
	synclog.Default().Debug(ctx, synclog.MsgDispatchSyncRunWokenByUnitCompletion,
		synclog.Run(synclog.ParseID(syncRunID)),
		synclog.Text(synclog.KeyPath, synclog.ParseLabel(path)),
		synclog.Instant(synclog.KeyAvailableAt, now.UTC()),
	)
	return true
}

var (
	dispatchWakePaths = []string{
		DispatchWakePathDispatchPass, DispatchWakePathLeaseRepair, DispatchWakePathProviderUnit,
		DispatchWakePathRouteUnavailable, DispatchWakePathUnreclaimableSweep,
	}
	dispatchWakeOutcomes = []string{DispatchWakeOutcomeArmed, DispatchWakeOutcomeFailed, DispatchWakeOutcomeNotWaiting}
)

const dispatchWakeMetricName = "dev_health_sync_dispatch_woken_total"

// dispatchWakeCounts is process-wide for the reason the rollup counter is
// (providerfoundation.syncRunRollupBumpedMetrics): one HELP/TYPE per metric name
// however many component families a process runs. Every series of the closed
// vocabulary is written on each scrape, zeros included, so a scrape can tell
// "no wakes" from "not exposed".
var dispatchWakeCounts = &dispatchWakeCounter{counts: map[[2]string]uint64{}}

type dispatchWakeCounter struct {
	mu     sync.Mutex
	counts map[[2]string]uint64
}

func (counter *dispatchWakeCounter) record(path, outcome string) {
	if !slices.Contains(dispatchWakePaths, path) {
		path = "other"
	}
	counter.mu.Lock()
	defer counter.mu.Unlock()
	counter.counts[[2]string{path, outcome}]++
}

func (counter *dispatchWakeCounter) count(path, outcome string) uint64 {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	return counter.counts[[2]string{path, outcome}]
}

// WritePrometheus implements health.MetricsSource structurally.
func (counter *dispatchWakeCounter) WritePrometheus(writer io.Writer) error {
	counter.mu.Lock()
	snapshot := make(map[[2]string]uint64, len(counter.counts))
	for key, value := range counter.counts {
		snapshot[key] = value
	}
	counter.mu.Unlock()
	for _, path := range dispatchWakePaths {
		for _, outcome := range dispatchWakeOutcomes {
			key := [2]string{path, outcome}
			if _, ok := snapshot[key]; !ok {
				snapshot[key] = 0
			}
		}
	}
	keys := make([][2]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left][0] != keys[right][0] {
			return keys[left][0] < keys[right][0]
		}
		return keys[left][1] < keys[right][1]
	})
	if _, err := fmt.Fprintf(writer,
		"# HELP %s Dispatch wakeups requested when a sync unit left its concurrency slot, by writer path and outcome (armed, not_waiting: nothing in the run was waiting for a slot, failed: the wake statement errored and the redispatch countdown is the fallback) (CHAOS-10013).\n# TYPE %s counter\n",
		dispatchWakeMetricName, dispatchWakeMetricName); err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := fmt.Fprintf(writer, "%s{path=%q,outcome=%q} %d\n",
			dispatchWakeMetricName, key[0], key[1], snapshot[key]); err != nil {
			return err
		}
	}
	return nil
}

// DispatchWakeMetricsSource returns the process-wide wake counter, to register
// ONCE per process with the health registry.
func DispatchWakeMetricsSource() *dispatchWakeCounter { return dispatchWakeCounts }

// DispatchWakeCount reads one series of the wake counter, for tests in other
// packages that assert the wake happened through the counter an operator reads.
func DispatchWakeCount(path, outcome string) uint64 { return dispatchWakeCounts.count(path, outcome) }
