package syncrunrollup

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// SlotFree names one unit that left its concurrency slot: its run and its (org, provider, cost class) bucket.
type SlotFree struct {
	RunID     string
	OrgID     string
	Provider  string
	CostClass string
}

// BucketWaitingRunPredicate is the SQL condition, on sync_runs AS run, of an open run of org `org` whose reference
// discovery succeeded and that has a unit waiting in bucket (provider, costClass) at `now`: planned, or retrying and
// due, among its first 256 planned or retrying units (the dispatch guard's older-run read bound). The arguments are
// the statement's placeholders. A run blocked before the guard (discovery not done) is not a waiter, so a wake never
// lands on a pass that cannot claim.
func BucketWaitingRunPredicate(org, provider, costClass, now string) string {
	return `run.status NOT IN ('success', 'partial_failed', 'failed')
      AND run.org_id = ` + org + `
      AND EXISTS (
          SELECT 1 FROM public.sync_run_reference_discoveries AS discovery
          WHERE discovery.sync_run_id = run.id AND discovery.status = 'success'
          LIMIT 1
      )
      AND EXISTS (
          SELECT 1
          FROM (
              SELECT unit.provider, unit.cost_class, unit.status, unit.available_at
              FROM public.sync_run_units AS unit
              WHERE unit.sync_run_id = run.id AND unit.status IN ('planned', 'retrying')
              LIMIT 256
          ) AS seen
          WHERE seen.provider = ` + provider + ` AND seen.cost_class = ` + costClass + `
              AND (seen.status = 'planned' OR seen.available_at <= ` + now + `)
      )`
}

// WakeBucketHeadSQL arms the dispatch wakeup of one waiting run of a bucket, chosen against the reference run $6.
//
// $7 = false (slot free): the bucket head. The oldest waiting run of the reference run's class (backfill or not, the
// rule of the guard's readRunOrder), else the oldest waiting run of the other class. A slot a non-backfill unit freed
// goes to the oldest non-backfill waiter, a slot a backfill unit freed to the oldest backfill waiter: the slot split
// of the backfill share (CHAOS-14211) stays as the units left it, and each class is served oldest first (CHAOS-8880).
//
// $7 = true (hand-off): the oldest waiting run of the SAME class that is newer than the reference run. Each hand-off
// moves strictly down the order, so a chain of passes that cannot use a slot ends.
var WakeBucketHeadSQL = `
WITH ref AS (
    SELECT run.id, run.created_at, (run.triggered_by = 'backfill' OR run.mode = 'backfill') AS backfill
    FROM public.sync_runs AS run
    WHERE run.id = $6::uuid
)
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    created_at, updated_at
)
SELECT $1::uuid, head.org_id, head.id, 'dispatch_sync_run', 'pending',
    $5::timestamptz, 0, $5::timestamptz, $5::timestamptz
FROM ref
CROSS JOIN LATERAL (
    SELECT run.id, run.org_id
    FROM public.sync_runs AS run
    WHERE ` + BucketWaitingRunPredicate("$2", "$3", "$4", "$5::timestamptz") + `
      AND (NOT $7::boolean OR (
          (run.triggered_by = 'backfill' OR run.mode = 'backfill') = ref.backfill
          AND (run.created_at, run.id) > (ref.created_at, ref.id)
      ))
    ORDER BY ((run.triggered_by = 'backfill' OR run.mode = 'backfill') IS DISTINCT FROM ref.backfill), run.created_at, run.id
    LIMIT 1
) AS head
` + OutboxWakeupOnConflictSQL

// WakeBucketHead wakes the head of the bucket a unit of freedRunID just left (see WakeBucketHeadSQL). Like
// WakeDispatch it runs after the freeing commit, as its own bounded statement, and never fails the caller.
func WakeBucketHead(ctx context.Context, db Execer, slot SlotFree, now time.Time) bool {
	return runWake(ctx, db, slot.RunID, now, DispatchWakePathBucketHead, WakeBucketHeadSQL,
		uuid.New(), slot.OrgID, slot.Provider, slot.CostClass, now.UTC(), slot.RunID, false)
}

// WakeNextInClass wakes the oldest waiting run of selfRunID's class that is newer than it, in the bucket a pass of
// selfRunID was granted slots it did not use.
func WakeNextInClass(ctx context.Context, db Execer, orgID, provider, costClass, selfRunID string, now time.Time) bool {
	return runWake(ctx, db, selfRunID, now, DispatchWakePathHandOff, WakeBucketHeadSQL,
		uuid.New(), orgID, provider, costClass, now.UTC(), selfRunID, true)
}

// WakeAfterSlotFree is the wake every writer that moves a unit out of its slot runs after its commit: the bucket head
// first, then the freeing run itself (CHAOS-10013), whose pass holds the slot back for the head when the head is older.
func WakeAfterSlotFree(ctx context.Context, db Execer, slot SlotFree, now time.Time, path string) {
	WakeBucketHead(ctx, db, slot, now)
	WakeDispatch(ctx, db, slot.RunID, now, path)
}
