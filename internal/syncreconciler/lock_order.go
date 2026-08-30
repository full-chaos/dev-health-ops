package syncreconciler

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"github.com/jackc/pgx/v5"
)

// lockSyncRunUnitsAscending takes the sync_run_units row lock for every
// given unit id, deduplicated, in ascending sorted order -- before ANY
// real write or syncrunrollup.Bump call in the caller's transaction
// (codex round 4, P1). LeaseRepair.Step and UnreclaimableSweep.Step both
// call this once, with every candidate unit id that pass will touch,
// before their write loop.
//
// Why this is needed even after round 3's fix (candidates sorted by
// ascending sync_run_id before the write-then-Bump loop): Bump's own
// `SELECT ... FOR UPDATE` on sync_runs holds that row's lock for the REST
// of the transaction, not just for the duration of the Bump call. So once
// the first candidate in a run has been written and Bump'd, this
// transaction already holds that run's lock for every later statement --
// including a SECOND candidate's own unit-row write, if two candidates in
// one pass happen to belong to the SAME run. That second write now
// happens WHILE holding a run lock, i.e. run-before-unit for that one
// candidate -- exactly the inversion round 3 fixed against every
// single-run terminal writer elsewhere (PostgresRepository.Fail,
// terminalizeUnroutableUnits, etc.), which always lock their one unit row
// before ever touching a run row: a concurrent single-run writer already
// holding THAT unit's lock and waiting on the run (held by this pass
// since its first candidate), against this pass waiting on that unit
// while already holding the run, is the ABBA cycle codex caught.
//
// Locking every candidate's unit row here, up front, in one fixed order,
// before this transaction EVER touches a run lock, restores the
// invariant for the WHOLE transaction at once: by the time the first
// Bump call takes any run lock, every unit lock this transaction will
// ever need is already held, so it can never later want a NEW unit lock
// while holding a run lock. Every single-run writer already satisfies
// "unit before run" trivially (it only ever takes one of each); this
// generalizes that to "all of this transaction's units before any of its
// runs", which subsumes it.
//
// Ascending unit id (a key independent of either caller's own candidate
// order) so the two reconciler paths agree on ONE order at the unit
// granularity too -- the same "acquire every lock in one global order"
// rule round 3 already applies at the run granularity via the
// ascending-sync_run_id candidate sort.
//
// Row-by-row rather than one `WHERE id = ANY($1) ORDER BY id FOR UPDATE`
// query on purpose: Postgres does not guarantee a LockRows node acquires
// locks in the ORDER BY order for every plan shape, so only an explicit
// per-row loop in a client-chosen order is an unambiguous guarantee.
func lockSyncRunUnitsAscending(ctx context.Context, tx pgx.Tx, unitIDs []string) error {
	seen := make(map[string]bool, len(unitIDs))
	sorted := make([]string, 0, len(unitIDs))
	for _, id := range unitIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)
	for _, id := range sorted {
		var locked int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM public.sync_run_units WHERE id = $1 FOR UPDATE`, id).Scan(&locked); err != nil {
			return err
		}
	}
	return nil
}

// unreclaimableBucketAdvisoryID computes the SAME advisory-lock key as
// syncdispatchruntime's (unexported) bucketAdvisoryLockKey and this
// package's own leaseRepairBucketAdvisoryID (lease_repair.go): SHA-256 of
// "orgID:provider:costClass", low 63 bits. Verified identical formula in
// both existing copies -- this is a THIRD literal copy of the same twelve
// lines, on purpose: reaching into syncdispatchruntime's unexported
// bucketAdvisoryLockKey, or refactoring LeaseRepair's already-tested
// leaseRepairBucketAdvisoryID mid-review, is a bigger and riskier change
// than one more copy of a formula three independent places (this package
// twice, syncdispatchruntime once -- Python has its own fourth verbatim
// copy) already deliberately maintain, by design, as a shared numeric
// CONTRACT rather than shared code.
func unreclaimableBucketAdvisoryID(orgID, provider, costClass string) int64 {
	digest := sha256.Sum256([]byte(orgID + ":" + provider + ":" + costClass))
	return int64(binary.BigEndian.Uint64(digest[:8]) & ((uint64(1) << 63) - 1))
}

// acquireUnreclaimableBucketLocks takes the SAME sorted per-(orgID,
// provider, costClass) advisory lock syncdispatchruntime's AuthorizeRun
// and this package's own LeaseRepair.Step already take, before
// UnreclaimableSweep.Step locks or writes any sync_run_units row (codex
// round 5, P1).
//
// Without this, dispatch's claimUnits -- a BULK multi-row UPDATE matching
// every eligible unit of one sync_run_id in a single statement, with NO
// explicit row-lock order of its own (Postgres locks matching rows in
// whatever order its query plan finds them) -- and this sweep's
// lockSyncRunUnitsAscending (ascending unit id, round 4) can each hold one
// contested unit row and wait on the other: dispatch's single statement
// locks unit B first, then blocks wanting unit A (already held by this
// sweep); this sweep, having locked A first per its own ascending order,
// blocks wanting unit B (now held by dispatch). Neither side's row-lock
// order can fix this alone, because they are two DIFFERENT, independently
// evolving code paths with no way to agree on one row order between them
// -- the same reason dispatch's own AuthorizeRun and LeaseRepair.Step
// don't try to out-order each other on ROWS at all, and instead take a
// shared advisory lock, sorted, before touching any row: whichever side
// gets there first holds total mutual exclusion over the bucket for its
// whole transaction, so the two paths can never even be in a position to
// contend on the same row's lock concurrently in the first place.
func acquireUnreclaimableBucketLocks(ctx context.Context, tx pgx.Tx, candidates []unreclaimableCandidate) error {
	type bucket struct{ orgID, provider, costClass string }
	seen := make(map[bucket]bool, len(candidates))
	ordered := make([]bucket, 0, len(candidates))
	for _, candidate := range candidates {
		key := bucket{candidate.orgID, candidate.provider, candidate.costClass}
		if seen[key] {
			continue
		}
		seen[key] = true
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].orgID != ordered[right].orgID {
			return ordered[left].orgID < ordered[right].orgID
		}
		if ordered[left].provider != ordered[right].provider {
			return ordered[left].provider < ordered[right].provider
		}
		return ordered[left].costClass < ordered[right].costClass
	})
	for _, entry := range ordered {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)",
			unreclaimableBucketAdvisoryID(entry.orgID, entry.provider, entry.costClass)); err != nil {
			return err
		}
	}
	return nil
}
