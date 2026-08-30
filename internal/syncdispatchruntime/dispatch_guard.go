package syncdispatchruntime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sort"
	"time"

	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// sync_run_units statuses this file's guard/claim logic switches on --
// success/failed already exist as syncRunUnitStatusSuccess/
// syncRunUnitStatusFailed (native_finalize_sync_run.go). Defined here,
// alongside their first native callers, matching that file's own pattern.
const (
	syncRunUnitStatusPlanned     = "planned"
	syncRunUnitStatusDispatching = "dispatching"
	syncRunUnitStatusRunning     = "running"
	syncRunUnitStatusRetrying    = "retrying"
)

// dispatchBucket ports guard.py's (org_id, provider, cost_class) tuple key
// verbatim -- the unit of concurrency admission and advisory locking.
type dispatchBucket struct {
	orgID     string
	provider  string
	costClass string
}

// dispatchGuardUnit is the narrow sync_run_units projection authorizeRun
// needs -- id/status/bucket dimensions/updated_at/available_at, ordered by
// id for a stable cap suffix (guard.py: "ordered by id (stable ordering for
// cap suffix)").
type dispatchGuardUnit struct {
	id          string
	status      string
	provider    string
	costClass   string
	updatedAt   time.Time
	availableAt *time.Time
}

// guardDecision ports guard.py's GuardDecision dataclass verbatim. Three
// shapes (see dispatch_guard.go's authorizeRun and guard.py's module
// docstring):
//
//   - Total-cap hard-deny: allowed=false, concurrencyCapped=false,
//     cappedUnitIDs lists every unit past the org's absolute cap. The
//     caller MUST mark the run FAILED.
//   - Concurrency partial-cap: allowed=true, concurrencyCapped=true,
//     cappedUnitIDs lists units held back by a saturated bucket. The
//     caller MUST leave them PLANNED and schedule a delayed redispatch,
//     MUST NOT mark the run FAILED.
//   - Full allow: allowed=true, concurrencyCapped=false.
//
// slotHeadroom is populated on every allow decision (never on hard-deny,
// matching Python: the hard-deny branch returns before any bucket is
// visited) -- the only sanctioned channel BudgetGuard's surplus retry uses
// to admit extra work without stepping around this cap (CHAOS-3465).
type guardDecision struct {
	allowed           bool
	reason            string
	cappedUnitIDs     []string
	concurrencyCapped bool
	slotHeadroom      map[dispatchBucket]int
}

// defaultSyncRunMaxUnits mirrors guard.py's _resolve_total_unit_cap default
// (SYNC_RUN_MAX_UNITS, fallback 1000).
func defaultSyncRunMaxUnits() int {
	return envPositiveInt("SYNC_RUN_MAX_UNITS", 1000)
}

// syncUnitConcurrencyPerBucket mirrors guard.py's
// SYNC_UNIT_CONCURRENCY_PER_BUCKET (fallback 8).
func syncUnitConcurrencyPerBucket() int {
	return envPositiveInt("SYNC_UNIT_CONCURRENCY_PER_BUCKET", 8)
}

// staleDispatchSeconds mirrors sync_units._stale_dispatch_seconds() /
// guard.py's local _stale_dispatch_seconds_guard() (same env var, same
// 900s default, duplicated in Python itself to avoid a circular import --
// this port has no such constraint but keeps one definition, referenced by
// both the guard and _claim_units' own stale-reclaim window).
func staleDispatchSeconds() time.Duration {
	return time.Duration(envPositiveInt("SYNC_UNIT_DISPATCH_STALE_SECONDS", 900)) * time.Second
}

// authorizeRun ports DispatchGuard.authorize_run verbatim. Unlike Python's
// static method (which independently re-queries SyncRun so it stays
// callable from anywhere), this takes orgID from the run its caller already
// loaded in the SAME transaction dispatch_sync_run holds across
// authorize->claim -- Python's own re-query cannot observe anything this
// transaction hasn't, so skipping it changes no observable behavior, only
// which frame issues the (redundant, in Python) lookup.
func authorizeRun(ctx context.Context, tx pgx.Tx, logger *slog.Logger, orgID, runID string, now time.Time) (guardDecision, error) {
	if logger == nil {
		logger = slog.Default()
	}
	units, err := loadDispatchGuardUnits(ctx, tx, runID)
	if err != nil {
		return guardDecision{}, err
	}

	// CHAOS-4586 (codex round 6, P1): the bucket-lock acquisition below
	// USED to sit after the total-unit-cap check, so a run over cap
	// returned "denied" before ever reaching it -- denyRun's subsequent
	// hasActive branch then bulk-updates this run's sync_run_units
	// (failPlannedUnits/failStaleDispatchingUnits, both plain WHERE
	// sync_run_id=$1 predicates matching potentially many rows, with NO
	// explicit row-lock order of their own) with NO bucket lock held at
	// all. UnreclaimableSweep.Step's own bucket lock (round 5) only
	// protects against dispatch passes that reach THIS lock; it protects
	// nothing against a hard-cap denial that returns before this. Moving
	// bucket-lock acquisition here, before EVERY early return
	// (including the hard-cap one below), makes it unconditional for the
	// whole function: whichever guardDecision this returns, the caller's
	// SAME transaction already holds every bucket this run's units belong
	// to, for the rest of that transaction -- covering denyRun's bulk
	// writes too, not just the allowed-path's own claimUnits.
	//
	// candidatesByBucket/deferredBuckets/allBuckets do not depend on
	// totalCap or anything computed below this line -- only on units,
	// orgID and now -- so this reordering changes no computed value, only
	// when the (harmless, already-idempotent) lock acquisition happens.
	staleCutoff := now.Add(-staleDispatchSeconds())

	// CHAOS-4586 (codex round 7, P2; round 8, P2 narrowed this): the lock
	// set must cover every bucket a MUTATING status could touch -- broader
	// than candidatesByBucket/deferredBuckets below, which apply
	// eligibility filters that exist for the CONCURRENCY-CAP decision loop
	// further down, not for locking completeness. Round 7 fixed this by
	// locking EVERY unit's bucket unconditionally; round 8 found that
	// over-broad -- a run with terminal (success/failed) or live RUNNING
	// units in one bucket and pending work in another held BOTH buckets
	// for this whole transaction, needlessly blocking unrelated runs on
	// the never-touched bucket, since neither denyRun's bulk writes nor
	// claimUnits ever mutate a terminal or RUNNING row.
	//
	// lockBucketsByUnit is therefore built in the SAME loop as
	// candidatesByBucket/deferredBuckets, adding a unit to it exactly when
	// some later write in THIS transaction could touch that row:
	//   - PLANNED: claimUnits' plain-claim UPDATE, and denyRun's
	//     failPlannedUnits (`status IN (planned, retrying)`, no other
	//     condition).
	//   - DISPATCHING, stale: claimUnits' stale-reclaim UPDATE, and
	//     denyRun's failStaleDispatchingUnits (`status = dispatching AND
	//     updated_at <= staleCutoff`). Fresh DISPATCHING is a capacity
	//     consumer ONLY -- neither write ever matches it -- so it does NOT
	//     need a lock.
	//   - RETRYING, ANY available_at (including nil): denyRun's
	//     failPlannedUnits matches every retrying row with NO available_at
	//     condition at all, so this must be locked regardless of whether
	//     it is a concurrency-cap candidate, a not-yet-due deferral, or
	//     the anomalous nil-available_at shape codex round 7 caught
	//     candidatesByBucket/deferredBuckets silently dropping.
	//   - RUNNING and terminal (SUCCESS/FAILED): never written by
	//     anything in this transaction -- no lock needed.
	lockBucketsByUnit := map[dispatchBucket][]dispatchGuardUnit{}
	candidatesByBucket := map[dispatchBucket][]dispatchGuardUnit{}
	deferredBuckets := map[dispatchBucket]bool{}
	for _, unit := range units {
		bucket := dispatchBucket{orgID: orgID, provider: unit.provider, costClass: unit.costClass}
		switch unit.status {
		case syncRunUnitStatusPlanned:
			candidatesByBucket[bucket] = append(candidatesByBucket[bucket], unit)
			lockBucketsByUnit[bucket] = append(lockBucketsByUnit[bucket], unit)
		case syncRunUnitStatusDispatching:
			if !unit.updatedAt.After(staleCutoff) {
				candidatesByBucket[bucket] = append(candidatesByBucket[bucket], unit)
				lockBucketsByUnit[bucket] = append(lockBucketsByUnit[bucket], unit)
			}
			// Fresh DISPATCHING -> consumer only (counted in active-count
			// below), never written by this pass -- no lock needed.
		case syncRunUnitStatusRetrying:
			lockBucketsByUnit[bucket] = append(lockBucketsByUnit[bucket], unit)
			if unit.availableAt != nil {
				if !unit.availableAt.After(now) {
					candidatesByBucket[bucket] = append(candidatesByBucket[bucket], unit)
				} else {
					deferredBuckets[bucket] = true
				}
			}
			// availableAt == nil: lock-worthy (above, codex round 7) but
			// not a concurrency-cap candidate or deferred bucket -- an
			// anomalous row shape denyRun could still write.
			// RUNNING (any age) -> capacity consumer only, never a candidate (F2)
			// and never written by anything in this transaction -- no lock needed.
			// SUCCESS / FAILED -> terminal, ignored, never written again.
		}
	}

	if err := acquireBucketAdvisoryLocks(ctx, tx, sortedDispatchBuckets(lockBucketsByUnit, nil)); err != nil {
		return guardDecision{}, err
	}

	// allBuckets stays scoped to candidatesByBucket/deferredBuckets on
	// purpose: it drives the per-bucket CONCURRENCY-CAP decision loop
	// below, which must only ever consider eligible buckets -- locking
	// completeness (above) and concurrency-cap eligibility (here) are
	// different concerns with different correct scopes; conflating them
	// would either under-lock (the round-7 bug) or over-restrict slot
	// accounting to buckets that were never real candidates.
	allBuckets := sortedDispatchBuckets(candidatesByBucket, deferredBuckets)

	// Python: any resolution failure (missing org, missing tier_limits
	// table, a malformed override) falls back to the env default
	// unconditionally (CHAOS-2580) -- ResolveMaxSyncUnitsCap's own doc
	// warns callers wanting that parity must substitute the default
	// themselves; this is that substitution.
	totalCap, err := scheduledsync.ResolveMaxSyncUnitsCap(ctx, tx, orgID, defaultSyncRunMaxUnits())
	if err != nil {
		totalCap = defaultSyncRunMaxUnits()
	}
	if len(units) > totalCap {
		capped := make([]string, 0, len(units)-totalCap)
		for _, unit := range units[totalCap:] {
			capped = append(capped, unit.id)
		}
		return guardDecision{
			allowed:       false,
			reason:        fmt.Sprintf("sync run unit cap exceeded: %d/%d", len(units), totalCap),
			cappedUnitIDs: capped,
		}, nil
	}

	concurrencyCap := syncUnitConcurrencyPerBucket()
	var cappedUnitIDs []string
	slotHeadroom := make(map[dispatchBucket]int, len(allBuckets))
	for _, bucket := range allBuckets {
		bucketCandidates := candidatesByBucket[bucket]
		activeCount, err := countActiveBucketUnits(ctx, tx, bucket, staleCutoff, now)
		if err != nil {
			return guardDecision{}, err
		}

		var reclaims, newWork []dispatchGuardUnit
		for _, unit := range bucketCandidates {
			if unit.status == syncRunUnitStatusDispatching {
				reclaims = append(reclaims, unit)
			} else {
				newWork = append(newWork, unit)
			}
		}
		// CHAOS-3990: reclaims are ordered oldest-stale-first ahead of new
		// work, never exempted from the cap -- see guard.py:262-286 for the
		// full starvation-fix rationale this ordering exists to preserve.
		sort.SliceStable(reclaims, func(i, j int) bool {
			if !reclaims[i].updatedAt.Equal(reclaims[j].updatedAt) {
				return reclaims[i].updatedAt.Before(reclaims[j].updatedAt)
			}
			return reclaims[i].id < reclaims[j].id
		})

		allowedSlots := concurrencyCap - activeCount
		if allowedSlots < 0 {
			allowedSlots = 0
		}
		prioritized := append(append([]dispatchGuardUnit{}, reclaims...), newWork...)
		var cappedNew, cappedStale int
		if len(prioritized) > allowedSlots {
			for _, unit := range prioritized[allowedSlots:] {
				cappedUnitIDs = append(cappedUnitIDs, unit.id)
				if unit.status == syncRunUnitStatusDispatching {
					cappedStale++
				} else {
					cappedNew++
				}
			}
		}
		headroom := allowedSlots - len(prioritized)
		if headroom < 0 {
			headroom = 0
		}
		slotHeadroom[bucket] = headroom

		reclaimedStale := len(reclaims)
		if reclaimedStale > allowedSlots {
			reclaimedStale = allowedSlots
		}
		emitBucketDecision(ctx, logger, bucket, activeCount, reclaimedStale, cappedNew, cappedStale, headroom)
	}

	if len(cappedUnitIDs) > 0 {
		return guardDecision{
			allowed:           true,
			reason:            fmt.Sprintf("sync unit concurrency cap exceeded: %d capped", len(cappedUnitIDs)),
			cappedUnitIDs:     cappedUnitIDs,
			concurrencyCapped: true,
			slotHeadroom:      slotHeadroom,
		}, nil
	}
	return guardDecision{allowed: true, slotHeadroom: slotHeadroom}, nil
}

// loadDispatchGuardUnits loads this run's units ordered by id (stable
// ordering for the total-cap suffix, guard.py:147-153).
//
// Error classification: a query/scan failure here is a bare execution
// failure indistinguishable from any other Postgres blip mid-dispatch --
// classified retryable via ErrDiscoveryTransientFailure's SAME reasoning
// (CHAOS-4175 round 3), reused here rather than minted again narrowly for
// dispatch, since the retry/backoff state machine this family's Dispatch()
// routes into is the identical one round 3 fixed for reference_discovery.
func loadDispatchGuardUnits(ctx context.Context, tx pgx.Tx, runID string) ([]dispatchGuardUnit, error) {
	rows, err := tx.Query(ctx, `
SELECT id::text, status, provider, cost_class, updated_at, available_at
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid
ORDER BY id`, runID)
	if err != nil {
		return nil, fmt.Errorf("%w: load dispatch guard units: %w", ErrDiscoveryTransientFailure, err)
	}
	defer rows.Close()
	var units []dispatchGuardUnit
	for rows.Next() {
		var unit dispatchGuardUnit
		if err := rows.Scan(&unit.id, &unit.status, &unit.provider, &unit.costClass, &unit.updatedAt, &unit.availableAt); err != nil {
			return nil, fmt.Errorf("%w: scan dispatch guard unit: %w", ErrDiscoveryTransientFailure, err)
		}
		units = append(units, unit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate dispatch guard units: %w", ErrDiscoveryTransientFailure, err)
	}
	return units, nil
}

// countActiveBucketUnits counts the capacity-CONSUMER set for one bucket
// across ALL runs (guard.py:230-260): fresh DISPATCHING (updated_at >
// staleCutoff) OR live RUNNING (lease_expires_at NULL, unknown/
// pre-migration and therefore live, OR lease_expires_at > now). RETRYING
// never consumes capacity. Consumer and candidate sets are disjoint by
// construction; no subtraction is performed.
//
// Error classification: same as loadDispatchGuardUnits -- a bare query
// execution failure, retryable.
func countActiveBucketUnits(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, staleCutoff, now time.Time) (int, error) {
	var count int
	err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.sync_run_units
WHERE org_id = $1 AND provider = $2 AND cost_class = $3
  AND (
        (status = $4 AND updated_at > $5)
     OR (status = $6 AND (lease_expires_at IS NULL OR lease_expires_at > $7))
      )`,
		bucket.orgID, bucket.provider, bucket.costClass,
		syncRunUnitStatusDispatching, staleCutoff,
		syncRunUnitStatusRunning, now,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("%w: count active bucket units: %w", ErrDiscoveryTransientFailure, err)
	}
	return count, nil
}

// sortedDispatchBuckets ports guard.py's `sorted(set(candidates_by_bucket) |
// deferred_buckets)` -- deterministic ordering is what makes the advisory
// lock acquisition below deadlock-free when two dispatchers race on the
// same bucket set.
func sortedDispatchBuckets(candidatesByBucket map[dispatchBucket][]dispatchGuardUnit, deferredBuckets map[dispatchBucket]bool) []dispatchBucket {
	seen := make(map[dispatchBucket]bool, len(candidatesByBucket)+len(deferredBuckets))
	for bucket := range candidatesByBucket {
		seen[bucket] = true
	}
	for bucket := range deferredBuckets {
		seen[bucket] = true
	}
	buckets := make([]dispatchBucket, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool {
		a, b := buckets[i], buckets[j]
		if a.orgID != b.orgID {
			return a.orgID < b.orgID
		}
		if a.provider != b.provider {
			return a.provider < b.provider
		}
		return a.costClass < b.costClass
	})
	return buckets
}

// bucketAdvisoryLockKey ports guard.py's _bucket_advisory_lock_key
// verbatim: SHA-256 of the canonical bucket string, truncated to 63 bits so
// it fits a signed Postgres bigint.
func bucketAdvisoryLockKey(bucket dispatchBucket) int64 {
	raw := []byte(bucket.orgID + ":" + bucket.provider + ":" + bucket.costClass)
	digest := sha256.Sum256(raw)
	keyUint := binary.BigEndian.Uint64(digest[:8])
	return int64(keyUint & ((1 << 63) - 1))
}

// BucketAdvisoryLockKeyForTest exposes bucketAdvisoryLockKey's exact
// formula across the package boundary, for ONE purpose only: a
// cross-package contract test in internal/syncreconciler
// (bucket_advisory_lock_contract_test.go, CHAOS-4586 codex round 5) proving
// that package's two independent copies of this same formula
// (leaseRepairBucketAdvisoryID, unreclaimableBucketAdvisoryID) compute the
// IDENTICAL numeric key this package does, for the same (orgID, provider,
// costClass) -- the shared invariant that lets LeaseRepair.Step,
// UnreclaimableSweep.Step and this package's own AuthorizeRun achieve
// mutual exclusion on a bucket via pg_advisory_xact_lock, despite living in
// separate Go packages with no shared symbol (Python needs no such test:
// guard.py's _bucket_advisory_lock_key is ONE function, imported and
// reused by both dispatch and lease-repair there -- only the Go ports
// duplicated it, once per package, because dispatchBucket's fields are
// unexported and Go has no equivalent of "import one private helper").
// Every real caller in this package still goes through the unexported
// bucketAdvisoryLockKey(dispatchBucket) above; this wrapper exists only so
// the test does not need dispatchBucket exported too.
func BucketAdvisoryLockKeyForTest(orgID, provider, costClass string) int64 {
	return bucketAdvisoryLockKey(dispatchBucket{orgID: orgID, provider: provider, costClass: costClass})
}

// acquireBucketAdvisoryLocks ports _acquire_bucket_advisory_locks. Buckets
// must already be sorted (sortedDispatchBuckets) to prevent deadlocks.
// Unlike Python, which no-ops on a non-Postgres dialect for its SQLite test
// suite, this port always targets Postgres -- there is no SQLite test
// backend in this codebase to no-op for.
//
// Error classification: a bare query execution failure, retryable (same
// reasoning as loadDispatchGuardUnits).
func acquireBucketAdvisoryLocks(ctx context.Context, tx pgx.Tx, buckets []dispatchBucket) error {
	for _, bucket := range buckets {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, bucketAdvisoryLockKey(bucket)); err != nil {
			return fmt.Errorf("%w: acquire bucket advisory lock: %w", ErrDiscoveryTransientFailure, err)
		}
	}
	return nil
}

// emitBucketDecision ports _emit_bucket_decision: a structured log line
// plus span attributes/event on the CURRENT span (set by
// spanForCoordinatorJob, already open for the whole dispatch job) --
// telemetry only, never gates dispatch, matching Python's own
// `except Exception: return` around its span calls (a span-attribute
// failure here is likewise swallowed, not propagated: attribute.String/Int
// on a Go span cannot fail the way Python's dynamic OTel calls could, so
// there is nothing to catch, but the log call is likewise best-effort).
func emitBucketDecision(ctx context.Context, logger *slog.Logger, bucket dispatchBucket, activeCount, reclaimedStale, cappedNew, cappedStale, slotHeadroom int) {
	bucketLabel := bucket.orgID + "/" + bucket.provider + "/" + bucket.costClass
	logger.InfoContext(ctx, "dispatch_guard.bucket_decision",
		slog.String("bucket", bucketLabel),
		slog.Int("guard.active_count", activeCount),
		slog.Int("guard.reclaimed_stale", reclaimedStale),
		slog.Int("guard.capped_new", cappedNew),
		slog.Int("guard.capped_stale", cappedStale),
		slog.Int("guard.slot_headroom", slotHeadroom),
	)
	span := oteltrace.SpanFromContext(ctx)
	if span == nil || !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("guard.bucket", bucketLabel),
		attribute.Int("guard.active_count", activeCount),
		attribute.Int("guard.reclaimed_stale", reclaimedStale),
		attribute.Int("guard.capped_new", cappedNew),
		attribute.Int("guard.capped_stale", cappedStale),
		attribute.Int("guard.slot_headroom", slotHeadroom),
	}
	span.SetAttributes(attrs...)
	span.AddEvent("dispatch_guard.bucket_decision", oteltrace.WithAttributes(attrs...))
}
