package syncdispatchruntime

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
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
	// authorizedUnitIDs is the EXACT set of units this pass's capacity
	// decision was computed over: every unit the snapshot classified as a
	// candidate, in a bucket this transaction locked and cap-checked.
	//
	// CHAOS-4605: it exists because the decision set and the claim set were
	// two DIFFERENT reads. authorizeRun classifies units once, at the top of
	// dispatch_sync_run's transaction; claimUnits re-derives its own claim
	// set from a FRESH read of the same table many statements later, under
	// READ COMMITTED and against a LATER `now`. Anything that becomes
	// claimable in between -- a RETRYING unit whose available_at falls
	// inside that window, or a RUNNING unit a concurrent LeaseRepair.Step
	// converted to a due RETRYING one -- was claimed with no cap check and,
	// in the RUNNING case, in a bucket this transaction never even locked.
	//
	// Widening the lock set does not close it: loadDispatchGuardUnits runs
	// BEFORE acquireBucketAdvisoryLocks, so a writer that already holds a
	// bucket lock commits into the gap and the lock this function finally
	// acquires certifies an already-stale snapshot. Pinning the claim to
	// this set does close it, in the only direction that is safe: a unit
	// that became claimable mid-pass is left for the NEXT pass, which
	// snapshots, locks and cap-checks it properly.
	//
	// Populated on every ALLOW decision and never on the hard-deny return,
	// matching slotHeadroom's own convention (the hard-deny branch returns
	// before any bucket is visited and its caller never reaches claimUnits).
	authorizedUnitIDs map[string]bool
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

// EffectiveAdmissionCaps reports the admission cap the dispatch guard applies
// per cost class right now (the class's budget limit, lowered by
// SYNC_UNIT_CONCURRENCY_PER_BUCKET) and that clamp itself, so a process can log
// the numbers it actually enforces at start (CHAOS-7881). Class names are the
// closed set of the budget table; no tenant data.
func EffectiveAdmissionCaps() (clamp int, caps map[string]int) {
	snapshot := readAdmissionCaps()
	return snapshot.clamp, snapshot.caps
}

// admissionCaps is ONE read of SYNC_UNIT_CONCURRENCY_PER_BUCKET and everything derived from it: the clamp, its source, and each class's
// cap and cap source. The start line is built from one snapshot, so a variable that changes while the line is built cannot give a
// cap derived with one clamp next to a reported clamp of another.
type admissionCaps struct {
	clamp       int
	clampSource string
	caps        map[string]int
	capSources  map[string]string
}

func readAdmissionCaps() admissionCaps {
	clamp, fromEnv := envPositiveIntFrom("SYNC_UNIT_CONCURRENCY_PER_BUCKET", 8)
	snapshot := admissionCaps{clamp: clamp, clampSource: clampSourceDefault,
		caps: make(map[string]int, 3), capSources: make(map[string]string, 3)}
	if fromEnv {
		snapshot.clampSource = clampSourceEnv
	}
	for _, class := range []string{"light", "medium", "heavy"} {
		snapshot.caps[class], snapshot.capSources[class] = capAndSourceForClamp(class, clamp)
	}
	return snapshot
}

// Clamp sources: "env" = SYNC_UNIT_CONCURRENCY_PER_BUCKET is set to a positive integer and that is the clamp; "default"
// = it is unset, not an integer or below 1, and the built-in default applies (CHAOS-8201).
const (
	clampSourceEnv     = "env"
	clampSourceDefault = "default"
)

// EffectiveAdmissionCapSources reports where the clamp and each class's cap come from, for the start line.
func EffectiveAdmissionCapSources() (clampSource string, capSources map[string]string) {
	snapshot := readAdmissionCaps()
	return snapshot.clampSource, snapshot.capSources
}

// logAdmissionCaps writes ONE Info line, when the service that runs the guard
// is constructed, with the cap the guard applies per cost class, the clamp it
// read from THIS process's environment and the worker budget limit of the same
// class from the one table, so a cap/budget mismatch or a lowered clamp is
// visible at Info where the cap is enforced (CHAOS-7881). No org id or tenant
// data.
func logAdmissionCaps(logger *synclog.Logger) {
	snapshot := readAdmissionCaps()
	clamp, caps, clampSource, capSources := snapshot.clamp, snapshot.caps, snapshot.clampSource, snapshot.capSources
	budget := func(class string) int {
		limit, _ := providerfoundation.CostClassBudgetLimit(class)
		return limit
	}
	logger.Info(context.Background(), synclog.MsgSyncDispatchAdmissionCaps,
		synclog.Count(synclog.KeyAdmissionCapLight, caps["light"]),
		synclog.Count(synclog.KeyAdmissionCapMedium, caps["medium"]),
		synclog.Count(synclog.KeyAdmissionCapHeavy, caps["heavy"]),
		synclog.Count(synclog.KeyBudgetLimitLight, budget("light")),
		synclog.Count(synclog.KeyBudgetLimitMedium, budget("medium")),
		synclog.Count(synclog.KeyBudgetLimitHeavy, budget("heavy")),
		synclog.Count(synclog.KeyAdmissionClamp, clamp),
		synclog.Text(synclog.KeyAdmissionCapLightSource, synclog.ParseLabel(capSources["light"])),
		synclog.Text(synclog.KeyAdmissionCapMediumSource, synclog.ParseLabel(capSources["medium"])),
		synclog.Text(synclog.KeyAdmissionCapHeavySource, synclog.ParseLabel(capSources["heavy"])),
		synclog.Text(synclog.KeyAdmissionClampSource, synclog.ParseLabel(clampSource)),
	)
}

// concurrencyCapForCostClass is the admission cap of one (org, provider,
// cost_class) bucket (CHAOS-7434). It is the worker budget's limit for that
// class (providerfoundation.CostClassBudgetLimit, the same table the worker's
// request budget reads), and SYNC_UNIT_CONCURRENCY_PER_BUCKET is only an UPPER
// CLAMP on it: the variable can lower a class's cap but never raise it past
// what the budget serves. Admitting more than the budget serves only makes the
// extra units loop in budget-contention snoozes (a denied files attempt loses
// all its requests), so a class outside the table (not one a provider unit
// uses) keeps the clamp as before.
func concurrencyCapForCostClass(costClass string) int {
	capValue, _ := concurrencyCapAndSourceForCostClass(costClass)
	return capValue
}

// Admission cap sources (CHAOS-8201): where a class's cap comes from. "table" = the class's budget limit (the clamp does
// not lower it, including the case where the two are equal); "clamp" = SYNC_UNIT_CONCURRENCY_PER_BUCKET is what sets it
// (it is below the class's limit, or the class is outside the table).
const (
	capSourceTable = "table"
	capSourceClamp = "clamp"
)

// concurrencyCapAndSourceForCostClass is the ONE place the cap is derived, so the number the guard enforces and the
// source the start line reports cannot drift apart.
func concurrencyCapAndSourceForCostClass(costClass string) (int, string) {
	return capAndSourceForClamp(costClass, syncUnitConcurrencyPerBucket())
}

// capAndSourceForClamp is the derivation for a clamp that was already read: the start line reads the variable once and derives
// every class from that one value.
func capAndSourceForClamp(costClass string, clamp int) (int, string) {
	if limit, ok := providerfoundation.CostClassBudgetLimit(costClass); ok && limit <= clamp {
		return limit, capSourceTable
	}
	return clamp, capSourceClamp
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
//
// The units are read in keyset pages of unitPageSize (never all at
// once); decideDispatchGuard folds them.
func authorizeRun(ctx context.Context, tx pgx.Tx, logger *synclog.Logger, orgID, runID string, now time.Time) (guardDecision, error) {
	staleCutoff := now.Add(-staleDispatchSeconds())
	env := guardEnv{
		lockBuckets: func(buckets []dispatchBucket) error { return acquireBucketAdvisoryLocks(ctx, tx, buckets) },
		totalCap: func() int {
			// Python: any resolution failure (missing org, missing tier_limits
			// table, a malformed override) falls back to the env default
			// unconditionally -- ResolveMaxSyncUnitsCap's own doc
			// warns callers wanting that parity must substitute the default
			// themselves; this is that substitution.
			totalCap, err := scheduledsync.ResolveMaxSyncUnitsCap(ctx, tx, orgID, defaultSyncRunMaxUnits())
			if err != nil {
				return defaultSyncRunMaxUnits()
			}
			return totalCap
		},
		activeCount: func(bucket dispatchBucket) (int, error) {
			return countActiveBucketUnits(ctx, tx, bucket, staleCutoff, now)
		},
		emit: func(bucket dispatchBucket, activeCount, reclaimedStale, cappedNew, cappedStale, headroom int) {
			emitBucketDecision(ctx, logger, bucket, activeCount, reclaimedStale, cappedNew, cappedStale, headroom)
		},
	}
	source := func(fold func(dispatchGuardUnit) error) error {
		return forEachDispatchGuardUnit(ctx, tx, runID, fold)
	}
	return decideDispatchGuard(source, env, orgID, now)
}

// guardEnv is everything decideDispatchGuard needs from the database, so the decision runs on a fake in tests.
type guardEnv struct {
	lockBuckets func([]dispatchBucket) error
	totalCap    func() int
	activeCount func(dispatchBucket) (int, error)
	emit        func(bucket dispatchBucket, activeCount, reclaimedStale, cappedNew, cappedStale, headroom int)
}

// guardUnitSource folds every unit of the run in id order; each call is one full paged read.
type guardUnitSource func(fold func(dispatchGuardUnit) error) error

// guardBucketFold is the per-bucket state of the second pass. The first allowedSlots units of `stale reclaims by
// (updated_at, id), then new work by id` are the claimable ones; every other candidate is capped. The capped ids and the
// authorized ids are the decision's output and still grow with the number of units; only the page size of the reads is
// bounded here.
type guardBucketFold struct {
	slots          int
	reclaims       reclaimHeap
	reclaimCount   int
	newCount       int
	newKept        []string
	newOverflow    []string
	cappedReclaims []dispatchGuardUnit
}

// reclaimHeap is a max-heap on reclaim priority: the worst-priority kept reclaim sits on top.
type reclaimHeap []dispatchGuardUnit

func reclaimBefore(a, b dispatchGuardUnit) bool {
	if !a.updatedAt.Equal(b.updatedAt) {
		return a.updatedAt.Before(b.updatedAt)
	}
	return a.id < b.id
}

func (heap reclaimHeap) Len() int           { return len(heap) }
func (heap reclaimHeap) Less(i, j int) bool { return reclaimBefore(heap[j], heap[i]) }
func (heap reclaimHeap) Swap(i, j int)      { heap[i], heap[j] = heap[j], heap[i] }
func (heap *reclaimHeap) Push(value any)    { *heap = append(*heap, value.(dispatchGuardUnit)) }
func (heap *reclaimHeap) Pop() any {
	old := *heap
	last := old[len(old)-1]
	*heap = old[:len(old)-1]
	return last
}

func (fold *guardBucketFold) addReclaim(unit dispatchGuardUnit) {
	fold.reclaimCount++
	if fold.slots == 0 {
		fold.cappedReclaims = append(fold.cappedReclaims, unit)
		return
	}
	heap.Push(&fold.reclaims, unit)
	if fold.reclaims.Len() > fold.slots {
		fold.cappedReclaims = append(fold.cappedReclaims, heap.Pop(&fold.reclaims).(dispatchGuardUnit))
	}
}

func (fold *guardBucketFold) addNew(id string) {
	fold.newCount++
	if len(fold.newKept) < fold.slots {
		fold.newKept = append(fold.newKept, id)
		return
	}
	fold.newOverflow = append(fold.newOverflow, id)
}

// cappedIDs is the old prioritized[allowedSlots:] in the old order: the capped reclaims (sorted), then capped new work by id.
func (fold *guardBucketFold) cappedIDs() []string {
	sort.Slice(fold.cappedReclaims, func(i, j int) bool { return reclaimBefore(fold.cappedReclaims[i], fold.cappedReclaims[j]) })
	ids := make([]string, 0, len(fold.cappedReclaims)+len(fold.newKept)+len(fold.newOverflow))
	for _, unit := range fold.cappedReclaims {
		ids = append(ids, unit.id)
	}
	claimableNew := fold.slots - fold.reclaimCount
	if claimableNew < 0 {
		claimableNew = 0
	}
	if claimableNew > len(fold.newKept) {
		claimableNew = len(fold.newKept)
	}
	ids = append(ids, fold.newKept[claimableNew:]...)
	return append(ids, fold.newOverflow...)
}

// decideDispatchGuard is the guard decision, folded over keyset pages (each read returns at most unitPageSize rows;
// the decision's capped and authorized id sets are not bounded by it) in two passes: pass 1 learns the bucket sets and
// the unit count; the buckets are locked; pass 2 (or, over the total cap, a suffix walk) computes the cap decision.
// The two passes read the table twice, so a row that changed between them is judged on what pass 2 saw, but only
// inside the buckets pass 1 locked: pass 2 never authorizes a unit of a bucket outside allBuckets.
func decideDispatchGuard(source guardUnitSource, env guardEnv, orgID string, now time.Time) (guardDecision, error) {
	staleCutoff := now.Add(-staleDispatchSeconds())
	bucketOf := func(unit dispatchGuardUnit) dispatchBucket {
		return dispatchBucket{orgID: orgID, provider: unit.provider, costClass: unit.costClass}
	}
	// classify mirrors the old per-unit switch. lock: a later write of this transaction could touch the row
	// (PLANNED, stale DISPATCHING, any RETRYING). candidate: a concurrency-cap candidate.
	// deferred: a not-yet-due RETRYING row, which makes its bucket a cap-loop bucket without being a candidate.
	classify := func(unit dispatchGuardUnit) (lock, candidate, deferred bool) {
		switch unit.status {
		case syncRunUnitStatusPlanned:
			return true, true, false
		case syncRunUnitStatusDispatching:
			stale := !unit.updatedAt.After(staleCutoff)
			return stale, stale, false
		case syncRunUnitStatusRetrying:
			if unit.availableAt == nil {
				return true, false, false
			}
			if !unit.availableAt.After(now) {
				return true, true, false
			}
			return true, false, true
		}
		return false, false, false
	}

	total := 0
	lockBuckets := map[dispatchBucket]bool{}
	loopBuckets := map[dispatchBucket]bool{}
	if err := source(func(unit dispatchGuardUnit) error {
		total++
		lock, candidate, deferred := classify(unit)
		bucket := bucketOf(unit)
		if lock {
			lockBuckets[bucket] = true
		}
		if candidate || deferred {
			loopBuckets[bucket] = true
		}
		return nil
	}); err != nil {
		return guardDecision{}, err
	}

	// The bucket locks are taken before EVERY early return, the hard-cap one included, so denyRun's bulk
	// writes run under the same locks as claimUnits.
	if err := env.lockBuckets(sortBucketList(lockBuckets)); err != nil {
		return guardDecision{}, err
	}
	allBuckets := sortBucketList(loopBuckets)

	totalCap := env.totalCap()
	if total > totalCap {
		capped := make([]string, 0, total-totalCap)
		index := 0
		if err := source(func(unit dispatchGuardUnit) error {
			if index >= totalCap && len(capped) < total-totalCap {
				capped = append(capped, unit.id)
			}
			index++
			return nil
		}); err != nil {
			return guardDecision{}, err
		}
		return guardDecision{
			allowed:       false,
			reason:        fmt.Sprintf("sync run unit cap exceeded: %d/%d", total, totalCap),
			cappedUnitIDs: capped,
		}, nil
	}

	activeCounts := make(map[dispatchBucket]int, len(allBuckets))
	folds := make(map[dispatchBucket]*guardBucketFold, len(allBuckets))
	for _, bucket := range allBuckets {
		activeCount, err := env.activeCount(bucket)
		if err != nil {
			return guardDecision{}, err
		}
		activeCounts[bucket] = activeCount
		slots := concurrencyCapForCostClass(bucket.costClass) - activeCount
		if slots < 0 {
			slots = 0
		}
		folds[bucket] = &guardBucketFold{slots: slots}
	}

	// The authorized set is every candidate of a locked, cap-checked bucket and nothing else.
	authorizedUnitIDs := make(map[string]bool)
	if err := source(func(unit dispatchGuardUnit) error {
		_, candidate, _ := classify(unit)
		if !candidate {
			return nil
		}
		fold := folds[bucketOf(unit)]
		if fold == nil {
			return nil
		}
		authorizedUnitIDs[unit.id] = true
		if unit.status == syncRunUnitStatusDispatching {
			fold.addReclaim(unit)
		} else {
			fold.addNew(unit.id)
		}
		return nil
	}); err != nil {
		return guardDecision{}, err
	}

	var cappedUnitIDs []string
	slotHeadroom := make(map[dispatchBucket]int, len(allBuckets))
	for _, bucket := range allBuckets {
		fold := folds[bucket]
		capped := fold.cappedIDs()
		cappedUnitIDs = append(cappedUnitIDs, capped...)
		headroom := fold.slots - (fold.reclaimCount + fold.newCount)
		if headroom < 0 {
			headroom = 0
		}
		slotHeadroom[bucket] = headroom
		reclaimedStale := fold.reclaimCount
		if reclaimedStale > fold.slots {
			reclaimedStale = fold.slots
		}
		env.emit(bucket, activeCounts[bucket], reclaimedStale, len(capped)-len(fold.cappedReclaims), len(fold.cappedReclaims), headroom)
	}

	if len(cappedUnitIDs) > 0 {
		return guardDecision{
			allowed:           true,
			reason:            fmt.Sprintf("sync unit concurrency cap exceeded: %d capped", len(cappedUnitIDs)),
			cappedUnitIDs:     cappedUnitIDs,
			concurrencyCapped: true,
			slotHeadroom:      slotHeadroom,
			authorizedUnitIDs: authorizedUnitIDs,
		}, nil
	}
	return guardDecision{allowed: true, slotHeadroom: slotHeadroom, authorizedUnitIDs: authorizedUnitIDs}, nil
}

// forEachDispatchGuardUnit folds this run's units in id order (stable ordering for the total-cap suffix,
// guard.py:147-153), unitPageSize rows per query.
//
// Error classification: a query/scan failure here is a bare execution
// failure indistinguishable from any other Postgres blip mid-dispatch --
// classified retryable via ErrDiscoveryTransientFailure's SAME reasoning
// (CHAOS-4175 round 3), reused here rather than minted again narrowly for
// dispatch, since the retry/backoff state machine this family's Dispatch()
// routes into is the identical one round 3 fixed for reference_discovery.
func forEachDispatchGuardUnit(ctx context.Context, tx pgx.Tx, runID string, fold func(dispatchGuardUnit) error) error {
	load := func(afterID string, limit int) ([]dispatchGuardUnit, error) {
		const columns = `SELECT id::text, status, provider, cost_class, updated_at, available_at FROM public.sync_run_units`
		var rows pgx.Rows
		var err error
		if afterID == "" {
			rows, err = tx.Query(ctx, columns+` WHERE sync_run_id = $1::uuid ORDER BY id LIMIT $2`, runID, limit)
		} else {
			rows, err = tx.Query(ctx, columns+` WHERE sync_run_id = $1::uuid AND id > $3::uuid ORDER BY id LIMIT $2`, runID, limit, afterID)
		}
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
	return forEachUnitPage(unitPageSize, load, func(unit dispatchGuardUnit) string { return unit.id }, func(page []dispatchGuardUnit) error {
		for _, unit := range page {
			if err := fold(unit); err != nil {
				return err
			}
		}
		return nil
	})
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

// sortBucketList ports guard.py's `sorted(set(...))` -- deterministic ordering is what makes the advisory
// lock acquisition deadlock-free when two dispatchers race on the same bucket set.
func sortBucketList(set map[dispatchBucket]bool) []dispatchBucket {
	buckets := make([]dispatchBucket, 0, len(set))
	for bucket := range set {
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
	keys := make([]int64, len(buckets))
	for index, bucket := range buckets {
		keys[index] = bucketAdvisoryLockKey(bucket)
	}
	return acquireAdvisoryLocksBounded(ctx, tx, keys, "bucket", dispatchLockWaitBudget)
}

// emitBucketDecision ports _emit_bucket_decision: a structured log line
// plus span attributes/event on the CURRENT span (set by
// spanForCoordinatorJob, already open for the whole dispatch job) --
// telemetry only, never gates dispatch, matching Python's own
// `except Exception: return` around its span calls (a span-attribute
// failure here is likewise swallowed, not propagated: attribute.String/Int
// on a Go span cannot fail the way Python's dynamic OTel calls could, so
// there is nothing to catch, but the log call is likewise best-effort).
func emitBucketDecision(ctx context.Context, logger *synclog.Logger, bucket dispatchBucket, activeCount, reclaimedStale, cappedNew, cappedStale, slotHeadroom int) {
	bucketLabel := bucket.orgID + "/" + bucket.provider + "/" + bucket.costClass
	logger.Info(ctx, synclog.MsgDispatchGuardBucketDecision, synclog.Text(synclog.KeyBucket, synclog.ParseLabel(bucketLabel)), synclog.Count(synclog.KeyGuardActiveCount, activeCount), synclog.Count(synclog.KeyGuardReclaimedStale, reclaimedStale), synclog.Count(synclog.KeyGuardCappedNew, cappedNew), synclog.Count(synclog.KeyGuardCappedStale, cappedStale), synclog.Count(synclog.KeyGuardSlotHeadroom, slotHeadroom))
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
