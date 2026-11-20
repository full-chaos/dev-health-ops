// Package syncbucket is the admission state of one sync dispatch bucket (org, provider, cost class) that every slot
// writer shares (CHAOS-14250): the bucket advisory lock, the cap, the active count, the sweep reservations and the
// pick of the bucket's oldest eligible waiters. The dispatch pass, the bucket sweep, providersync's unit commits and
// the reconciler's repairs all reserve a freed slot through ReserveHeads, in their own transaction, under the lock.
//
// Lock rule (acp lock-1): the bucket lock is the first lock a writer takes, before any row; while it is held a writer
// runs only the database statements of its own write and of the pick, never an outbound call.
package syncbucket

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/jackc/pgx/v5"
)

// Bucket is one (org, provider, cost class) admission bucket.
type Bucket struct {
	OrgID     string
	Provider  string
	CostClass string
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return max(value, 0)
}

// EnvPositiveInt reads a positive integer variable: unset, not an integer or below 1 gives fallback (fromEnv false).
func EnvPositiveInt(name string, fallback int) (value int, fromEnv bool) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, false
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 1 {
		return fallback, false
	}
	return parsed, true
}

// ConcurrencyClamp is SYNC_UNIT_CONCURRENCY_PER_BUCKET (default 8), an upper clamp on every class's cap.
func ConcurrencyClamp() (int, bool) { return EnvPositiveInt("SYNC_UNIT_CONCURRENCY_PER_BUCKET", 8) }

// Cap sources: "table" = the class's budget limit; "clamp" = SYNC_UNIT_CONCURRENCY_PER_BUCKET sets it.
const (
	CapSourceTable = "table"
	CapSourceClamp = "clamp"
)

// CapForClamp is a class's cap for a clamp already read: the worker budget's limit for the class
// (providerfoundation.CostClassBudgetLimit), lowered by the clamp; a class outside the table keeps the clamp.
func CapForClamp(costClass string, clamp int) (int, string) {
	if limit, ok := providerfoundation.CostClassBudgetLimit(costClass); ok && limit <= clamp {
		return limit, CapSourceTable
	}
	return clamp, CapSourceClamp
}

// Cap is the admission cap of a bucket's class.
func Cap(costClass string) int {
	clamp, _ := ConcurrencyClamp()
	capValue, _ := CapForClamp(costClass, clamp)
	return capValue
}

// StaleWindow is SYNC_UNIT_DISPATCH_STALE_SECONDS (default 900 s): a dispatching unit older than it holds no slot and
// is dispatchable again.
func StaleWindow() time.Duration {
	value, _ := EnvPositiveInt("SYNC_UNIT_DISPATCH_STALE_SECONDS", 900)
	return time.Duration(value) * time.Second
}

// RedispatchCountdown is SYNC_DISPATCH_REDISPATCH_COUNTDOWN (default 60 s).
func RedispatchCountdown() time.Duration {
	return time.Duration(envInt("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", 60)) * time.Second
}

const redispatchCountdownMaxDefaultSeconds = 300

// RedispatchCountdownMax is SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX (default 300 s), never below base.
func RedispatchCountdownMax(base time.Duration) time.Duration {
	limit := time.Duration(envInt("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", redispatchCountdownMaxDefaultSeconds)) * time.Second
	return max(limit, base)
}

// ReservationTTL is how long a reservation holds its slot: twice the redispatch countdown maximum. A reserved run
// whose pass never runs stops holding the slot after it, and the sweep picks again.
func ReservationTTL() time.Duration {
	return 2 * RedispatchCountdownMax(RedispatchCountdown())
}

// LockKey is the bucket's pg_advisory_xact_lock key: the first 8 bytes of sha256("org:provider:cost_class"), 63 bits.
func LockKey(bucket Bucket) int64 {
	digest := sha256.Sum256([]byte(bucket.OrgID + ":" + bucket.Provider + ":" + bucket.CostClass))
	return int64(binary.BigEndian.Uint64(digest[:8]) & ((1 << 63) - 1))
}

// Sorted returns the distinct buckets in lock order (org, provider, cost class).
func Sorted(buckets []Bucket) []Bucket {
	seen := make(map[Bucket]bool, len(buckets))
	out := make([]Bucket, 0, len(buckets))
	for _, bucket := range buckets {
		if !seen[bucket] {
			seen[bucket] = true
			out = append(out, bucket)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.OrgID != b.OrgID {
			return a.OrgID < b.OrgID
		}
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		return a.CostClass < b.CostClass
	})
	return out
}

// LockCommitPath takes the bucket locks of a unit writer's commit (providersync Complete/Fail) as the transaction's
// first statements, in lock order, and observes the wait in LockWaitMetricsSource (observe only, never a failure).
func LockCommitPath(ctx context.Context, tx pgx.Tx, buckets []Bucket, path string) error {
	started := time.Now()
	for _, bucket := range Sorted(buckets) {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", LockKey(bucket)); err != nil {
			return fmt.Errorf("bucket lock: %w", err)
		}
	}
	lockWaits.observe(ctx, path, time.Since(started))
	return nil
}

// countActiveSQL counts the bucket's capacity consumers: fresh dispatching units and running units whose lease is
// live or unknown; the second count is the part of them whose run is a backfill.
const countActiveSQL = `
SELECT count(*),
       count(*) FILTER (WHERE run.triggered_by = 'backfill' OR run.mode = 'backfill')
FROM public.sync_run_units AS unit
LEFT JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE unit.org_id = $1 AND unit.provider = $2 AND unit.cost_class = $3
  AND (
        (unit.status = 'dispatching' AND unit.updated_at > $4)
     OR (unit.status = 'running' AND (unit.lease_expires_at IS NULL OR unit.lease_expires_at > $5))
      )`

// CountActive runs countActiveSQL.
func CountActive(ctx context.Context, tx pgx.Tx, bucket Bucket, staleCutoff, now time.Time) (int, int, error) {
	var count, backfill int
	if err := tx.QueryRow(ctx, countActiveSQL, bucket.OrgID, bucket.Provider, bucket.CostClass, staleCutoff, now).Scan(&count, &backfill); err != nil {
		return 0, 0, fmt.Errorf("count active bucket units: %w", err)
	}
	return count, backfill, nil
}

// DispatchableUnitSQL is THE dispatchable rule of a unit (C1): planned, or retrying and due, or dispatching and stale.
// It reads the unit as `unit`, the instant as $1 and the stale cutoff as $2. The materializer's stale recovery uses it
// as written; the bucket's head read (eligibleWaiterSQL) is the same rule split into index probes, and the dispatch
// guard's classify is Dispatchable; a cell checks the three agree.
const DispatchableUnitSQL = `(unit.status = 'planned'
				OR (unit.status = 'dispatching' AND unit.updated_at <= $2)
				OR (unit.status = 'retrying' AND unit.available_at IS NOT NULL AND unit.available_at <= $1))`

// Dispatchable is DispatchableUnitSQL in Go, for the guard's classify of rows it already read.
func Dispatchable(status string, updatedAt time.Time, availableAt *time.Time, now, staleCutoff time.Time) bool {
	switch status {
	case "planned":
		return true
	case "dispatching":
		return !updatedAt.After(staleCutoff)
	case "retrying":
		return availableAt != nil && !availableAt.After(now)
	}
	return false
}

// eligibleWaiterSQL is the one eligibility rule of a bucket's waiter: an open run whose reference discovery
// succeeded, with a dispatchable unit (DispatchableUnitSQL) in the bucket. It reads the run as `run`, the bucket as
// $2/$3, the instant as $5 and the stale cutoff as $7. Each probe is a scalar subquery tested IS NOT NULL (acp ack c:
// the planner estimates it at 99.5 %, so the ordered walk with the LIMIT is the plan): planned and retrying units
// on ix_sync_run_units_run_bucket_waiting, dispatching units on ix_sync_run_units_run_bucket_dispatching, the discovery
// on uq_sync_run_reference_discoveries_run.
const eligibleWaiterSQL = `run.status NOT IN ('success', 'partial_failed', 'failed')
    AND ((
        SELECT 1 FROM public.sync_run_units AS unit
        WHERE unit.sync_run_id = run.id AND unit.provider = $2 AND unit.cost_class = $3
            AND unit.status IN ('planned', 'retrying')
            AND (unit.status = 'planned' OR unit.available_at <= $5)
        LIMIT 1
    ) IS NOT NULL OR (
        SELECT 1 FROM public.sync_run_units AS unit
        WHERE unit.sync_run_id = run.id AND unit.provider = $2 AND unit.cost_class = $3
            AND unit.status = 'dispatching' AND unit.updated_at <= $7
        LIMIT 1
    ) IS NOT NULL)
    AND (
        SELECT 1 FROM public.sync_run_reference_discoveries AS discovery
        WHERE discovery.sync_run_id = run.id AND discovery.status = 'success'
        LIMIT 1
    ) IS NOT NULL`

// HeadsSQL lists the eligible waiters of one class of a bucket, oldest first, walking the org's open runs of the class
// in (created_at, id) order on ix_sync_runs_org_class_active_order. The class is compared as an integer ($4: 1
// backfill, 0 not), the index's own expression (acp ack b). Its cost grows with the ineligible runs of the org and
// class ahead of the head (ticket 14338 (filed separately)).
const HeadsSQL = `
SELECT run.id::text
FROM public.sync_runs AS run
WHERE run.org_id = $1
    AND (run.triggered_by = 'backfill' OR run.mode = 'backfill')::integer = $4::integer
    AND ` + eligibleWaiterSQL + `
ORDER BY run.created_at, run.id
LIMIT $6`

// ReadHeads reads up to limit eligible waiters of one class, oldest first, leaving out the runs in skip (at most the
// bucket's cap plus one): it reads limit+len(skip) heads and drops them here.
func ReadHeads(ctx context.Context, tx pgx.Tx, bucket Bucket, backfill bool, now, staleCutoff time.Time, limit int, skip map[string]bool) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	class := 0
	if backfill {
		class = 1
	}
	rows, err := tx.Query(ctx, HeadsSQL, bucket.OrgID, bucket.Provider, bucket.CostClass, class, now, limit+len(skip), staleCutoff)
	if err != nil {
		return nil, fmt.Errorf("read bucket heads: %w", err)
	}
	defer rows.Close()
	var heads []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan bucket head: %w", err)
		}
		if !skip[id] && len(heads) < limit {
			heads = append(heads, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bucket heads: %w", err)
	}
	return heads, nil
}

// ReservationsSQL counts the bucket's live reservations, all and backfill, leaving out the run $4 (empty: none).
const ReservationsSQL = `
SELECT count(*), count(*) FILTER (WHERE reservation.backfill)
FROM public.sync_dispatch_bucket_reservations AS reservation
WHERE reservation.org_id = $1 AND reservation.provider = $2 AND reservation.cost_class = $3
    AND reservation.sync_run_id::text <> $4
    AND reservation.reserved_at > $5::timestamptz - make_interval(secs => $6::double precision)`

// CountReservations runs ReservationsSQL.
func CountReservations(ctx context.Context, tx pgx.Tx, bucket Bucket, exceptRunID string, now time.Time) (int, int, error) {
	var reserved, reservedBackfill int
	if err := tx.QueryRow(ctx, ReservationsSQL, bucket.OrgID, bucket.Provider, bucket.CostClass, exceptRunID, now,
		ReservationTTL().Seconds()).Scan(&reserved, &reservedBackfill); err != nil {
		return 0, 0, fmt.Errorf("count bucket reservations: %w", err)
	}
	return reserved, reservedBackfill, nil
}

// RetireRunReservationsSQL drops every reservation of a run: its pass is the one the slots were kept for.
const RetireRunReservationsSQL = `DELETE FROM public.sync_dispatch_bucket_reservations WHERE sync_run_id = $1::uuid`

// RetireRunReservations runs RetireRunReservationsSQL.
func RetireRunReservations(ctx context.Context, tx pgx.Tx, runID string) error {
	if _, err := tx.Exec(ctx, RetireRunReservationsSQL, runID); err != nil {
		return fmt.Errorf("retire run reservations: %w", err)
	}
	return nil
}

// ExpireReservationsSQL drops the bucket's void reservations: written $5 seconds or more before $4.
const ExpireReservationsSQL = `
DELETE FROM public.sync_dispatch_bucket_reservations
WHERE org_id = $1 AND provider = $2 AND cost_class = $3
    AND reserved_at <= $4::timestamptz - make_interval(secs => $5::double precision)`

const reservedRunsSQL = `
SELECT sync_run_id::text FROM public.sync_dispatch_bucket_reservations
WHERE org_id = $1 AND provider = $2 AND cost_class = $3`

const insertReservationSQL = `
INSERT INTO public.sync_dispatch_bucket_reservations (org_id, provider, cost_class, sync_run_id, backfill, reserved_at)
VALUES ($1, $2, $3, $4::uuid, $5, $6)`

// earliestReservationSQL is when the bucket's oldest live reservation was written (NULL: none).
const earliestReservationSQL = `
SELECT min(reserved_at) FROM public.sync_dispatch_bucket_reservations
WHERE org_id = $1 AND provider = $2 AND cost_class = $3`

// EarliestReservationExpiry is when the bucket's oldest reservation expires (ok false: the bucket has none).
func EarliestReservationExpiry(ctx context.Context, tx pgx.Tx, bucket Bucket) (time.Time, bool, error) {
	var at *time.Time
	if err := tx.QueryRow(ctx, earliestReservationSQL, bucket.OrgID, bucket.Provider, bucket.CostClass).Scan(&at); err != nil {
		return time.Time{}, false, fmt.Errorf("read earliest reservation: %w", err)
	}
	if at == nil {
		return time.Time{}, false, nil
	}
	return at.Add(ReservationTTL()), true, nil
}

func readReservedRuns(ctx context.Context, tx pgx.Tx, bucket Bucket) (map[string]bool, error) {
	rows, err := tx.Query(ctx, reservedRunsSQL, bucket.OrgID, bucket.Provider, bucket.CostClass)
	if err != nil {
		return nil, fmt.Errorf("read reserved runs: %w", err)
	}
	defer rows.Close()
	reserved := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan reserved run: %w", err)
		}
		reserved[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reserved runs: %w", err)
	}
	return reserved, nil
}

// Heads is what ReserveHeads did: the runs it reserved a slot for (oldest first, backfill then non-backfill), and
// whether self is among the bucket's heads for a free slot (self is never reserved: its pass is the one running).
type Heads struct {
	Reserved     []string
	SelfIsAHead  bool
	StillWaiting bool
}

// ReserveHeads gives the bucket's free slots to its oldest eligible waiters, by eligibility at this instant (never a
// queue position): free = cap - active - live reservations; up to the backfill share (CHAOS-14211, counting active
// backfill units and backfill reservations) to the oldest backfill waiters, the rest to the oldest non-backfill
// waiters. It writes one reservation per pick except self (empty: none). The caller holds the bucket lock (the first
// lock of its transaction) and wakes the reserved runs after its commit, each with its own statement. It first drops
// the bucket's void reservations (older than ReservationTTL), so a head that never claimed is picked again by
// eligibility.
func ReserveHeads(ctx context.Context, tx pgx.Tx, bucket Bucket, now time.Time, self string) (Heads, error) {
	var result Heads
	staleCutoff := now.Add(-StaleWindow())
	if _, err := tx.Exec(ctx, ExpireReservationsSQL, bucket.OrgID, bucket.Provider, bucket.CostClass, now, ReservationTTL().Seconds()); err != nil {
		return result, fmt.Errorf("expire bucket reservations: %w", err)
	}
	active, activeBackfill, err := CountActive(ctx, tx, bucket, staleCutoff, now)
	if err != nil {
		return result, err
	}
	reserved, reservedBackfill, err := CountReservations(ctx, tx, bucket, self, now)
	if err != nil {
		return result, err
	}
	reservedRuns, err := readReservedRuns(ctx, tx, bucket)
	if err != nil {
		return result, err
	}
	delete(reservedRuns, self)
	capValue := Cap(bucket.CostClass)
	free := max(0, capValue-active-reserved)
	backfillUsable := free
	if capValue >= 2 {
		backfillUsable = min(free, max(0, capValue-1-activeBackfill-reservedBackfill))
	}
	backfillHeads, err := ReadHeads(ctx, tx, bucket, true, now, staleCutoff, backfillUsable, reservedRuns)
	if err != nil {
		return result, err
	}
	otherHeads, err := ReadHeads(ctx, tx, bucket, false, now, staleCutoff, free-len(backfillHeads), reservedRuns)
	if err != nil {
		return result, err
	}
	for _, pick := range []struct {
		heads    []string
		backfill bool
	}{{backfillHeads, true}, {otherHeads, false}} {
		for _, runID := range pick.heads {
			if runID == self {
				result.SelfIsAHead = true
				continue
			}
			if _, err := tx.Exec(ctx, insertReservationSQL, bucket.OrgID, bucket.Provider, bucket.CostClass, runID, pick.backfill, now); err != nil {
				return result, fmt.Errorf("reserve bucket slot: %w", err)
			}
			result.Reserved = append(result.Reserved, runID)
		}
	}
	result.StillWaiting = len(backfillHeads)+len(otherHeads) > 0 || len(reservedRuns) > 0
	for _, backfill := range []bool{true, false} {
		if result.StillWaiting {
			break
		}
		heads, err := ReadHeads(ctx, tx, bucket, backfill, now, staleCutoff, 1, nil)
		if err != nil {
			return result, err
		}
		result.StillWaiting = len(heads) > 0
	}
	return result, nil
}
