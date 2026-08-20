package syncreconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The unreclaimable-dispatching sweep (CHAOS-4005, porting CHAOS-3990).
//
// Lease repair can only reach a RUNNING unit whose lease expired. A unit that
// was claimed to 'dispatching' and never published holds no lease at all, so
// there is nothing to expire and no mechanism in the system can reach it: it
// keeps a per-bucket concurrency slot forever and its run can never finalize.
// That is the strand CHAOS-3990 diagnosed in production.
//
// CHAOS-3990 shipped this sweep in Python, but the 60s reconcile cadence is
// owned by THIS package (internal/scheduler/fixed/inventory.go), with no
// bridge, so the Python implementation never executed in production. This is
// the port. The Python version is the specification; every guard below exists
// because an adversarial review found the naive form broken, and each is
// annotated with what it prevents.
// DefaultUnreclaimableAge and DefaultUnreclaimableIdle mirror Python's
// SYNC_UNIT_UNRECLAIMABLE_SECONDS (3600) and SYNC_UNIT_DISPATCH_STALE_SECONDS
// (900) defaults.
const (
	DefaultUnreclaimableAge   = time.Hour
	DefaultUnreclaimableIdle  = 15 * time.Minute
	unreclaimableDefaultAge   = time.Hour
	unreclaimableMinimumLimit = 1
	unreclaimableMaximumLimit = 100
	// The paging loop filters AFTER the SQL window, so a degraded state -- say
	// a large backlog of old dispatching rows that all belong to River -- could
	// otherwise walk the whole table on every 60s pass looking for a strand
	// that is not there. The reconciler's contract is bounded work per pass, so
	// the scan is capped: an unfound strand is simply found on a later pass.
	unreclaimableMaximumPages   = 10
	unreclaimableErrorCategory  = "feature_disabled"
	unreclaimableProviderUnitID = "sync.provider_unit"
)

// SweepMode decides whether the sweep may write.
//
// It defaults to shadow: this is the safety net that was silently absent from
// production, so its selection is proven against real rows before it is
// allowed to terminalize anything.
type SweepMode string

const (
	SweepModeShadow SweepMode = "shadow"
	SweepModeActive SweepMode = "active"
)

// CeleryPresence mirrors Python's CeleryConsumerPresence.
//
// Python answers this with a live pidbox broadcast. Go has no such probe, and
// building one would reimplement the mechanism CHAOS-3992 documents as
// unreliable. Instead the deployment declares it: EXPECTED_WORKER_GROUPS is
// set only by Go-only deployments (see src/dev_health_ops/api/_health.py),
// so an unset value means Celery may be alive and the sweep must defer.
type CeleryPresence string

const (
	CeleryPresent CeleryPresence = "present"
	CeleryAbsent  CeleryPresence = "absent"
	CeleryUnknown CeleryPresence = "unknown"
)

// UnreclaimableSweepConfig is the policy half of the sweep.
type UnreclaimableSweepConfig struct {
	// Age bounds how long a unit may exist without a single attempt before it
	// is presumed dead. Deliberately far longer than the stale-dispatch
	// window: a Celery-published unit legitimately waits in 'dispatching'
	// until a consumer claims it.
	Age time.Duration
	// Idle is the ordinary stale-dispatch window. A unit the dispatcher only
	// just published is never swept.
	Idle time.Duration
	// Mode gates writing. Shadow selects and reports without mutating.
	Mode SweepMode
	// Switches and Route describe what any runtime could execute right now.
	Switches providersync.CompleteRouteSwitches
	// Presence is the deployment's Celery declaration, resolved once at wiring.
	Presence CeleryPresence
}

func (config UnreclaimableSweepConfig) valid() bool {
	return config.Age > 0 && config.Idle > 0 &&
		(config.Mode == SweepModeShadow || config.Mode == SweepModeActive) &&
		(config.Presence == CeleryPresent || config.Presence == CeleryAbsent ||
			config.Presence == CeleryUnknown)
}

// PresenceFromExpectedWorkerGroups maps the deployment declaration onto the
// resolver's vocabulary. Empty or whitespace means "not declared", which is
// UNKNOWN and therefore defers -- it never means "no Celery".
func PresenceFromExpectedWorkerGroups(raw string) CeleryPresence {
	for _, group := range strings.Split(raw, ",") {
		if strings.TrimSpace(group) != "" {
			return CeleryAbsent
		}
	}
	return CeleryUnknown
}

// UnreclaimableSweepResult reports what one pass saw and did. Candidates is
// what selection matched; Terminalized is what was actually written, and is
// always zero in shadow mode.
type UnreclaimableSweepResult struct {
	Mode         SweepMode
	Candidates   int
	Terminalized int
	RunIDs       []string
	Pairs        []string
	UnitIDs      []string
}

// UnreclaimableSweep terminalizes units nothing is working.
type UnreclaimableSweep struct {
	begin  leaseRepairBeginFunc
	config UnreclaimableSweepConfig
}

func NewUnreclaimableSweep(
	pool *pgxpool.Pool,
	config UnreclaimableSweepConfig,
) (*UnreclaimableSweep, error) {
	if pool == nil || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return &UnreclaimableSweep{begin: pool.Begin, config: config}, nil
}

func newUnreclaimableSweep(
	begin leaseRepairBeginFunc,
	config UnreclaimableSweepConfig,
) (*UnreclaimableSweep, error) {
	if begin == nil || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return &UnreclaimableSweep{begin: begin, config: config}, nil
}

type unreclaimableCandidate struct {
	id         string
	syncRunID  string
	orgID      string
	provider   string
	datasetKey string
	costClass  string
	updatedAt  time.Time
}

func (candidate unreclaimableCandidate) pair() string {
	return candidate.provider + "/" + candidate.datasetKey
}

// Step runs one bounded pass.
func (sweep *UnreclaimableSweep) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (UnreclaimableSweepResult, error) {
	if sweep == nil || sweep.begin == nil || ctx == nil || now.IsZero() ||
		limit < unreclaimableMinimumLimit || limit > unreclaimableMaximumLimit {
		return UnreclaimableSweepResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return UnreclaimableSweepResult{}, err
	}
	result := UnreclaimableSweepResult{Mode: sweep.config.Mode}

	// Fail safe: the sweep shares its pass with lease repair and wakeup
	// materialization. An undecidable route disposition must degrade to
	// "sweep nothing", never abort the pass and take those repairs with it.
	if sweep.config.Presence == CeleryUnknown {
		return result, nil
	}

	now = now.UTC()
	tx, err := sweep.begin(ctx)
	if err != nil || tx == nil {
		return UnreclaimableSweepResult{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	candidates, err := sweep.selectUnreclaimable(ctx, tx, now, limit)
	if err != nil {
		return UnreclaimableSweepResult{}, err
	}
	result.Candidates = len(candidates)
	seenRuns := make(map[string]struct{}, len(candidates))
	seenPairs := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		result.UnitIDs = append(result.UnitIDs, candidate.id)
		if _, ok := seenRuns[candidate.syncRunID]; !ok {
			seenRuns[candidate.syncRunID] = struct{}{}
			result.RunIDs = append(result.RunIDs, candidate.syncRunID)
		}
		if _, ok := seenPairs[candidate.pair()]; !ok {
			seenPairs[candidate.pair()] = struct{}{}
			result.Pairs = append(result.Pairs, candidate.pair())
		}
	}

	// Shadow mode reports selection and writes nothing. The transaction is
	// rolled back by the deferred call.
	if sweep.config.Mode != SweepModeActive {
		return result, nil
	}

	for _, candidate := range candidates {
		affected, err := sweep.terminalize(ctx, tx, candidate, now)
		if err != nil {
			return UnreclaimableSweepResult{}, err
		}
		if affected < 0 || affected > 1 {
			return UnreclaimableSweepResult{}, ErrUnavailable
		}
		result.Terminalized += int(affected)
	}
	if err := tx.Commit(ctx); err != nil {
		return UnreclaimableSweepResult{}, ErrUnavailable
	}
	return result, nil
}

// selectUnreclaimable pages until it holds `limit` genuinely sweepable units.
//
// Both the outbox filter and the routability filter are applied INSIDE the
// loop. Applying either after the limit would let a page of ineligible rows
// mask a genuine strand behind them on every pass -- the same deterministic
// loser that CHAOS-3990 fixed one layer up.
func (sweep *UnreclaimableSweep) selectUnreclaimable(
	ctx context.Context,
	tx pgx.Tx,
	now time.Time,
	limit int,
) ([]unreclaimableCandidate, error) {
	ageCutoff := now.Add(-sweep.config.Age)
	idleCutoff := now.Add(-sweep.config.Idle)
	selected := make([]unreclaimableCandidate, 0, limit)
	offset := 0
	for pages := 0; len(selected) < limit && pages < unreclaimableMaximumPages; pages++ {
		page, err := scanUnreclaimablePage(
			ctx, tx, ageCutoff, idleCutoff, limit, offset,
		)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		offset += len(page)
		unpublished, err := dropPublishedUnits(ctx, tx, page)
		if err != nil {
			return nil, err
		}
		for _, candidate := range unpublished {
			if !sweep.unroutable(candidate) {
				continue
			}
			selected = append(selected, candidate)
			if len(selected) == limit {
				break
			}
		}
		if len(page) < limit {
			break
		}
	}
	return selected, nil
}

// unroutable mirrors Python's resolve_unit_transport: a pair is sweepable only
// when River declines it AND nothing else can run it.
func (sweep *UnreclaimableSweep) unroutable(candidate unreclaimableCandidate) bool {
	descriptor, known := sweep.config.Switches.Descriptor(
		candidate.provider, candidate.datasetKey,
	)
	// An unknown pair is not proof of anything; refuse to destroy on a guess.
	if !known {
		return false
	}
	if descriptor.RouteReady && descriptor.RouteEnabled {
		return false
	}
	return sweep.config.Presence == CeleryAbsent
}

// TWO CLOCKS, deliberately.
//
// created_at bounds how long the unit has existed without a single attempt;
// updated_at gates the ordinary stale window. The age bound MUST hang off
// created_at, because the dispatcher's stale reclaim re-stamps updated_at on
// every pass -- an age bound on updated_at resets forever and the sweep
// silently never fires, which is the exact failure this sweep exists to catch.
//
// The heartbeat predicate is NULL *or stale*, not strict NULL: a budget
// deferral stamps last_heartbeat_at even though no worker ran, and the later
// claim never clears it, so strict NULL would permanently exempt every unit
// that was ever budget-deferred.
//
// status = 'dispatching' only. A RUNNING unit is never selected.
const selectUnreclaimableCandidatesSQL = `
SELECT unit.id::text, unit.sync_run_id::text, unit.org_id,
	unit.provider, unit.dataset_key, unit.cost_class, unit.updated_at
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE unit.status = 'dispatching'
	AND unit.lease_owner IS NULL
	AND unit.lease_expires_at IS NULL
	AND (unit.last_heartbeat_at IS NULL OR unit.last_heartbeat_at <= $2)
	AND unit.attempts = 0
	AND unit.created_at <= $1
	AND unit.updated_at <= $2
	AND run.status NOT IN ('success', 'partial_failed', 'failed')
	AND run.org_id = unit.org_id
ORDER BY unit.created_at, unit.id
LIMIT $3 OFFSET $4
`

func scanUnreclaimablePage(
	ctx context.Context,
	tx pgx.Tx,
	ageCutoff time.Time,
	idleCutoff time.Time,
	limit int,
	offset int,
) ([]unreclaimableCandidate, error) {
	rows, err := tx.Query(
		ctx, selectUnreclaimableCandidatesSQL, ageCutoff, idleCutoff, limit, offset,
	)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	page := make([]unreclaimableCandidate, 0, limit)
	for rows.Next() {
		var candidate unreclaimableCandidate
		if err := rows.Scan(
			&candidate.id, &candidate.syncRunID, &candidate.orgID,
			&candidate.provider, &candidate.datasetKey, &candidate.costClass,
			&candidate.updatedAt,
		); err != nil {
			return nil, ErrUnavailable
		}
		page = append(page, candidate)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return page, nil
}

// A unit holding an outbox row DID enter the River relay. River owns its fate
// and CHAOS-3951's reclaim covers it; terminalizing here would race that
// recovery.
const selectPublishedDedupeKeysSQL = `
SELECT dedupe_key
FROM public.worker_job_outbox
WHERE dedupe_key = ANY($1)
`

func dropPublishedUnits(
	ctx context.Context,
	tx pgx.Tx,
	page []unreclaimableCandidate,
) ([]unreclaimableCandidate, error) {
	keys := make([]string, 0, len(page))
	for _, candidate := range page {
		keys = append(keys, unreclaimableDedupeKey(candidate.id))
	}
	rows, err := tx.Query(ctx, selectPublishedDedupeKeysSQL, keys)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	published := make(map[string]struct{}, len(keys))
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, ErrUnavailable
		}
		published[key] = struct{}{}
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	unpublished := make([]unreclaimableCandidate, 0, len(page))
	for _, candidate := range page {
		if _, ok := published[unreclaimableDedupeKey(candidate.id)]; ok {
			continue
		}
		unpublished = append(unpublished, candidate)
	}
	return unpublished, nil
}

func unreclaimableDedupeKey(unitID string) string {
	return unreclaimableProviderUnitID + ":" + unitID
}

// OPTIMISTIC CONCURRENCY on updated_at.
//
// The outbox check happens during selection, so a dispatcher that reclaims and
// publishes this unit afterwards would insert its outbox row while the
// never-leased shape still matched. The dispatcher's claim re-stamps
// updated_at in that same transaction, so pinning the exact value read makes
// any concurrent touch invalidate this write instead of racing it.
const terminalizeUnreclaimableSQL = `
UPDATE public.sync_run_units
SET status = 'failed',
	available_at = NULL,
	error = $2,
	last_retry_reason = $3,
	result = $4::jsonb,
	lease_owner = NULL,
	lease_expires_at = NULL,
	updated_at = $5
WHERE id = $1::uuid
	AND status = 'dispatching'
	AND lease_owner IS NULL
	AND attempts = 0
	AND updated_at = $6
`

func (sweep *UnreclaimableSweep) terminalize(
	ctx context.Context,
	tx pgx.Tx,
	candidate unreclaimableCandidate,
	now time.Time,
) (int64, error) {
	reason := unreclaimableReason(candidate)
	payload, err := json.Marshal(map[string]string{
		"error_category": unreclaimableErrorCategory,
		"reason":         reason,
		"provider":       candidate.provider,
		"dataset_key":    candidate.datasetKey,
	})
	if err != nil {
		return 0, ErrUnavailable
	}
	command, err := tx.Exec(
		ctx, terminalizeUnreclaimableSQL,
		candidate.id, unreclaimableErrorCategory, reason, string(payload),
		now, candidate.updatedAt,
	)
	if err != nil {
		return 0, ErrUnavailable
	}
	return command.RowsAffected(), nil
}

// unreclaimableReason is the durable record an operator reads off the row.
// The bare category alone is what made a retry loop with thousands of attempts
// unexplainable in production (CHAOS-3990).
func unreclaimableReason(candidate unreclaimableCandidate) string {
	return fmt.Sprintf(
		"unreclaimable dispatch for %s: held 'dispatching' with no lease, no "+
			"heartbeat, no attempt and no worker_job_outbox row, so no runtime "+
			"was ever going to execute it",
		candidate.pair(),
	)
}
