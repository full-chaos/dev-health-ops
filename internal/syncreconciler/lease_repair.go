package syncreconciler

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	expiredLeaseDefaultMaximumRetries int64 = 1
	expiredLeaseDefaultRetryBackoff         = time.Minute
	leaseRepairMinimumLimit                 = 1
	leaseRepairMaximumLimit                 = 100
	leaseRepairRetryReason                  = "expired_lease"
	leaseRepairExpiredError                 = "sync unit lease expired"
	leaseRepairWorkerLostCategory           = "worker_lost"
	leaseRepairRetryExhaustedCategory       = "worker_lost_retry_exhausted"
)

var linearBackfillWorkItemDatasets = map[string]struct{}{
	"work_items":         {},
	"work_item_labels":   {},
	"work_item_projects": {},
	"work_item_history":  {},
	"work_item_comments": {},
}

var linearBackfillRetrySurfaces = []string{
	"ai_attribution",
	"estimate_coverage_metrics_daily",
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
	"sprints",
	"work_item_cycle_times",
	"work_item_dependencies",
	"work_item_interactions",
	"work_item_metrics_daily",
	"work_item_reopen_events",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_transitions",
	"work_item_user_metrics_daily",
	"work_items",
}

type leaseRepairBeginFunc func(context.Context) (pgx.Tx, error)

// LeaseRepairResult contains only committed state transitions. It does not
// materialize a dispatch wakeup: that remains the responsibility of the
// existing materializer, so future wiring can choose an explicit transaction
// boundary for wakeup delivery.
type LeaseRepairResult struct {
	Selected int
	Retried  int
	Failed   int
}

// LeaseRepairConfig holds the policy that Python currently supplies through
// SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES and
// SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS. Defaults intentionally match
// that compatibility contract, while validation rejects negative policy values.
type LeaseRepairConfig struct {
	MaximumRetries int64
	RetryBackoff   time.Duration
}

func DefaultLeaseRepairConfig() LeaseRepairConfig {
	return LeaseRepairConfig{
		MaximumRetries: expiredLeaseDefaultMaximumRetries,
		RetryBackoff:   expiredLeaseDefaultRetryBackoff,
	}
}

func (config LeaseRepairConfig) valid() bool {
	return config.MaximumRetries >= 0 && config.RetryBackoff >= 0
}

// LeaseRepair is a dormant PostgreSQL-only repair primitive. Construction has
// no side effects, and no command constructs it today. Its SQL locks a bounded
// ordered candidate window and then uses the observed owner in every terminal
// write so concurrent replicas cannot repair a live or replaced lease.
type LeaseRepair struct {
	begin  leaseRepairBeginFunc
	config LeaseRepairConfig
}

func NewLeaseRepair(pool *pgxpool.Pool) (*LeaseRepair, error) {
	return NewLeaseRepairWithConfig(pool, DefaultLeaseRepairConfig())
}

func NewLeaseRepairWithConfig(pool *pgxpool.Pool, config LeaseRepairConfig) (*LeaseRepair, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newLeaseRepairWithConfig(pool.Begin, config)
}

func newLeaseRepair(begin leaseRepairBeginFunc) (*LeaseRepair, error) {
	return newLeaseRepairWithConfig(begin, DefaultLeaseRepairConfig())
}

func newLeaseRepairWithConfig(begin leaseRepairBeginFunc, config LeaseRepairConfig) (*LeaseRepair, error) {
	if begin == nil || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return &LeaseRepair{begin: begin, config: config}, nil
}

// Step repairs one deterministic window of expired RUNNING sync units. It
// never creates transport/outbox rows. A future command owner must arrange
// any wakeup materialization separately after this committed state change.
func (repair *LeaseRepair) Step(ctx context.Context, now time.Time, limit int) (LeaseRepairResult, error) {
	if repair == nil || repair.begin == nil || ctx == nil || now.IsZero() ||
		limit < leaseRepairMinimumLimit || limit > leaseRepairMaximumLimit {
		return LeaseRepairResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return LeaseRepairResult{}, err
	}
	now = now.UTC()

	tx, err := repair.begin(ctx)
	if err != nil || tx == nil {
		return LeaseRepairResult{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	candidates, err := selectExpiredLeaseCandidates(ctx, tx, now, limit)
	if err != nil {
		return LeaseRepairResult{}, err
	}
	if err := acquireLeaseRepairBucketLocks(ctx, tx, candidates); err != nil {
		return LeaseRepairResult{}, err
	}
	result := LeaseRepairResult{Selected: len(candidates)}
	for _, candidate := range candidates {
		decision := decideExpiredLeaseRepair(candidate, repair.config)
		var affected int64
		if decision.retry {
			affected, err = markExpiredLeaseRetrying(ctx, tx, candidate, now, repair.config)
			if err == nil && affected == 1 {
				result.Retried++
			}
		} else {
			affected, err = markExpiredLeaseFailed(ctx, tx, candidate, decision.exhausted, now)
			if err == nil && affected == 1 {
				result.Failed++
			}
		}
		if err != nil {
			return LeaseRepairResult{}, err
		}
		if affected < 0 || affected > 1 {
			return LeaseRepairResult{}, ErrUnavailable
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseRepairResult{}, ErrUnavailable
	}
	return result, nil
}

type expiredLeaseCandidate struct {
	id         string
	syncRunID  string
	leaseOwner string
	provider   string
	mode       string
	datasetKey string
	orgID      string
	costClass  string
	retryCount int64
}

type expiredLeaseDecision struct {
	retry     bool
	exhausted bool
}

func decideExpiredLeaseRepair(candidate expiredLeaseCandidate, config LeaseRepairConfig) expiredLeaseDecision {
	_, eligibleDataset := linearBackfillWorkItemDatasets[candidate.datasetKey]
	eligible := candidate.provider == "linear" && candidate.mode == "backfill" && eligibleDataset
	return expiredLeaseDecision{
		retry:     eligible && candidate.retryCount < config.MaximumRetries,
		exhausted: eligible && candidate.retryCount >= config.MaximumRetries,
	}
}

func selectExpiredLeaseCandidates(
	ctx context.Context,
	tx pgx.Tx,
	now time.Time,
	limit int,
) ([]expiredLeaseCandidate, error) {
	rows, err := tx.Query(ctx, selectExpiredLeaseCandidatesSQL, now, limit)
	if err != nil || rows == nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	candidates := make([]expiredLeaseCandidate, 0, limit)
	seen := make(map[string]struct{}, limit)
	for rows.Next() {
		var candidate expiredLeaseCandidate
		if err := rows.Scan(
			&candidate.id,
			&candidate.syncRunID,
			&candidate.leaseOwner,
			&candidate.provider,
			&candidate.mode,
			&candidate.datasetKey,
			&candidate.orgID,
			&candidate.costClass,
			&candidate.retryCount,
		); err != nil {
			return nil, ErrUnavailable
		}
		if !uuidPattern.MatchString(candidate.id) || !uuidPattern.MatchString(candidate.syncRunID) ||
			candidate.leaseOwner == "" || candidate.orgID == "" || candidate.costClass == "" || candidate.retryCount < 0 {
			return nil, ErrUnavailable
		}
		if _, duplicate := seen[candidate.id]; duplicate {
			return nil, ErrUnavailable
		}
		seen[candidate.id] = struct{}{}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrUnavailable
	}
	if len(candidates) > limit {
		return nil, ErrUnavailable
	}
	return candidates, nil
}

func markExpiredLeaseRetrying(
	ctx context.Context,
	tx pgx.Tx,
	candidate expiredLeaseCandidate,
	now time.Time,
	config LeaseRepairConfig,
) (int64, error) {
	retryAt := now.Add(config.RetryBackoff)
	command, err := tx.Exec(ctx, markExpiredLeaseRetryingSQL,
		candidate.id, candidate.leaseOwner, now, retryAt, linearBackfillRetrySurfaces)
	if err != nil {
		return 0, ErrUnavailable
	}
	return command.RowsAffected(), nil
}

func markExpiredLeaseFailed(
	ctx context.Context,
	tx pgx.Tx,
	candidate expiredLeaseCandidate,
	exhausted bool,
	now time.Time,
) (int64, error) {
	category := leaseRepairWorkerLostCategory
	surfaces := []string{}
	if exhausted {
		category = leaseRepairRetryExhaustedCategory
		surfaces = linearBackfillRetrySurfaces
	}
	command, err := tx.Exec(ctx, markExpiredLeaseFailedSQL,
		candidate.id, candidate.leaseOwner, now, category, exhausted, surfaces)
	if err != nil {
		return 0, ErrUnavailable
	}
	return command.RowsAffected(), nil
}

// selectExpiredLeaseCandidatesSQL is intentionally independent from the
// dispatch materializer. It is a bounded metadata scan only: Python acquires
// sorted advisory bucket locks before mutating capacity state, so this Go
// component follows the same lock order rather than taking row locks first.
// org_id equality is a defensive tenancy fence for malformed rows.
const selectExpiredLeaseCandidatesSQL = `
SELECT unit.id::text, unit.sync_run_id::text, unit.lease_owner,
	unit.provider, unit.mode, unit.dataset_key, unit.org_id, unit.cost_class,
	unit.expired_lease_retry_count
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE unit.status = 'running'
	AND unit.lease_owner IS NOT NULL
	AND unit.lease_expires_at IS NOT NULL
	AND unit.lease_expires_at <= $1
	AND run.status NOT IN ('success', 'partial_failed', 'failed')
	AND run.org_id = unit.org_id
ORDER BY unit.lease_expires_at, unit.id
LIMIT $2
`

type leaseRepairBucket struct {
	orgID      string
	provider   string
	costClass  string
	advisoryID int64
}

func acquireLeaseRepairBucketLocks(ctx context.Context, tx pgx.Tx, candidates []expiredLeaseCandidate) error {
	buckets := make(map[string]leaseRepairBucket, len(candidates))
	for _, candidate := range candidates {
		key := candidate.orgID + "\x00" + candidate.provider + "\x00" + candidate.costClass
		buckets[key] = leaseRepairBucket{
			orgID: candidate.orgID, provider: candidate.provider, costClass: candidate.costClass,
			advisoryID: leaseRepairBucketAdvisoryID(candidate.orgID, candidate.provider, candidate.costClass),
		}
	}
	ordered := make([]leaseRepairBucket, 0, len(buckets))
	for _, bucket := range buckets {
		ordered = append(ordered, bucket)
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
	for _, bucket := range ordered {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", bucket.advisoryID); err != nil {
			return ErrUnavailable
		}
	}
	return nil
}

func leaseRepairBucketAdvisoryID(orgID, provider, costClass string) int64 {
	digest := sha256.Sum256([]byte(orgID + ":" + provider + ":" + costClass))
	return int64(binary.BigEndian.Uint64(digest[:8]) & ((uint64(1) << 63) - 1))
}

// Both mutations repeat the status, owner, expiry, nonterminal-run, and
// tenancy predicates. This CAS complements the advisory locks: it remains
// correct if a future caller reuses the write helper without the selector and
// fails closed if ownership changes before the write.
const markExpiredLeaseRetryingSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'retrying',
	available_at = $4,
	error = 'sync unit lease expired',
	result = jsonb_build_object(
		'error_category', 'worker_lost',
		'retry_count', unit.expired_lease_retry_count + 1,
		'retry_reason', 'expired_lease',
		'next_retry_at', to_jsonb($4::timestamptz),
		'retry_exhausted', FALSE,
		'retry_surfaces', to_jsonb($5::text[]),
		'last_lease_expired_at', to_jsonb($3::timestamptz)
	),
	expired_lease_retry_count = unit.expired_lease_retry_count + 1,
	last_retry_reason = 'expired_lease',
	retry_exhausted_at = NULL,
	rate_limit_deferrals = 0,
	rate_limit_first_seen_at = NULL,
	updated_at = $3,
	lease_owner = NULL,
	lease_expires_at = NULL
FROM public.sync_runs AS run
WHERE unit.id = $1::uuid
	AND unit.lease_owner = $2
	AND unit.status = 'running'
	AND unit.lease_owner IS NOT NULL
	AND unit.lease_expires_at IS NOT NULL
	AND unit.lease_expires_at <= $3
	AND run.id = unit.sync_run_id
	AND run.status NOT IN ('success', 'partial_failed', 'failed')
	AND run.org_id = unit.org_id
`

const markExpiredLeaseFailedSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'failed',
	error = 'sync unit lease expired',
	result = jsonb_build_object(
		'error_category', $4::text,
		'retry_count', unit.expired_lease_retry_count,
		'retry_reason', 'expired_lease',
		'next_retry_at', NULL,
		'retry_exhausted', $5::boolean,
		'retry_surfaces', to_jsonb($6::text[]),
		'last_lease_expired_at', to_jsonb($3::timestamptz)
	),
	last_retry_reason = 'expired_lease',
	retry_exhausted_at = CASE WHEN $5::boolean THEN $3 ELSE NULL END,
	updated_at = $3,
	lease_owner = NULL,
	lease_expires_at = NULL
FROM public.sync_runs AS run
WHERE unit.id = $1::uuid
	AND unit.lease_owner = $2
	AND unit.status = 'running'
	AND unit.lease_owner IS NOT NULL
	AND unit.lease_expires_at IS NOT NULL
	AND unit.lease_expires_at <= $3
	AND run.id = unit.sync_run_id
	AND run.status NOT IN ('success', 'partial_failed', 'failed')
	AND run.org_id = unit.org_id
`
