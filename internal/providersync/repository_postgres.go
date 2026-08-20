package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct{ Pool *pgxpool.Pool }

func NewPostgresRepository(pool *pgxpool.Pool) (*PostgresRepository, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return &PostgresRepository{Pool: pool}, nil
}

func (repository *PostgresRepository) Claim(ctx context.Context, request ClaimRequest) (Claim, error) {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		request.validate() != nil || request.OrgID == "" {
		return Claim{}, ErrInvalidConfiguration
	}
	var claim Claim
	var processorFlags, datasetOptions, unitResult, sourceMetadata, integrationConfig []byte
	err := repository.Pool.QueryRow(
		ctx,
		claimUnitSQL,
		request.UnitID,
		request.Owner,
		request.Now.UTC(),
		request.Now.UTC().Add(request.LeaseDuration),
		request.AllowExpiredRecovery,
		request.OrgID,
	).Scan(
		&claim.ID,
		&claim.SyncRunID,
		&claim.OrgID,
		&claim.IntegrationID,
		&claim.SourceID,
		&claim.Provider,
		&claim.Dataset,
		&claim.CostClass,
		&claim.Mode,
		&claim.SinceAt,
		&claim.BeforeAt,
		&processorFlags,
		&unitResult,
		&claim.Attempt,
		&claim.LeaseExpiresAt,
		&claim.Recovered,
		&claim.SourceExternalID,
		&claim.SourceName,
		&sourceMetadata,
		&datasetOptions,
		&integrationConfig,
		&claim.CredentialID,
		&claim.CredentialFingerprint,
		&claim.AuthSource,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, ErrUnitNotClaimable
	}
	if err != nil {
		return Claim{}, ErrInvalidConfiguration
	}
	claim.Owner = request.Owner
	if decodeErr := decodeClaimJSON(processorFlags, &claim.ProcessorFlags); decodeErr != nil {
		return Claim{}, decodeErr
	}
	if decodeErr := decodeClaimDocuments(
		&claim, datasetOptions, unitResult, sourceMetadata, integrationConfig,
	); decodeErr != nil {
		return Claim{}, decodeErr
	}
	if err := claim.Validate(); err != nil {
		return Claim{}, err
	}
	return claim, nil
}

// decodeClaimDocuments decodes the four JSON documents that ride along with a
// claimed unit into their claim fields. Every document must decode into its
// own target: the claim SQL coalesces each column to '{}', so distinct
// columns routinely carry byte-identical JSON, and pairing raw text with
// targets through a map literal would collapse those duplicates and leave all
// but one target untouched.
func decodeClaimDocuments(
	claim *Claim,
	datasetOptions, unitResult, sourceMetadata, integrationConfig []byte,
) error {
	for _, document := range []struct {
		raw    []byte
		target *map[string]any
	}{
		{datasetOptions, &claim.DatasetOptions},
		{unitResult, &claim.Result},
		{sourceMetadata, &claim.SourceMetadata},
		{integrationConfig, &claim.IntegrationConfig},
	} {
		if err := json.Unmarshal(document.raw, document.target); err != nil {
			return ErrInvalidConfiguration
		}
	}
	return nil
}

// Complete atomically terminalizes the authoritative unit, advances its
// monotonic watermark, and arms the existing finalize outbox. Queue state is
// never treated as the product-state completion record.
func (repository *PostgresRepository) Complete(
	ctx context.Context,
	claim Claim,
	result map[string]any,
	watermark *time.Time,
	startedAt time.Time,
	completedAt time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || result == nil || startedAt.IsZero() ||
		completedAt.Before(startedAt) {
		return ErrInvalidConfiguration
	}
	var err error
	result, watermark, err = applyGitHubWorkItemsIncompletePolicy(
		claim.Provider, claim.Dataset, result, watermark,
	)
	if err != nil {
		return err
	}
	datasetKeys, auditedResult, err := workItemAliasCompletionMetadata(
		claim.Provider, claim.Dataset, claim.ProcessorFlags, result,
	)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(auditedResult)
	if err != nil {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	command, err := tx.Exec(ctx, completeUnitSQL,
		claim.ID, claim.Owner, completedAt.UTC(),
		int(completedAt.Sub(startedAt).Seconds()), encoded,
	)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	// The prepared effect payload is recovery state, not a product record.
	// Delete it in the same transaction that terminalizes the unit so any
	// later watermark/outbox failure rolls both operations back together.
	if _, err := tx.Exec(
		ctx, deletePreparedRouteSnapshotSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	); err != nil {
		return ErrInvalidConfiguration
	}
	// The chunk tables are additive during rolling migration. Older test and
	// deployment fixtures may terminalize a legacy unit before 0102 is
	// installed, so probe the catalog before issuing the cleanup statement.
	if err := deletePreparedChunkStateTx(ctx, tx, claim); err != nil {
		return err
	}
	if watermark != nil {
		for _, datasetKey := range datasetKeys {
			// THE write boundary (CHAOS-3412 C10(c), CHAOS-3427). Every Go
			// watermark write routes through normalizeWatermarkWrite: routes
			// that prefer a provider-supplied watermark over claim.BeforeAt
			// bypass every planner-side clamp otherwise, and the INSERT half
			// of the upsert has no monotonic gate at all to fall back on.
			normalized := normalizeWatermarkWrite(
				*watermark, claim.BeforeAt, completedAt,
				claim.OrgID, claim.SourceExternalID, datasetKey,
			)
			if _, err := tx.Exec(ctx, upsertWatermarkSQL,
				uuid.New(), claim.OrgID, claim.SourceExternalID, datasetKey,
				normalized, completedAt.UTC(),
			); err != nil {
				return ErrInvalidConfiguration
			}
		}
	}
	if _, err := tx.Exec(ctx, upsertFinalizeSQL,
		uuid.New(), claim.OrgID, claim.SyncRunID, completedAt.UTC(),
	); err != nil {
		return ErrInvalidConfiguration
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

// CompleteLinearWorkItemFamily is the typed completion boundary for the
// Linear five-alias family. It writes the canonical unit audit and all five
// alias watermarks in one PostgreSQL transaction; callers cannot accidentally
// complete one alias while leaving the others unadvanced. The generic
// Complete method remains for legacy providers and is intentionally not used
// by this proof path.
func (repository *PostgresRepository) CompleteLinearWorkItemFamily(
	ctx context.Context,
	claim Claim,
	result LinearWorkItemsCompletionResult,
	startedAt time.Time,
	completedAt time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || startedAt.IsZero() || completedAt.Before(startedAt) ||
		ValidateLinearWorkItemsCompletion(claim, result) != nil {
		return ErrInvalidConfiguration
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	command, err := tx.Exec(ctx, completeUnitSQL,
		claim.ID, claim.Owner, completedAt.UTC(),
		int(completedAt.Sub(startedAt).Seconds()), encoded,
	)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, deletePreparedRouteSnapshotSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	); err != nil {
		return ErrInvalidConfiguration
	}
	for _, datasetKey := range workitemcontract.FamilyDatasets() {
		normalized := normalizeWatermarkWrite(
			result.Watermark, claim.BeforeAt, completedAt,
			claim.OrgID, claim.SourceExternalID, datasetKey,
		)
		if _, err := tx.Exec(ctx, upsertWatermarkSQL,
			uuid.New(), claim.OrgID, claim.SourceExternalID, datasetKey,
			normalized, completedAt.UTC(),
		); err != nil {
			return ErrInvalidConfiguration
		}
	}
	if _, err := tx.Exec(ctx, upsertFinalizeSQL,
		uuid.New(), claim.OrgID, claim.SyncRunID, completedAt.UTC(),
	); err != nil {
		return ErrInvalidConfiguration
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

const deletePreparedRouteSnapshotSQL = `
DELETE FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`

// ReleaseForRetry returns a live claim to dispatching for the same River job's
// bounded retry. A process death cannot call this method; expired-lease
// recovery remains the fresh-process path in Claim.
func (repository *PostgresRepository) ReleaseForRetry(
	ctx context.Context,
	claim Claim,
	now time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() {
		return ErrInvalidConfiguration
	}
	command, err := repository.Pool.Exec(ctx, releaseForRetrySQL,
		claim.ID, claim.Owner, now.UTC(),
	)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// DeferForBudgetContention records a healthy request-reservation collision.
// It is intentionally separate from the Python planner's intrinsic
// budget_deferred episode: a sibling holding a short-lived HTTP slot says
// nothing about whether this unit can fit the configured sync budget.
// RateLimitEpisode is the persisted rate-limit deferral bookkeeping for one
// unit. Python carries the equivalent state in task kwargs
// (rate_limit_deferrals / rate_limit_first_seen_at); Go keeps it on the row so
// a process restart resumes the same episode rather than starting a fresh
// 2-hour budget.
type RateLimitEpisode struct {
	Deferrals   int
	FirstSeenAt *time.Time
}

// RateLimitEpisode reads the current deferral counters for a leased unit.
func (repository *PostgresRepository) RateLimitEpisode(
	ctx context.Context,
	claim Claim,
) (RateLimitEpisode, error) {
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil {
		return RateLimitEpisode{}, ErrInvalidConfiguration
	}
	var episode RateLimitEpisode
	if err := repository.Pool.QueryRow(ctx, `
		SELECT COALESCE(rate_limit_deferrals, 0), rate_limit_first_seen_at
		FROM public.sync_run_units
		WHERE id = $1::uuid`, claim.ID,
	).Scan(&episode.Deferrals, &episode.FirstSeenAt); err != nil {
		return RateLimitEpisode{}, ErrInvalidConfiguration
	}
	return episode, nil
}

// DeferForRateLimit keeps a rate-limited unit claimable without consuming its
// bounded failure budget, mirroring Python's treatment of a 429 as deferred
// work rather than a failure. The attempt decrement matches
// DeferForBudgetContention: River's snooze is already attempt-neutral, and the
// authoritative sync-unit attempt must stay neutral with it.
func (repository *PostgresRepository) DeferForRateLimit(
	ctx context.Context,
	claim Claim,
	availableAt time.Time,
	now time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() || !availableAt.After(now) ||
		availableAt.Sub(now) > rateLimitMaxTotalWait {
		return ErrInvalidConfiguration
	}
	command, err := repository.Pool.Exec(
		ctx, deferForRateLimitSQL,
		claim.ID, claim.Owner, now.UTC(), availableAt.UTC(),
	)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// rateLimitMaxTotalWait bounds a single deferral's not-before fence. It equals
// the episode wall-clock budget in internal/jobs/providerunit, which is the
// longest a deferral could legitimately be scheduled for.
const rateLimitMaxTotalWait = 2 * time.Hour

func (repository *PostgresRepository) DeferForBudgetContention(
	ctx context.Context,
	claim Claim,
	availableAt time.Time,
	now time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() || !availableAt.After(now) ||
		availableAt.Sub(now) > 5*time.Minute {
		return ErrInvalidConfiguration
	}
	command, err := repository.Pool.Exec(
		ctx, deferForBudgetContentionSQL,
		claim.ID, claim.Owner, now.UTC(), availableAt.UTC(),
	)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// DeferChunkContinuation keeps a prepared chunk unit claimable for the same
// River job without consuming either River's attempt or the authoritative
// sync-unit attempt. The durable checkpoint remains fenced by the generation;
// the next claim resumes from its next ordinal.
func (repository *PostgresRepository) DeferChunkContinuation(
	ctx context.Context,
	claim Claim,
	availableAt time.Time,
	now time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() || !availableAt.After(now) ||
		availableAt.Sub(now) > 15*time.Minute {
		return ErrInvalidConfiguration
	}
	command, err := repository.Pool.Exec(
		ctx, deferForChunkContinuationSQL, claim.ID, claim.Owner,
		now.UTC(), availableAt.UTC(),
	)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// Fail terminalizes an exhausted unit and arms run finalization.
func (repository *PostgresRepository) Fail(
	ctx context.Context,
	claim Claim,
	category string,
	startedAt time.Time,
	completedAt time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || category == "" || len(category) > 64 ||
		startedAt.IsZero() || completedAt.Before(startedAt) {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, marshalErr := json.Marshal(map[string]any{"error_category": category})
	if marshalErr != nil {
		return ErrInvalidConfiguration
	}
	command, err := tx.Exec(ctx, failUnitSQL,
		claim.ID, claim.Owner, completedAt.UTC(),
		int(completedAt.Sub(startedAt).Seconds()), category, result,
	)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	// Unlike a prepared recovery snapshot, a chunk checkpoint is only ever
	// resumable by a RUNNING unit of the same generation. Terminalizing the
	// unit makes its sidecars unreachable, so keeping them would retain
	// provider payloads that nothing can read and nothing else reclaims.
	if err := deletePreparedChunkStateTx(ctx, tx, claim); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, upsertFinalizeSQL,
		uuid.New(), claim.OrgID, claim.SyncRunID, completedAt.UTC(),
	); err != nil {
		return ErrInvalidConfiguration
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func decodeClaimJSON(raw []byte, target *map[string]bool) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return ErrInvalidConfiguration
	}
	if *target == nil {
		*target = map[string]bool{}
	}
	return nil
}

func (repository *PostgresRepository) Assert(ctx context.Context, claim Claim, now time.Time) error {
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil || now.IsZero() {
		return ErrLeaseLost
	}
	var live bool
	if err := repository.Pool.QueryRow(ctx, assertLeaseSQL, claim.ID, claim.Owner, now.UTC()).Scan(&live); err != nil || !live {
		return ErrLeaseLost
	}
	return nil
}

func (repository *PostgresRepository) Renew(
	ctx context.Context,
	claim Claim,
	now time.Time,
	expiresAt time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil ||
		now.IsZero() || !expiresAt.After(now) {
		return ErrLeaseLost
	}
	command, err := repository.Pool.Exec(ctx, renewLeaseSQL, claim.ID, claim.Owner, now.UTC(), expiresAt.UTC())
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

const claimUnitSQL = `
WITH candidate AS (
    SELECT unit.id, unit.status = 'running' AS recovered
    FROM public.sync_run_units AS unit
    JOIN public.sync_runs AS run
      ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
    WHERE unit.id = $1::uuid
      AND run.status NOT IN ('success', 'partial_failed', 'failed')
      AND (
        (
          unit.status = 'dispatching'
          AND (unit.available_at IS NULL OR unit.available_at <= $3)
        )
        OR (
          $5::boolean
          AND unit.status = 'running'
          AND unit.lease_expires_at IS NOT NULL
          AND unit.lease_expires_at <= $3
        )
      )
      AND unit.org_id = $6
    FOR UPDATE OF unit
),
claimed AS (
    UPDATE public.sync_run_units AS unit
    SET status = 'running',
        attempts = unit.attempts + 1,
        available_at = NULL,
        error = NULL,
        lease_owner = $2,
        lease_expires_at = $4,
        last_heartbeat_at = $3,
        expired_lease_retry_count = unit.expired_lease_retry_count
          + CASE WHEN candidate.recovered THEN 1 ELSE 0 END,
        last_retry_reason = CASE WHEN candidate.recovered THEN 'expired_lease' ELSE unit.last_retry_reason END,
        updated_at = $3
    FROM candidate
    WHERE unit.id = candidate.id
    RETURNING unit.*, candidate.recovered
)
SELECT
    claimed.id::text,
    claimed.sync_run_id::text,
    claimed.org_id,
    claimed.integration_id::text,
    claimed.source_id::text,
    claimed.provider,
    claimed.dataset_key,
    claimed.cost_class,
    claimed.mode,
    claimed.since_at,
    claimed.before_at,
    COALESCE(claimed.processor_flags::text, '{}'),
    COALESCE(claimed.result::text, '{}'),
    claimed.attempts,
    claimed.lease_expires_at,
    claimed.recovered,
    source.external_id,
    source.full_name,
    COALESCE(source.metadata::text, '{}'),
    COALESCE(dataset.options::text, '{}'),
    COALESCE(integration.config::text, '{}'),
    COALESCE(run.credential_id, integration.credential_id)::text,
    COALESCE(run.credential_fingerprint, ''),
    COALESCE(run.auth_source, 'integration_credential')
FROM claimed
JOIN public.sync_runs AS run
  ON run.id = claimed.sync_run_id AND run.org_id = claimed.org_id
JOIN public.integrations AS integration
  ON integration.id = claimed.integration_id AND integration.org_id = claimed.org_id
JOIN public.integration_sources AS source
  ON source.id = claimed.source_id
 AND source.integration_id = claimed.integration_id
 AND source.org_id = claimed.org_id
LEFT JOIN public.integration_datasets AS dataset
  ON dataset.integration_id = claimed.integration_id
 AND dataset.org_id = claimed.org_id
 AND dataset.dataset_key = claimed.dataset_key`

const assertLeaseSQL = `
SELECT EXISTS (
    SELECT 1
    FROM public.sync_run_units AS unit
    JOIN public.sync_runs AS run
      ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
    WHERE unit.id = $1::uuid
      AND unit.status = 'running'
      AND unit.lease_owner = $2
      AND unit.lease_expires_at IS NOT NULL
      AND unit.lease_expires_at > $3
      AND run.status NOT IN ('success', 'partial_failed', 'failed')
)`

const renewLeaseSQL = `
UPDATE public.sync_run_units AS unit
SET lease_expires_at = $4,
    last_heartbeat_at = $3,
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3
  AND EXISTS (
    SELECT 1
    FROM public.sync_runs AS run
    WHERE run.id = unit.sync_run_id
      AND run.org_id = unit.org_id
      AND run.status NOT IN ('success', 'partial_failed', 'failed')
  )`

// completeUnitSQL mirrors the Python SUCCESS stamp
// (src/dev_health_ops/workers/sync_units.py, the `status=SUCCESS` UPDATE) in
// its episode bookkeeping, not only in its status transition. Python clears
// THREE things there and every one of them is load-bearing:
//
//   - the rate-limit episode pair (CHAOS-2760),
//   - the budget episode pair (CHAOS-3412) -- SUCCESS proves the unit is not
//     permanently oversized, so its next budget deferral must start a fresh
//     count and a fresh wall clock instead of inheriting a resolved episode's,
//   - the AGGREGATE blocked clock `first_blocked_at` -- the unit got through,
//     so it is not "going nowhere" any more.
//
// Leaving any of them set here hands `sync/budget_guard.py`'s exhaustion
// predicates a resolved episode's counters on the unit's NEXT deferral, and
// terminalizes a healthy unit early. Python cannot see this SQL, so its own
// derivation guard (test_deferral_lifecycle_columns_are_classified_and_stamped_correctly)
// cannot catch a half-wired Go stamp -- repository_postgres_sql_test.go asserts
// it on this side.
//
// releaseForRetrySQL below deliberately does NOT clear any of them; see its
// own comment.
const completeUnitSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'success',
    duration_seconds = $4,
    result = $5::jsonb,
    error = NULL,
    rate_limit_deferrals = 0,
    rate_limit_first_seen_at = NULL,
    budget_deferrals = 0,
    budget_first_deferred_at = NULL,
    first_blocked_at = NULL,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3
  AND EXISTS (
    SELECT 1 FROM public.sync_runs AS run
    WHERE run.id = unit.sync_run_id
      AND run.org_id = unit.org_id
      AND run.status NOT IN ('success', 'partial_failed', 'failed')
  )`

// releaseForRetrySQL merges 'error_category' into the existing result
// document rather than replacing it wholesale (codex H1, second half,
// CHAOS-3122). The prior blind `result = jsonb_build_object(...)` overwrite
// deleted the go_effect_ledger_v1 key whenever a unit failed after an
// earlier attempt had loaded (or begun writing) an effect: the next attempt
// then found no ledger, forgot the frozen normalizedAt/digest a
// EffectReadbackRequired effect depends on, and could no longer classify a
// possibly-landed-but-uncommitted ClickHouse row as exact, absent, or
// conflict. A capped-pagination failure (see github_prs_route.go) made this
// newly, deterministically reachable, but the gap itself predates that fix
// and applies to any Collect failure after a ledger was loaded, on any
// EffectReadbackRequired pair. A single UPDATE's SET expression reads the
// current row atomically -- no separate lock is needed the way
// mutateGenerationJournal's two-round-trip read-modify-write requires.
//
// CHAOS-3427: this stamp deliberately clears NEITHER episode pair nor the
// aggregate blocked clock. That asymmetry with completeUnitSQL is the same one
// Python has, and it is load-bearing: `_RATE_LIMIT_EPISODE_ERROR_CATEGORIES`
// (budget_guard.py) does not contain 'provider_unit_retryable', so a release
// for retry is not a rate-limit or budget episode boundary and must not reset
// an episode that is still genuinely in progress. Do not "fix" it into
// symmetry with the SUCCESS stamp.
//
// The jsonb merge here keeps OTHER result keys (go_effect_ledger_v1) but the
// concatenation puts jsonb_build_object on the RIGHT, so 'error_category' is
// OVERWRITTEN, never preserved. That direction is a hard rule, not an
// accident -- see repository_postgres_sql_test.go's
// TestNoUnitStampPreservesAPriorErrorCategory.
const releaseForRetrySQL = `
UPDATE public.sync_run_units AS unit
SET status = 'dispatching',
    available_at = NULL,
    error = 'provider_unit_retryable',
    result = (
      COALESCE(unit.result::jsonb, '{}'::jsonb) ||
      jsonb_build_object('error_category', 'provider_unit_retryable')
    ),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3`

// deferForBudgetContentionSQL keeps the unit claimable by the SAME snoozed
// River job after $4, while stale-dispatch repair remains a much later safety
// net. The distinct result counter survives process restarts and does not
// increment or clear either existing Python deferral episode. The claim's
// domain attempt is restored under the same lease CAS because healthy sibling
// contention is neutral in both River and sync_run_units. The aggregate
// first_blocked_at clock remains set until SUCCESS.
const deferForBudgetContentionSQL = `
WITH contention AS (
    SELECT unit.id,
           COALESCE(
             CASE
               WHEN jsonb_typeof(unit.result::jsonb) = 'object'
               THEN (unit.result::jsonb -> 'provider_budget_contention_deferrals')::integer
               ELSE 0
             END,
             0
           ) + 1 AS next_deferrals
    FROM public.sync_run_units AS unit
    WHERE unit.id = $1::uuid
)
UPDATE public.sync_run_units AS unit
SET status = 'dispatching',
    attempts = GREATEST(unit.attempts - 1, 0),
    available_at = $4,
    error = 'provider_budget_contention',
    result = (
      COALESCE(unit.result::jsonb, jsonb_build_object()) ||
      jsonb_build_object(
        'error_category', 'provider_budget_contention',
        'not_before', to_jsonb($4::timestamptz),
        'provider_budget_contention_deferrals', contention.next_deferrals
      )
    ),
    first_blocked_at = COALESCE(unit.first_blocked_at, $3),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    last_retry_reason = 'provider_budget_contention',
    updated_at = $3
FROM contention
WHERE unit.id = contention.id
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3`

const deferForRateLimitSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'dispatching',
    attempts = GREATEST(unit.attempts - 1, 0),
    available_at = $4,
    error = 'provider_rate_limited',
    rate_limit_deferrals = COALESCE(unit.rate_limit_deferrals, 0) + 1,
    rate_limit_first_seen_at = COALESCE(unit.rate_limit_first_seen_at, $3),
    result = (
      COALESCE(unit.result::jsonb, jsonb_build_object()) ||
      jsonb_build_object(
        'error_category', 'provider_rate_limited',
        'not_before', to_jsonb($4::timestamptz),
        'rate_limit_deferrals', COALESCE(unit.rate_limit_deferrals, 0) + 1
      )
    ),
    first_blocked_at = COALESCE(unit.first_blocked_at, $3),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    last_retry_reason = 'provider_rate_limited',
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3`

const deferForChunkContinuationSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'dispatching',
    attempts = GREATEST(unit.attempts - 1, 0),
    available_at = $4,
    error = 'provider_unit_chunk_continuation',
    result = (
      COALESCE(unit.result::jsonb, jsonb_build_object()) ||
      jsonb_build_object(
        'error_category', 'provider_unit_chunk_continuation',
        'not_before', to_jsonb($4::timestamptz)
      )
    ),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    last_retry_reason = 'provider_unit_chunk_continuation',
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3`

// failUnitSQL carries forward EXACTLY ONE key and drops the rest.
//
// A wholesale replace lost go_effect_ledger_v1, and with it the prepared
// snapshot reference. Contract point 5 retains the snapshot on failure, and
// the reference is what makes the retained row VALIDATABLE -- the row is still
// findable by sync_run_unit_id, but without the digest, size and schema
// recorded in the ledger nothing can tell a good payload from a corrupt one.
//
// A blanket `unit.result || $6` overcorrected. The result document also
// accumulates CLAIMABLE-STATE keys -- next_retry_at, retry_exhausted,
// retry_reason, and the rate-limit and budget deferral keys -- written by
// markExpiredLeaseRetryingSQL, the soft-timeout retry path, rate-limit
// deferral and the budget guard. Nothing clears them on claim, so preserving
// everything freezes a live "will retry at T" claim onto a unit that is
// terminally failed. The admin integrations API projects these keys with no
// status gate and treats them as authoritative, and every other terminal stamp
// in the repo deliberately nulls next_retry_at (lease_repair.go,
// sync_units.py) -- this was the only one that would not have.
//
// The CASE is not decoration: unit.result is sa.JSON, so it can hold a JSON
// literal `null`, and `'null'::jsonb || '{...}'::jsonb` does NOT raise -- it
// ARRAY-concatenates to `[null, {...}]`. The failure document then has no
// readable error_category at all, because `->>` on an array returns NULL. A
// raise would have been the kinder outcome; this loses the reason a unit
// failed, silently. Verified on PostgreSQL 18.4 rather than assumed.
//
// `?` returns false for every non-object, so it is the whole guard: a null,
// array or scalar predecessor takes the ELSE branch, and the THEN branch only
// ever concatenates two objects.
const failUnitSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'failed',
    duration_seconds = $4,
    error = $5,
    result = CASE
      WHEN unit.result::jsonb ? 'go_effect_ledger_v1'
      THEN jsonb_build_object(
             'go_effect_ledger_v1', unit.result::jsonb -> 'go_effect_ledger_v1'
           ) || $6::jsonb
      ELSE $6::jsonb
    END,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3`

// upsertWatermarkSQL is monotonic (CHAOS-2578) with ONE narrow exception
// (CHAOS-3412 clause C10(b), mirrored from Python's `_monotonic_update`):
// when the STORED value is in the future and the incoming one is not, the
// incoming value wins.
//
// The exception is gated on provably-invalid state only. A watermark marks
// data ALREADY synced, so it can never legitimately sit ahead of now; such a
// value can only come from a skewed provider record or from pre-fix planner
// code that persisted a future window end. Because the write is otherwise
// monotonic, nothing could ever lower it again: the planner's window would
// start in the future, plan no unit, and the run would finalize FAILED
// forever with nothing left to repair it. C10(a) (the planner-side recovery
// clamp) and this clause only work together -- without this, the healing
// re-stamp is silently discarded by GREATEST and every tick re-syncs a
// recovery window forever.
//
// Widening this to "any lower value wins" would be a defect, not a
// simplification: it destroys the CHAOS-2578 guarantee that a late or
// out-of-order result cannot roll a LEGITIMATE watermark backwards. Python
// pinned that by mutation; repository_postgres_sql_test.go pins it here.
//
// $6 is the write instant, used as `now` for the future test in-database so
// the decision stays atomic against concurrent writers.
const upsertWatermarkSQL = `
INSERT INTO public.sync_watermarks (
    id, org_id, repo_id, source_id, target, dataset_key,
    last_synced_at, updated_at
) VALUES ($1, $2, $3, $3, $4, $4, $5, $6)
ON CONFLICT (org_id, source_id, dataset_key) DO UPDATE
SET last_synced_at = CASE
        WHEN public.sync_watermarks.last_synced_at > $6::timestamptz
         AND EXCLUDED.last_synced_at <= $6::timestamptz
        THEN EXCLUDED.last_synced_at
        ELSE GREATEST(
            public.sync_watermarks.last_synced_at,
            EXCLUDED.last_synced_at
        )
    END,
    updated_at = EXCLUDED.updated_at`

const upsertFinalizeSQL = `
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    created_at, updated_at
) VALUES ($1, $2, $3::uuid, 'finalize_sync_run', 'pending', $4, 0, $4, $4)
ON CONFLICT (sync_run_id, kind) DO UPDATE
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

var _ LeaseRepository = (*PostgresRepository)(nil)
