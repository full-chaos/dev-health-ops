package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"time"

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
	for raw, target := range map[string]*map[string]any{
		string(datasetOptions):    &claim.DatasetOptions,
		string(unitResult):        &claim.Result,
		string(sourceMetadata):    &claim.SourceMetadata,
		string(integrationConfig): &claim.IntegrationConfig,
	} {
		if err := json.Unmarshal([]byte(raw), target); err != nil {
			return Claim{}, ErrInvalidConfiguration
		}
	}
	if err := claim.Validate(); err != nil {
		return Claim{}, err
	}
	return claim, nil
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
        unit.status = 'dispatching'
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

// failUnitSQL MERGES the failure result rather than replacing it, exactly as
// releaseForRetrySQL above already does. A wholesale replace destroyed the
// go_effect_ledger_v1 key, and with it the prepared snapshot reference --
// the only thing that makes a retained sidecar row findable. Contract point 5
// retains the snapshot on failure so a later attempt or an operator can reason
// about it; a retained row whose only pointer has been erased is not retained,
// it is leaked, and nothing short of the sync_run_units CASCADE ever reclaims
// it (up to 64 MiB each).
const failUnitSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'failed',
    duration_seconds = $4,
    error = $5,
    result = COALESCE(unit.result::jsonb, '{}'::jsonb) || $6::jsonb,
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
