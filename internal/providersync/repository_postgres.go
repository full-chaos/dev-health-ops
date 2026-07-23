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
	if watermark != nil {
		if _, err := tx.Exec(ctx, upsertWatermarkSQL,
			uuid.New(), claim.OrgID, claim.SourceExternalID, claim.Dataset,
			watermark.UTC(), completedAt.UTC(),
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

const completeUnitSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'success',
    duration_seconds = $4,
    result = $5::jsonb,
    error = NULL,
    rate_limit_deferrals = 0,
    rate_limit_first_seen_at = NULL,
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

const releaseForRetrySQL = `
UPDATE public.sync_run_units AS unit
SET status = 'dispatching',
    available_at = NULL,
    error = 'provider_unit_retryable',
    result = jsonb_build_object('error_category', 'provider_unit_retryable'),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3`

const failUnitSQL = `
UPDATE public.sync_run_units AS unit
SET status = 'failed',
    duration_seconds = $4,
    error = $5,
    result = $6::jsonb,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = $3,
    updated_at = $3
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3`

const upsertWatermarkSQL = `
INSERT INTO public.sync_watermarks (
    id, org_id, repo_id, source_id, target, dataset_key,
    last_synced_at, updated_at
) VALUES ($1, $2, $3, $3, $4, $4, $5, $6)
ON CONFLICT (org_id, source_id, dataset_key) DO UPDATE
SET last_synced_at = GREATEST(
        public.sync_watermarks.last_synced_at,
        EXCLUDED.last_synced_at
    ),
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
