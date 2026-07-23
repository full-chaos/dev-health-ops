package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"time"

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
	if repository == nil || repository.Pool == nil || ctx == nil || request.validate() != nil {
		return Claim{}, ErrInvalidConfiguration
	}
	var claim Claim
	var processorFlags, datasetOptions, sourceMetadata, integrationConfig []byte
	err := repository.Pool.QueryRow(
		ctx,
		claimUnitSQL,
		request.UnitID,
		request.Owner,
		request.Now.UTC(),
		request.Now.UTC().Add(request.LeaseDuration),
		request.AllowExpiredRecovery,
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

var _ LeaseRepository = (*PostgresRepository)(nil)
