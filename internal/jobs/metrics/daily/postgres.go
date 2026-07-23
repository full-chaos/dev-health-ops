package daily

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultLease = 10 * time.Minute

// PostgresStore is the durable fence around the temporary compatibility
// compute adapter. Queue retries may repeat a request, but only a claimant
// with the current persisted token can make a partition/finalizer successful.
type PostgresStore struct {
	pool  *pgxpool.Pool
	lease time.Duration
	now   func() time.Time
}

func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, ErrUnavailable
	}
	return &PostgresStore{pool: pool, lease: defaultLease, now: time.Now}, nil
}

func (store *PostgresStore) LoadRun(ctx context.Context, runID string) (Run, error) {
	if !store.valid() || !validUUID(runID) {
		return Run{}, ErrUnavailable
	}
	var run Run
	err := store.pool.QueryRow(ctx, `
SELECT id::text, org_id::text, generation, status
FROM public.daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&run.ID, &run.OrganizationID, &run.Generation, &run.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrInvalidState
	}
	if err != nil {
		return Run{}, ErrUnavailable
	}
	return run, nil
}

func (store *PostgresStore) ClaimDispatch(ctx context.Context, runID string) (*Run, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	var run Run
	err := store.pool.QueryRow(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'running', updated_at = $1
WHERE id = $2::uuid AND status IN ('pending', 'running')
RETURNING id::text, org_id::text, generation, status`, store.now().UTC(), runID).
		Scan(&run.ID, &run.OrganizationID, &run.Generation, &run.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	return &run, nil
}

func (store *PostgresStore) DispatchablePartitions(ctx context.Context, runID string) ([]Partition, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	rows, err := store.pool.Query(ctx, `
SELECT partition.id::text, partition.run_id::text FROM public.daily_metrics_partitions AS partition
JOIN public.daily_metrics_runs AS run ON run.id = partition.run_id
WHERE partition.run_id = $1::uuid AND partition.status IN ('pending', 'failed')
  AND run.status = 'running' ORDER BY partition.ordinal`, runID)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var result []Partition
	for rows.Next() {
		var partition Partition
		if err := rows.Scan(&partition.ID, &partition.RunID); err != nil {
			return nil, ErrUnavailable
		}
		result = append(result, partition)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return result, nil
}

func (store *PostgresStore) ClaimPartition(ctx context.Context, partitionID string) (*PartitionClaim, error) {
	if !store.valid() || !validUUID(partitionID) {
		return nil, ErrUnavailable
	}
	now, token := store.now().UTC(), uuid.New()
	var claim PartitionClaim
	err := store.pool.QueryRow(ctx, `
UPDATE public.daily_metrics_partitions
SET status = 'running', claim_token = $2, lease_expires_at = $3,
    attempt_count = attempt_count + 1, updated_at = $1
WHERE id = $4::uuid AND (status IN ('pending', 'failed') OR
      (status = 'running' AND lease_expires_at <= $1))
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_runs AS run
      WHERE run.id = daily_metrics_partitions.run_id AND run.status = 'running'
  )
RETURNING id::text, run_id::text, claim_token::text`,
		now, token, now.Add(store.lease), partitionID,
	).Scan(&claim.Partition.ID, &claim.Partition.RunID, &claim.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	return &claim, nil
}

func (store *PostgresStore) CompletePartition(ctx context.Context, claim PartitionClaim) error {
	return store.transitionPartition(ctx, claim, "succeeded")
}

func (store *PostgresStore) ReleasePartition(ctx context.Context, claim PartitionClaim) error {
	return store.transitionPartition(ctx, claim, "failed")
}

func (store *PostgresStore) transitionPartition(ctx context.Context, claim PartitionClaim, status string) error {
	if !store.valid() || !validUUID(claim.Partition.ID) || !validUUID(claim.Partition.RunID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_partitions
SET status = $1, claim_token = NULL, lease_expires_at = NULL,
    completed_at = CASE WHEN $1 = 'succeeded' THEN $2 ELSE completed_at END,
    updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running' AND claim_token = $5::uuid
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_runs AS run
      WHERE run.id = daily_metrics_partitions.run_id AND run.status = 'running'
  )`,
		status, store.now().UTC(), claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil || command.RowsAffected() != 1 {
		return ErrUnavailable
	}
	return nil
}

func (store *PostgresStore) ClaimFinalize(ctx context.Context, runID string) (*FinalizeClaim, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	now, token := store.now().UTC(), uuid.New()
	var claim FinalizeClaim
	err := store.pool.QueryRow(ctx, `
UPDATE public.daily_metrics_runs AS run
SET finalization_status = 'running', finalization_claim_token = $2,
    finalization_lease_expires_at = $3, updated_at = $1
WHERE run.id = $4::uuid AND run.status = 'running'
  AND NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )
  AND (run.finalization_status IN ('pending', 'failed') OR
      (run.finalization_status = 'running' AND run.finalization_lease_expires_at <= $1))
RETURNING run.id::text, run.org_id::text, run.generation, run.status, run.finalization_claim_token::text`,
		now, token, now.Add(store.lease), runID,
	).Scan(&claim.Run.ID, &claim.Run.OrganizationID, &claim.Run.Generation, &claim.Run.Status, &claim.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	return &claim, nil
}

func (store *PostgresStore) CompleteFinalize(ctx context.Context, claim FinalizeClaim) error {
	return store.transitionFinalize(ctx, claim, "succeeded")
}

func (store *PostgresStore) ReleaseFinalize(ctx context.Context, claim FinalizeClaim) error {
	return store.transitionFinalize(ctx, claim, "failed")
}

func (store *PostgresStore) transitionFinalize(ctx context.Context, claim FinalizeClaim, status string) error {
	if !store.valid() || !validUUID(claim.Run.ID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET finalization_status = $1, finalization_claim_token = NULL,
    finalization_lease_expires_at = NULL,
    finalized_at = CASE WHEN $1 = 'succeeded' THEN $2 ELSE finalized_at END,
    status = CASE WHEN $1 = 'succeeded' THEN 'succeeded' ELSE status END,
    updated_at = $2
WHERE id = $3::uuid AND finalization_status = 'running'
  AND finalization_claim_token = $4::uuid AND status = 'running'`, status, store.now().UTC(), claim.Run.ID, claim.Token)
	if err != nil || command.RowsAffected() != 1 {
		return ErrUnavailable
	}
	return nil
}

func (store *PostgresStore) valid() bool {
	return store != nil && store.pool != nil && store.now != nil && store.lease >= time.Second && store.lease <= time.Hour
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}
