package remaining

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidState = errors.New("remaining metrics durable state is invalid")
	ErrLeaseLost    = errors.New("remaining metrics execution lease was lost")
	ErrUnavailable  = errors.New("remaining metrics durable state is unavailable")
)

const defaultLease = 10 * time.Minute

type Run struct {
	ID             string
	OrganizationID string
	Family         string
	Generation     string
	Status         string
	Seed           *int64
}

type Partition struct {
	ID      string
	RunID   string
	Ordinal int
}

type Claim struct {
	Partition     Partition
	Token         string
	LeaseDuration time.Duration
}

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
SELECT id::text, org_id::text, family, generation, status, generation_seed
FROM public.remaining_metric_runs WHERE id = $1::uuid`, runID).Scan(
		&run.ID, &run.OrganizationID, &run.Family, &run.Generation, &run.Status, &run.Seed,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrInvalidState
	}
	if err != nil {
		return Run{}, ErrUnavailable
	}
	return run, nil
}

// PendingPartitions returns only incomplete work in ordinal order, so a
// backfill retry never restarts a completed partition.
func (store *PostgresStore) PendingPartitions(ctx context.Context, runID string) ([]Partition, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	rows, err := store.pool.Query(ctx, `
SELECT partition.id::text, partition.run_id::text, partition.ordinal
FROM public.remaining_metric_partitions AS partition
JOIN public.remaining_metric_runs AS run ON run.id = partition.run_id
WHERE partition.run_id = $1::uuid AND partition.status IN ('pending', 'failed')
  AND run.status IN ('pending', 'running')
ORDER BY partition.ordinal`, runID)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var result []Partition
	for rows.Next() {
		var partition Partition
		if err := rows.Scan(&partition.ID, &partition.RunID, &partition.Ordinal); err != nil {
			return nil, ErrUnavailable
		}
		result = append(result, partition)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return result, nil
}

func (store *PostgresStore) ClaimPartition(ctx context.Context, partitionID string) (*Claim, error) {
	if !store.valid() || !validUUID(partitionID) {
		return nil, ErrUnavailable
	}
	now, token := store.now().UTC(), uuid.New()
	var claim Claim
	err := store.pool.QueryRow(ctx, `
WITH active_run AS (
    UPDATE public.remaining_metric_runs
    SET status = 'running', updated_at = $1
    WHERE id = (
        SELECT run_id FROM public.remaining_metric_partitions WHERE id = $4::uuid
    ) AND status IN ('pending', 'running')
    RETURNING id
)
UPDATE public.remaining_metric_partitions
SET status = 'running', claim_token = $2, lease_expires_at = $3,
    attempt_count = attempt_count + 1, updated_at = $1
WHERE id = $4::uuid AND (
    status IN ('pending', 'failed') OR
    (status = 'running' AND lease_expires_at <= $1)
  )
  AND run_id IN (SELECT id FROM active_run)
RETURNING id::text, run_id::text, ordinal, claim_token::text`,
		now, token, now.Add(store.lease), partitionID,
	).Scan(&claim.Partition.ID, &claim.Partition.RunID, &claim.Partition.Ordinal, &claim.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	claim.LeaseDuration = store.lease
	return &claim, nil
}

func (store *PostgresStore) RenewPartition(ctx context.Context, claim Claim) error {
	if !store.validClaim(claim) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_partitions
SET lease_expires_at = $1, updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running'
  AND claim_token = $5::uuid AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = remaining_metric_partitions.run_id AND run.status = 'running'
  )`, now.Add(store.lease), now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) CompletePartition(ctx context.Context, claim Claim, outputEvidence string) error {
	if !store.validClaim(claim) || outputEvidence == "" || len(outputEvidence) > 4096 {
		return ErrInvalidState
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_partitions
SET status = 'succeeded', output_evidence = $1, completed_at = $2,
    claim_token = NULL, lease_expires_at = NULL, updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running'
  AND claim_token = $5::uuid AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = remaining_metric_partitions.run_id AND run.status = 'running'
  )`, outputEvidence, now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) ReleasePartition(ctx context.Context, claim Claim) error {
	if !store.validClaim(claim) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_partitions
SET status = 'failed', claim_token = NULL, lease_expires_at = NULL, updated_at = $1
WHERE id = $2::uuid AND run_id = $3::uuid AND status = 'running'
  AND claim_token = $4::uuid AND lease_expires_at > $1
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = remaining_metric_partitions.run_id AND run.status = 'running'
  )`, now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) CancelRun(ctx context.Context, runID string) error {
	if !store.valid() || !validUUID(runID) {
		return ErrUnavailable
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_runs
SET status = 'canceled', canceled_at = $1, updated_at = $1
WHERE id = $2::uuid AND status IN ('pending', 'running')`, store.now().UTC(), runID)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() > 1 {
		return ErrInvalidState
	}
	return nil
}

func (store *PostgresStore) FinalizeRun(ctx context.Context, runID string) error {
	if !store.valid() || !validUUID(runID) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_runs AS run
SET status = 'succeeded', updated_at = $1
WHERE run.id = $2::uuid AND run.status = 'running'
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )`, now, runID)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	run, loadErr := store.LoadRun(ctx, runID)
	if loadErr == nil && run.Status == "succeeded" {
		return nil
	}
	return ErrInvalidState
}

func (store *PostgresStore) validClaim(claim Claim) bool {
	return store.valid() && validUUID(claim.Partition.ID) && validUUID(claim.Partition.RunID) && validUUID(claim.Token)
}

func (store *PostgresStore) valid() bool {
	return store != nil && store.pool != nil && store.now != nil && store.lease >= time.Second && store.lease <= time.Hour
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}
