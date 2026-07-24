package daily

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultLease = 10 * time.Minute

const dailyRepositoryPartitionSize = 100

var dailyRunNamespace = uuid.MustParse("db1556db-28a7-58f6-982d-fc6f54dc7240")

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

// StartRunTx atomically creates or verifies a deterministic daily run, its
// ordered repository partitions, and the deferred/executable dispatch
// handoff. It never commits the caller's transaction.
func (store *PostgresStore) StartRunTx(
	ctx context.Context,
	tx pgx.Tx,
	request StartRunRequest,
	publisher RunPublisher,
) (Run, error) {
	if !store.valid() || tx == nil || publisher == nil {
		return Run{}, ErrUnavailable
	}
	request, partitions, err := normalizeStartRunRequest(request)
	if err != nil {
		return Run{}, err
	}
	run := Run{
		ID: uuid.NewSHA1(
			dailyRunNamespace,
			[]byte(request.OrganizationID+"|"+request.TargetDay.Format("2006-01-02")+"|"+request.Generation),
		).String(),
		OrganizationID: request.OrganizationID,
		Generation:     request.Generation,
		Status:         "pending",
	}
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::date, $4, 'pending', 'pending', $5, $5)
ON CONFLICT DO NOTHING`,
		run.ID, run.OrganizationID, request.TargetDay.Format("2006-01-02"), run.Generation, now)
	if err != nil {
		return Run{}, ErrUnavailable
	}
	if command.RowsAffected() == 0 {
		if err := verifyStartedRun(ctx, tx, run, request.TargetDay, partitions); err != nil {
			return Run{}, err
		}
		var status string
		if err := tx.QueryRow(ctx, `
SELECT status FROM public.daily_metrics_runs WHERE id = $1::uuid`, run.ID).Scan(&status); err != nil {
			return Run{}, ErrUnavailable
		}
		if status == "succeeded" {
			completionKey, keyErr := joboutbox.CompletionKey("daily_metrics_run", run.ID)
			if keyErr != nil {
				return Run{}, ErrInvalidState
			}
			if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
				return Run{}, ErrUnavailable
			}
		}
	} else {
		for ordinal, repositoryIDs := range partitions {
			partitionID := dailyPartitionID(run.ID, ordinal)
			raw, marshalErr := json.Marshal(repositoryIDs)
			if marshalErr != nil {
				return Run{}, ErrInvalidState
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO public.daily_metrics_partitions
    (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending', 0, $5, $5)`,
				partitionID, run.ID, ordinal, raw, now); err != nil {
				return Run{}, ErrUnavailable
			}
		}
	}
	if err := publisher.PublishDispatchTx(ctx, tx, run, request.PrerequisiteCompletionKey); err != nil {
		return Run{}, err
	}
	return run, nil
}

func normalizeStartRunRequest(request StartRunRequest) (StartRunRequest, [][]string, error) {
	if !validUUID(request.OrganizationID) || request.Generation == "" ||
		len(request.Generation) > 64 || len(request.RepositoryIDs) > 1000 ||
		len(request.PrerequisiteCompletionKey) > 256 {
		return StartRunRequest{}, nil, ErrInvalidState
	}
	request.OrganizationID = uuid.MustParse(request.OrganizationID).String()
	request.TargetDay = request.TargetDay.UTC()
	if request.TargetDay.IsZero() {
		return StartRunRequest{}, nil, ErrInvalidState
	}
	seen := make(map[string]struct{}, len(request.RepositoryIDs))
	repositories := make([]string, 0, len(request.RepositoryIDs))
	for _, repositoryID := range request.RepositoryIDs {
		if !validUUID(repositoryID) {
			return StartRunRequest{}, nil, ErrInvalidState
		}
		canonical := uuid.MustParse(repositoryID).String()
		if _, duplicate := seen[canonical]; duplicate {
			continue
		}
		seen[canonical] = struct{}{}
		repositories = append(repositories, canonical)
	}
	sort.Strings(repositories)
	request.RepositoryIDs = repositories
	partitions := make([][]string, 0, max(1, (len(repositories)+dailyRepositoryPartitionSize-1)/dailyRepositoryPartitionSize))
	for len(repositories) > 0 {
		size := min(dailyRepositoryPartitionSize, len(repositories))
		partitions = append(partitions, append([]string(nil), repositories[:size]...))
		repositories = repositories[size:]
	}
	if len(partitions) == 0 {
		partitions = append(partitions, []string{})
	}
	return request, partitions, nil
}

func verifyStartedRun(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	targetDay time.Time,
	partitions [][]string,
) error {
	var organizationID, generation, day string
	if err := tx.QueryRow(ctx, `
SELECT org_id::text, generation, target_day::text
FROM public.daily_metrics_runs WHERE id = $1::uuid`, run.ID).
		Scan(&organizationID, &generation, &day); err != nil {
		return ErrUnavailable
	}
	if organizationID != run.OrganizationID || generation != run.Generation ||
		day != targetDay.Format("2006-01-02") {
		return ErrInvalidState
	}
	rows, err := tx.Query(ctx, `
SELECT ordinal, repo_ids::text
FROM public.daily_metrics_partitions
WHERE run_id = $1::uuid ORDER BY ordinal`, run.ID)
	if err != nil {
		return ErrUnavailable
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var ordinal int
		var raw string
		if err := rows.Scan(&ordinal, &raw); err != nil {
			return ErrUnavailable
		}
		if index >= len(partitions) || ordinal != index {
			return ErrInvalidState
		}
		var existing []string
		if json.Unmarshal([]byte(raw), &existing) != nil ||
			len(existing) != len(partitions[index]) {
			return ErrInvalidState
		}
		for repositoryIndex := range existing {
			if existing[repositoryIndex] != partitions[index][repositoryIndex] {
				return ErrInvalidState
			}
		}
		index++
	}
	if rows.Err() != nil {
		return ErrUnavailable
	}
	if index != len(partitions) {
		return ErrInvalidState
	}
	return nil
}

func dailyPartitionID(runID string, ordinal int) string {
	return uuid.NewSHA1(uuid.MustParse(runID), []byte("partition:"+strconv.Itoa(ordinal))).String()
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
	claim.LeaseDuration = store.lease
	return &claim, nil
}

func (store *PostgresStore) RenewPartition(ctx context.Context, claim PartitionClaim) error {
	if !store.valid() || !validUUID(claim.Partition.ID) || !validUUID(claim.Partition.RunID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_partitions
SET lease_expires_at = $1, updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running' AND claim_token = $5::uuid
  AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_runs AS run
      WHERE run.id = daily_metrics_partitions.run_id AND run.status = 'running'
  )`, now.Add(store.lease), now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) CompletePartition(
	ctx context.Context,
	claim PartitionClaim,
	publisher Publisher,
) error {
	if !store.valid() || publisher == nil || !validUUID(claim.Partition.ID) ||
		!validUUID(claim.Partition.RunID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	var run Run
	err = tx.QueryRow(ctx, `
SELECT id::text, org_id::text, generation, status
FROM public.daily_metrics_runs
WHERE id = $1::uuid
FOR UPDATE`, claim.Partition.RunID).
		Scan(&run.ID, &run.OrganizationID, &run.Generation, &run.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return ErrUnavailable
	}
	if run.Status != "running" {
		return ErrLeaseLost
	}
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_partitions
SET status = 'succeeded', claim_token = NULL, lease_expires_at = NULL,
    completed_at = $1, updated_at = $1
WHERE id = $2::uuid AND run_id = $3::uuid AND status = 'running'
  AND claim_token = $4::uuid AND lease_expires_at > $1`,
		now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	var incomplete int
	if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM public.daily_metrics_partitions
WHERE run_id = $1::uuid AND status <> 'succeeded'`, run.ID).Scan(&incomplete); err != nil {
		return ErrUnavailable
	}
	if incomplete == 0 {
		if err := publisher.PublishFinalizeTx(ctx, tx, run); err != nil {
			return ErrUnavailable
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
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
  AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_runs AS run
      WHERE run.id = daily_metrics_partitions.run_id AND run.status = 'running'
  )`,
		status, store.now().UTC(), claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
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
	claim.LeaseDuration = store.lease
	return &claim, nil
}

func (store *PostgresStore) RenewFinalize(ctx context.Context, claim FinalizeClaim) error {
	if !store.valid() || !validUUID(claim.Run.ID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET finalization_lease_expires_at = $1, updated_at = $2
WHERE id = $3::uuid AND finalization_status = 'running'
  AND finalization_claim_token = $4::uuid AND status = 'running'
  AND finalization_lease_expires_at > $2`,
		now.Add(store.lease), now, claim.Run.ID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) CompleteFinalize(ctx context.Context, claim FinalizeClaim) error {
	if !store.valid() || !validUUID(claim.Run.ID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	completionKey, err := joboutbox.CompletionKey("daily_metrics_run", claim.Run.ID)
	if err != nil {
		return ErrInvalidState
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET finalization_status = 'succeeded', finalization_claim_token = NULL,
    finalization_lease_expires_at = NULL, finalized_at = $1,
    status = 'succeeded', updated_at = $1
WHERE id = $2::uuid AND finalization_status = 'running'
  AND finalization_claim_token = $3::uuid AND status = 'running'
  AND finalization_lease_expires_at > $1`, now, claim.Run.ID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
		return ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
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
  AND finalization_claim_token = $4::uuid AND status = 'running'
  AND finalization_lease_expires_at > $2`, status, store.now().UTC(), claim.Run.ID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
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
