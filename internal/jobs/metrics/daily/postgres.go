package daily

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultLease = 10 * time.Minute

const dailyRepositoryPartitionSize = 100

var dailyRunNamespace = uuid.MustParse("db1556db-28a7-58f6-982d-fc6f54dc7240")

const scheduledFanoutGenerationPrefix = "fixed-schedule:daily_metrics_fanout:"

// PostgresStore is the durable fence around the temporary compatibility
// compute adapter. Queue retries may repeat a request, but only a claimant
// with the current persisted token can make a partition/finalizer successful.
type PostgresStore struct {
	pool          *pgxpool.Pool
	lease         time.Duration
	now           func() time.Time
	leaseObserver jobruntime.DailyMetricsLeaseObserver
}

func NewPostgresStore(
	pool *pgxpool.Pool,
	observers ...jobruntime.DailyMetricsLeaseObserver,
) (*PostgresStore, error) {
	if pool == nil {
		return nil, ErrUnavailable
	}
	var observer jobruntime.DailyMetricsLeaseObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &PostgresStore{pool: pool, lease: defaultLease, now: time.Now, leaseObserver: observer}, nil
}

// observeLease records a durably resolved lease encounter. Metric failures are
// dropped: telemetry must never decide whether a run can make progress.
func (store *PostgresStore) observeLease(
	stage jobruntime.DailyMetricsLeaseStage,
	result jobruntime.DailyMetricsLeaseResult,
) {
	if store.leaseObserver != nil {
		_ = store.leaseObserver.ObserveDailyMetricsLease(stage, result)
	}
}

// leaseDecision is the classification of a claim target read under a row lock.
// It exists so a claim can tell a live lease apart from a genuine no-op; the
// conditional UPDATE alone reports both as zero matched rows.
type leaseDecision int

const (
	// leaseIdle means the target is claimable now.
	leaseIdle leaseDecision = iota
	// leaseReclaimable means an expired lease is being taken over.
	leaseReclaimable
	// leaseHeld means a live lease belongs to someone else.
	leaseHeld
	// leaseSettled means there is genuinely nothing to claim.
	leaseSettled
)

// classifyLease resolves a target's lease state against the current clock.
func classifyLease(claimable bool, status string, leaseExpiresAt *time.Time, now time.Time) leaseDecision {
	if status == "running" && leaseExpiresAt != nil {
		if leaseExpiresAt.After(now) {
			return leaseHeld
		}
		return leaseReclaimable
	}
	if claimable {
		return leaseIdle
	}
	return leaseSettled
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
	run := newRun(request.OrganizationID, request.TargetDay, request.Generation)
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

// StartScheduledFanoutRunTx creates the durable state for one organization in
// the nightly fixed schedule. Repository discovery intentionally happens in
// the heavy worker after this coordinator transaction commits.
func (store *PostgresStore) StartScheduledFanoutRunTx(
	ctx context.Context,
	tx pgx.Tx,
	request ScheduledFanoutRequest,
	publisher RunPublisher,
) (Run, error) {
	if !store.valid() || tx == nil || publisher == nil {
		return Run{}, ErrUnavailable
	}
	normalized, err := normalizeScheduledFanoutRequest(request)
	if err != nil {
		return Run{}, err
	}
	run := newRun(normalized.OrganizationID, normalized.TargetDay, normalized.Generation)
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::date, $4, 'pending', 'pending', $5, $5)
ON CONFLICT DO NOTHING`,
		run.ID, run.OrganizationID, normalized.TargetDay.Format("2006-01-02"), run.Generation, now)
	if err != nil {
		return Run{}, ErrUnavailable
	}
	if command.RowsAffected() == 0 {
		if err := verifyScheduledFanoutRun(ctx, tx, run, normalized.TargetDay); err != nil {
			return Run{}, err
		}
	}
	if err := publisher.PublishDispatchTx(ctx, tx, run, ""); err != nil {
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
	partitions, err := normalizeRepositoryPartitions(request.RepositoryIDs)
	if err != nil {
		return StartRunRequest{}, nil, err
	}
	request.RepositoryIDs = nil
	for _, partition := range partitions {
		request.RepositoryIDs = append(request.RepositoryIDs, partition...)
	}
	if len(partitions) == 0 {
		partitions = append(partitions, []string{})
	}
	return request, partitions, nil
}

func normalizeScheduledFanoutRequest(request ScheduledFanoutRequest) (ScheduledFanoutRequest, error) {
	if !validUUID(request.OrganizationID) || !isScheduledFanoutGeneration(request.Generation) {
		return ScheduledFanoutRequest{}, ErrInvalidState
	}
	request.OrganizationID = uuid.MustParse(request.OrganizationID).String()
	request.TargetDay = request.TargetDay.UTC()
	if request.TargetDay.IsZero() {
		return ScheduledFanoutRequest{}, ErrInvalidState
	}
	return request, nil
}

func newRun(organizationID string, targetDay time.Time, generation string) Run {
	return Run{
		ID: uuid.NewSHA1(
			dailyRunNamespace,
			[]byte(organizationID+"|"+targetDay.Format("2006-01-02")+"|"+generation),
		).String(),
		OrganizationID: organizationID,
		Generation:     generation,
		Status:         "pending",
	}
}

func isScheduledFanoutGeneration(generation string) bool {
	return strings.HasPrefix(generation, scheduledFanoutGenerationPrefix) && len(generation) <= 64
}

func normalizeRepositoryPartitions(repositoryIDs []string) ([][]string, error) {
	seen := make(map[string]struct{}, len(repositoryIDs))
	repositories := make([]string, 0, len(repositoryIDs))
	for _, repositoryID := range repositoryIDs {
		if !validUUID(repositoryID) {
			return nil, ErrInvalidState
		}
		canonical := uuid.MustParse(repositoryID).String()
		if _, duplicate := seen[canonical]; duplicate {
			continue
		}
		seen[canonical] = struct{}{}
		repositories = append(repositories, canonical)
	}
	sort.Strings(repositories)
	return partitionRepositoryIDs(repositories), nil
}

func partitionRepositoryIDs(repositories []string) [][]string {
	partitions := make([][]string, 0, (len(repositories)+dailyRepositoryPartitionSize-1)/dailyRepositoryPartitionSize)
	for len(repositories) > 0 {
		size := min(dailyRepositoryPartitionSize, len(repositories))
		partitions = append(partitions, append([]string(nil), repositories[:size]...))
		repositories = repositories[size:]
	}
	return partitions
}

func verifyScheduledFanoutRun(ctx context.Context, tx pgx.Tx, run Run, targetDay time.Time) error {
	var organizationID, generation, day string
	if err := tx.QueryRow(ctx, `
SELECT org_id::text, generation, target_day::text
FROM public.daily_metrics_runs WHERE id = $1::uuid`, run.ID).
		Scan(&organizationID, &generation, &day); err != nil {
		return ErrUnavailable
	}
	if organizationID != run.OrganizationID || generation != run.Generation ||
		day != targetDay.Format("2006-01-02") || !isScheduledFanoutGeneration(generation) {
		return ErrInvalidState
	}
	return nil
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
RETURNING id::text, org_id::text, generation, status,
  generation LIKE '`+scheduledFanoutGenerationPrefix+`%' AND NOT EXISTS (
    SELECT 1 FROM public.daily_metrics_partitions WHERE run_id = daily_metrics_runs.id
  )`, store.now().UTC(), runID).
		Scan(&run.ID, &run.OrganizationID, &run.Generation, &run.Status, &run.RepositoryDiscoveryRequired)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	return &run, nil
}

// MaterializeScheduledFanout persists the repository snapshot selected by the
// heavy worker before it publishes any partition work. On retry it preserves an
// already materialized partition set rather than replacing it with a newer
// ClickHouse snapshot.
//
// An empty snapshot terminalizes the run as no_repositories with its completion
// fence and does not create a synthetic empty partition. Such a partition would
// report a successful compute while doing no work.
func (store *PostgresStore) MaterializeScheduledFanout(
	ctx context.Context,
	run Run,
	repositoryIDs []string,
) (bool, error) {
	if !store.valid() || !validUUID(run.ID) || !validUUID(run.OrganizationID) ||
		!isScheduledFanoutGeneration(run.Generation) {
		return false, ErrInvalidState
	}
	partitions, err := normalizeRepositoryPartitions(repositoryIDs)
	if err != nil {
		return false, err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	var organizationID, generation, status string
	if err := tx.QueryRow(ctx, `
SELECT org_id::text, generation, status
FROM public.daily_metrics_runs
WHERE id = $1::uuid
FOR UPDATE`, run.ID).Scan(&organizationID, &generation, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrInvalidState
		}
		return false, ErrUnavailable
	}
	if organizationID != run.OrganizationID || generation != run.Generation || status != "running" {
		return false, ErrInvalidState
	}
	var existing int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.daily_metrics_partitions WHERE run_id = $1::uuid`, run.ID).Scan(&existing); err != nil {
		return false, ErrUnavailable
	}
	if existing != 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, ErrUnavailable
		}
		return false, nil
	}
	now := store.now().UTC()
	if len(partitions) == 0 {
		command, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'no_repositories', finalization_status = 'succeeded', finalized_at = $1, updated_at = $1
WHERE id = $2::uuid AND status = 'running'`, now, run.ID)
		if err != nil {
			return false, ErrUnavailable
		}
		if command.RowsAffected() != 1 {
			return false, ErrInvalidState
		}
		completionKey, err := joboutbox.CompletionKey("daily_metrics_run", run.ID)
		if err != nil {
			return false, ErrInvalidState
		}
		if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
			return false, ErrUnavailable
		}
	} else {
		for ordinal, ids := range partitions {
			raw, err := json.Marshal(ids)
			if err != nil {
				return false, ErrInvalidState
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO public.daily_metrics_partitions
    (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending', 0, $5, $5)`,
				dailyPartitionID(run.ID, ordinal), run.ID, ordinal, raw, now); err != nil {
				return false, ErrUnavailable
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, ErrUnavailable
	}
	return true, nil
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

// ClaimPartition fences one partition for execution. It reads the row under a
// lock first so it can distinguish a live lease from a settled partition: a
// live lease is reported as a LeaseActiveError so the caller parks and returns
// after it expires, because returning "nothing to do" here retires the only
// worker that could ever reclaim it.
func (store *PostgresStore) ClaimPartition(ctx context.Context, partitionID string) (*PartitionClaim, error) {
	if !store.valid() || !validUUID(partitionID) {
		return nil, ErrUnavailable
	}
	now, token := store.now().UTC(), uuid.New()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	var status, runStatus string
	var leaseExpiresAt *time.Time
	err = tx.QueryRow(ctx, `
SELECT partition.status, partition.lease_expires_at, run.status
FROM public.daily_metrics_partitions AS partition
JOIN public.daily_metrics_runs AS run ON run.id = partition.run_id
WHERE partition.id = $1::uuid
FOR UPDATE OF partition`, partitionID).Scan(&status, &leaseExpiresAt, &runStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	// A run that is no longer running owns the decision; the partition must not
	// be revived under it, and there is nothing for this job to come back for.
	if runStatus != "running" {
		return nil, nil
	}
	decision := classifyLease(status == "pending" || status == "failed", status, leaseExpiresAt, now)
	switch decision {
	case leaseHeld:
		store.observeLease(jobruntime.DailyMetricsLeaseStagePartition, jobruntime.DailyMetricsLeaseResultSnoozed)
		return nil, &LeaseActiveError{RetryAfter: leaseExpiresAt.Sub(now)}
	case leaseSettled:
		return nil, nil
	}
	var claim PartitionClaim
	err = tx.QueryRow(ctx, `
UPDATE public.daily_metrics_partitions
SET status = 'running', claim_token = $2, lease_expires_at = $3,
    attempt_count = attempt_count + 1, updated_at = $1
WHERE id = $4::uuid
RETURNING id::text, run_id::text, claim_token::text`,
		now, token, now.Add(store.lease), partitionID,
	).Scan(&claim.Partition.ID, &claim.Partition.RunID, &claim.Token)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrUnavailable
	}
	if decision == leaseReclaimable {
		store.observeLease(jobruntime.DailyMetricsLeaseStagePartition, jobruntime.DailyMetricsLeaseResultReclaimed)
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

// ReleasePartition stands a claimed partition back down. Release is fenced on a
// live lease, so a claimant that has already outlived its lease cannot release
// it; that outcome is recorded rather than left for the caller to discard.
func (store *PostgresStore) ReleasePartition(ctx context.Context, claim PartitionClaim) error {
	err := store.transitionPartition(ctx, claim, "failed")
	if errors.Is(err, ErrLeaseLost) {
		store.observeLease(
			jobruntime.DailyMetricsLeaseStagePartition,
			jobruntime.DailyMetricsLeaseResultReleaseLost,
		)
	}
	return err
}

func (store *PostgresStore) transitionPartition(ctx context.Context, claim PartitionClaim, status string) error {
	if !store.valid() || !validUUID(claim.Partition.ID) || !validUUID(claim.Partition.RunID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_partitions
SET status = $1::varchar, claim_token = NULL, lease_expires_at = NULL,
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

// ClaimFinalize fences a run's finalization. Like ClaimPartition it reads the
// row under a lock first, so that a finalize lease orphaned by a dead claimant
// is reported as a LeaseActiveError instead of as "nothing to do". This is the
// claim that matters most: CompleteFinalize is the only writer of the run's
// completion fence, so a finalize that quietly retires strands every handoff
// gated on this run forever.
func (store *PostgresStore) ClaimFinalize(ctx context.Context, runID string) (*FinalizeClaim, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	now, token := store.now().UTC(), uuid.New()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	var status, finalizationStatus string
	var leaseExpiresAt *time.Time
	var partitionsReady bool
	err = tx.QueryRow(ctx, `
SELECT run.status, run.finalization_status, run.finalization_lease_expires_at,
  NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )
FROM public.daily_metrics_runs AS run
WHERE run.id = $1::uuid
FOR UPDATE OF run`, runID).Scan(&status, &finalizationStatus, &leaseExpiresAt, &partitionsReady)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	if status != "running" {
		return nil, nil
	}
	claimable := partitionsReady && (finalizationStatus == "pending" || finalizationStatus == "failed")
	decision := classifyLease(claimable, finalizationStatus, leaseExpiresAt, now)
	switch decision {
	case leaseHeld:
		store.observeLease(jobruntime.DailyMetricsLeaseStageFinalize, jobruntime.DailyMetricsLeaseResultSnoozed)
		return nil, &LeaseActiveError{RetryAfter: leaseExpiresAt.Sub(now)}
	case leaseSettled:
		return nil, nil
	case leaseReclaimable:
		// An expired finalize lease is only reclaimable once its partitions are
		// all succeeded; otherwise the partition layer still owns the run.
		if !partitionsReady {
			return nil, nil
		}
	}
	var claim FinalizeClaim
	err = tx.QueryRow(ctx, `
UPDATE public.daily_metrics_runs AS run
SET finalization_status = 'running', finalization_claim_token = $2,
    finalization_lease_expires_at = $3, updated_at = $1
WHERE run.id = $4::uuid
RETURNING run.id::text, run.org_id::text, run.generation, run.status, run.finalization_claim_token::text`,
		now, token, now.Add(store.lease), runID,
	).Scan(&claim.Run.ID, &claim.Run.OrganizationID, &claim.Run.Generation, &claim.Run.Status, &claim.Token)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrUnavailable
	}
	if decision == leaseReclaimable {
		store.observeLease(jobruntime.DailyMetricsLeaseStageFinalize, jobruntime.DailyMetricsLeaseResultReclaimed)
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
	err := store.transitionFinalize(ctx, claim, "failed")
	if errors.Is(err, ErrLeaseLost) {
		store.observeLease(
			jobruntime.DailyMetricsLeaseStageFinalize,
			jobruntime.DailyMetricsLeaseResultReleaseLost,
		)
	}
	return err
}

func (store *PostgresStore) transitionFinalize(ctx context.Context, claim FinalizeClaim, status string) error {
	if !store.valid() || !validUUID(claim.Run.ID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET finalization_status = $1::varchar, finalization_claim_token = NULL,
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
