package daily

import (
	"context"
	"encoding/json"
	"errors"
	"os"
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

// defaultDailyRepositoryPartitionSize is the fallback per-partition repository
// cap when dailyRepositoryPartitionSizeEnvKey is unset or invalid.
const defaultDailyRepositoryPartitionSize = 3

// dailyRepositoryPartitionSizeEnvKey is a coordinated contract with
// CHAOS-4264 (bridge-runner OOM + ambiguity reaper): both PRs read this exact
// env key for the per-partition repository-count cap job_daily.py's Python
// bridge subprocess computes in one invocation, so they agree on the bound
// without one PR waiting on the other to land first (chris's ruling
// 2026-08-25). The default (3) is deliberately much smaller than the prior
// fixed 100: unlike the dora family's BackfillDays, where one bounded query
// covers many days inside a single job execution, each extra repository here
// adds real per-repository compute inside the SAME subprocess CHAOS-4264
// found being SIGKILLed.
const dailyRepositoryPartitionSizeEnvKey = "DEV_HEALTH_DAILY_PARTITION_MAX_REPOS"

var dailyRepositoryPartitionSize = loadDailyRepositoryPartitionSize()

// maxDailyMetricsRepositoriesPerRun bounds the total number of repositories
// one daily-metrics run will partition, for BOTH sources: an explicit
// StartRunRequest (normalizeStartRunRequest, unchanged since before this
// ticket) and live ClickHouse discovery (MaterializeScheduledFanout, CHAOS-
// 4263, codex adversarial review round 3). Before round 3, only the explicit
// path enforced this; deferred discovery accepted the entire unbounded
// ClickHouse result and chunked it into as many `daily_partition` jobs as
// dailyRepositoryPartitionSize allowed, with no upper bound on the total
// count. This PR's post-sync backfill (up to 15 daily_metrics_runs per sync
// event: day D plus up to maxPostSyncDailyBackfillDays) multiplies that
// per-sync-event exposure on the shared metrics queue for an unusually large
// tenant, so the cap now applies to the deferred path too, instead of
// silently accepting an unbounded partition-job burst.
const maxDailyMetricsRepositoriesPerRun = 1000

func loadDailyRepositoryPartitionSize() int {
	raw := strings.TrimSpace(os.Getenv(dailyRepositoryPartitionSizeEnvKey))
	if raw == "" {
		return defaultDailyRepositoryPartitionSize
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return defaultDailyRepositoryPartitionSize
	}
	return value
}

var dailyRunNamespace = uuid.MustParse("db1556db-28a7-58f6-982d-fc6f54dc7240")

const scheduledFanoutGenerationPrefix = "fixed-schedule:daily_metrics_fanout:"

// postSyncGenerationPrefix identifies a daily-metrics run created per
// completed sync (CHAOS-4263). Distinct from scheduledFanoutGenerationPrefix,
// whose exact value is a cross-language contract with Python's
// recommendations readiness gate (CHAOS-4066, see isScheduledFanoutGeneration)
// and must not be widened or reused for this.
const postSyncGenerationPrefix = "post-sync:"

// PostgresStore is the durable fence around the temporary compatibility
// compute adapter. Queue retries may repeat a request, but only a claimant
// with the current persisted token can make a partition/finalizer successful.
type PostgresStore struct {
	pool                       *pgxpool.Pool
	lease                      time.Duration
	now                        func() time.Time
	leaseObserver              jobruntime.DailyMetricsLeaseObserver
	discoveryObserver          jobruntime.DailyMetricsDiscoveryObserver
	redriveObserver            jobruntime.DailyMetricsRedriveObserver
	finalizeSweepObserver      jobruntime.DailyMetricsFinalizeSweepObserver
	finalizeRedriveObserver    jobruntime.DailyMetricsFinalizeRedriveObserver
	partitionRecomputeObserver jobruntime.DailyMetricsPartitionRecomputeObserver
}

// SetRedriveObserver wires the optional operator-redrive telemetry observer
// (CHAOS-4358). Telemetry must never gate durable state: a nil or never-set
// observer makes observeRedrive a silent no-op, matching discoveryObserver's
// discipline.
func (store *PostgresStore) SetRedriveObserver(observer jobruntime.DailyMetricsRedriveObserver) {
	if store == nil {
		return
	}
	store.redriveObserver = observer
}

func (store *PostgresStore) observeRedrive(reason string, count int) {
	if store.redriveObserver != nil && count > 0 {
		_ = store.redriveObserver.ObserveDailyMetricsRedrive(reason, count)
	}
}

// SetFinalizeSweepObserver wires the optional stranded-finalize sweep
// telemetry observer (CHAOS-4389). Telemetry must never gate durable state:
// a nil or never-set observer makes observeFinalizeSweep a silent no-op,
// matching redriveObserver's discipline.
func (store *PostgresStore) SetFinalizeSweepObserver(observer jobruntime.DailyMetricsFinalizeSweepObserver) {
	if store == nil {
		return
	}
	store.finalizeSweepObserver = observer
}

func (store *PostgresStore) observeFinalizeSweep(outcome string, count int) {
	if store.finalizeSweepObserver != nil && count > 0 {
		_ = store.finalizeSweepObserver.ObserveDailyMetricsFinalizeSweep(outcome, count)
	}
}

// SetFinalizeRedriveObserver wires the optional historical finalize-redrive
// telemetry observer (CHAOS-4405). Telemetry must never gate durable state:
// a nil or never-set observer makes observeFinalizeRedrive a silent no-op,
// matching finalizeSweepObserver's discipline.
func (store *PostgresStore) SetFinalizeRedriveObserver(observer jobruntime.DailyMetricsFinalizeRedriveObserver) {
	if store == nil {
		return
	}
	store.finalizeRedriveObserver = observer
}

func (store *PostgresStore) observeFinalizeRedrive(outcome string, count int) {
	if store.finalizeRedriveObserver != nil && count > 0 {
		_ = store.finalizeRedriveObserver.ObserveDailyMetricsFinalizeRedrive(outcome, count)
	}
}

// SetPartitionRecomputeObserver wires the optional CHAOS-4459 historical
// partition-recompute telemetry observer. Telemetry must never gate durable
// state: a nil or never-set observer makes observePartitionRecompute a
// silent no-op, matching finalizeRedriveObserver's discipline.
func (store *PostgresStore) SetPartitionRecomputeObserver(observer jobruntime.DailyMetricsPartitionRecomputeObserver) {
	if store == nil {
		return
	}
	store.partitionRecomputeObserver = observer
}

func (store *PostgresStore) observePartitionRecompute(family, outcome string, count int) {
	if store.partitionRecomputeObserver != nil && count > 0 {
		_ = store.partitionRecomputeObserver.ObserveDailyMetricsPartitionRecompute(family, outcome, count)
	}
}

// SetDiscoveryObserver wires the optional repository-discovery outcome
// observer (CHAOS-4263). Telemetry must never gate durable state: a nil or
// never-set observer makes observeDiscovery a silent no-op, matching
// leaseObserver's discipline.
func (store *PostgresStore) SetDiscoveryObserver(observer jobruntime.DailyMetricsDiscoveryObserver) {
	if store == nil {
		return
	}
	store.discoveryObserver = observer
}

func (store *PostgresStore) observeDiscovery(
	trigger jobruntime.DailyMetricsRunTrigger,
	outcome jobruntime.DailyMetricsDiscoveryOutcome,
) {
	if store.discoveryObserver != nil {
		_ = store.discoveryObserver.ObserveDailyMetricsDiscovery(trigger, outcome)
	}
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
		if len(partitions) == 0 {
			// Repository discovery was deferred on the request that first
			// created this run (CHAOS-4263): its partitions are not this
			// request's to verify. By the time a retry lands here, the heavy
			// worker may already have materialized a live ClickHouse snapshot,
			// and that snapshot is authoritative -- never this retry's own
			// (always empty) recomputation of it.
			if err := verifyDeferredDiscoveryRun(ctx, tx, run, request.TargetDay); err != nil {
				return Run{}, err
			}
		} else if err := verifyStartedRun(ctx, tx, run, request.TargetDay, partitions); err != nil {
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
		if err := verifyDeferredDiscoveryRun(ctx, tx, run, normalized.TargetDay); err != nil {
			return Run{}, err
		}
	}
	if err := publisher.PublishDispatchTx(ctx, tx, run, ""); err != nil {
		return Run{}, err
	}
	return run, nil
}

func normalizeStartRunRequest(request StartRunRequest) (StartRunRequest, [][]RepositoryID, error) {
	if !validUUID(request.OrganizationID) || request.Generation == "" ||
		len(request.Generation) > 64 || len(request.RepositoryIDs) > maxDailyMetricsRepositoriesPerRun ||
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
	// An empty RepositoryIDs request (the post-sync caller no longer resolves
	// any -- CHAOS-4263) leaves partitions empty rather than synthesizing one
	// zero-repository partition. A synthetic empty partition reports a
	// successful compute despite never touching a real repository; leaving
	// zero partitions instead makes ClaimDispatch mark this run
	// RepositoryDiscoveryRequired, so it resolves live ClickHouse repository
	// identity exactly like the scheduled fixed-schedule fan-out already does,
	// and MaterializeScheduledFanout's existing no_repositories handling
	// applies if that discovery genuinely finds nothing.
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

// isPostSyncGeneration reports whether a generation belongs to a post-sync
// daily-metrics run. Repository discovery for these is deferred to the same
// live ClickHouse RepositoryDiscoverer the nightly fixed schedule uses
// (CHAOS-4263), rather than the dead Postgres integration_sources ids the
// triggering sync itself carries in sync_run_units.
func isPostSyncGeneration(generation string) bool {
	return strings.HasPrefix(generation, postSyncGenerationPrefix) && len(generation) <= 64
}

func normalizeRepositoryPartitions(repositoryIDs []RepositoryID) ([][]RepositoryID, error) {
	seen := make(map[RepositoryID]struct{}, len(repositoryIDs))
	repositories := make([]RepositoryID, 0, len(repositoryIDs))
	for _, repositoryID := range repositoryIDs {
		if !validUUID(string(repositoryID)) {
			return nil, ErrInvalidState
		}
		canonical := RepositoryID(uuid.MustParse(string(repositoryID)).String())
		if _, duplicate := seen[canonical]; duplicate {
			continue
		}
		seen[canonical] = struct{}{}
		repositories = append(repositories, canonical)
	}
	sort.Slice(repositories, func(left, right int) bool { return repositories[left] < repositories[right] })
	return partitionRepositoryIDs(repositories), nil
}

func partitionRepositoryIDs(repositories []RepositoryID) [][]RepositoryID {
	partitions := make([][]RepositoryID, 0, (len(repositories)+dailyRepositoryPartitionSize-1)/dailyRepositoryPartitionSize)
	for len(repositories) > 0 {
		size := min(dailyRepositoryPartitionSize, len(repositories))
		partitions = append(partitions, append([]RepositoryID(nil), repositories[:size]...))
		repositories = repositories[size:]
	}
	return partitions
}

// verifyDeferredDiscoveryRun confirms an existing daily_metrics_runs row
// matches the caller's own identity for a run whose repository discovery is
// deferred (the nightly fixed schedule, or a post-sync re-drive per
// CHAOS-4263). It intentionally does not compare partitions: by the time a
// retry lands here the heavy worker may already have materialized a live
// ClickHouse snapshot, and that snapshot is authoritative, never a retry's own
// (always empty, for this class of request) recomputation of it. The
// generation format itself was already validated once, at whichever
// Start*RunTx call originally created this row.
func verifyDeferredDiscoveryRun(ctx context.Context, tx pgx.Tx, run Run, targetDay time.Time) error {
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
	return nil
}

func verifyStartedRun(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	targetDay time.Time,
	partitions [][]RepositoryID,
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
		var existing []RepositoryID
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
	var targetDay string
	err := store.pool.QueryRow(ctx, `
SELECT id::text, org_id::text, generation, status, target_day::text
FROM public.daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&run.ID, &run.OrganizationID, &run.Generation, &run.Status, &targetDay)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrInvalidState
	}
	if err != nil {
		return Run{}, ErrUnavailable
	}
	if run.TargetDay, err = time.Parse("2006-01-02", targetDay); err != nil {
		return Run{}, ErrInvalidState
	}
	return run, nil
}

func (store *PostgresStore) ClaimDispatch(ctx context.Context, runID string) (*Run, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	var run Run
	var targetDay string
	// RepositoryDiscoveryRequired is generation-agnostic: it is true exactly
	// when the run was created with no partitions yet, whichever entry point
	// created it (the nightly fixed schedule, or a post-sync re-drive per
	// CHAOS-4263). Both StartScheduledFanoutRunTx and StartRunTx (for an empty
	// RepositoryIDs request) leave partitions unmaterialized for this reason;
	// a request with explicit RepositoryIDs inserts its partitions in the same
	// transaction that creates the run, so it is never seen as pending here.
	err := store.pool.QueryRow(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'running', updated_at = $1
WHERE id = $2::uuid AND status IN ('pending', 'running')
RETURNING id::text, org_id::text, generation, status, target_day::text,
  NOT EXISTS (
    SELECT 1 FROM public.daily_metrics_partitions WHERE run_id = daily_metrics_runs.id
  )`, store.now().UTC(), runID).
		Scan(&run.ID, &run.OrganizationID, &run.Generation, &run.Status, &targetDay, &run.RepositoryDiscoveryRequired)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	if run.TargetDay, err = time.Parse("2006-01-02", targetDay); err != nil {
		return nil, ErrInvalidState
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
	repositoryIDs []RepositoryID,
) (bool, error) {
	if !store.valid() || !validUUID(run.ID) || !validUUID(run.OrganizationID) ||
		(!isScheduledFanoutGeneration(run.Generation) && !isPostSyncGeneration(run.Generation)) {
		return false, ErrInvalidState
	}
	trigger := jobruntime.DailyMetricsRunTriggerScheduledFanout
	if isPostSyncGeneration(run.Generation) {
		trigger = jobruntime.DailyMetricsRunTriggerPostSync
	}
	// Live ClickHouse discovery has no natural upper bound the way an
	// explicit StartRunRequest does -- fail loud rather than silently
	// chunking an unbounded repository set into an unbounded number of
	// daily_partition jobs (CHAOS-4263, codex adversarial review round 3).
	// The actual terminal-state write happens below, inside the same locked
	// transaction every other outcome uses (round 4 fix): an early return
	// here, before any write, left the durable run stuck in 'running'
	// forever -- no partitions, no completion fence, no terminal state,
	// worse than the burst it replaced, since a readiness gate that treats a
	// nonterminal run as unfinished would block that org's whole day
	// indefinitely with no re-drive path.
	overCap := len(repositoryIDs) > maxDailyMetricsRepositoriesPerRun
	var partitions [][]RepositoryID
	if !overCap {
		var err error
		partitions, err = normalizeRepositoryPartitions(repositoryIDs)
		if err != nil {
			return false, err
		}
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
	if overCap {
		command, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'failed', finalization_status = 'failed', finalized_at = $1, updated_at = $1
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
		// Marked even though this terminalized as a failure: everything
		// downstream in this same post-sync fanout chain (workgraph.build,
		// investment.materialize, membership_backfill, ...) is gated on this
		// exact completion key, and this org's discovered repository count
		// cannot change by waiting -- never marking it would strand the
		// ENTIRE chain, not just daily, on a condition that cannot resolve
		// on its own.
		if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
			return false, ErrUnavailable
		}
		if err := tx.Commit(ctx); err != nil {
			return false, ErrUnavailable
		}
		store.observeDiscovery(trigger, jobruntime.DailyMetricsDiscoveryOutcomeRepositoryCapExceeded)
		return false, ErrRepositoryCapExceeded
	}
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
	outcome := jobruntime.DailyMetricsDiscoveryOutcomeMaterialized
	if len(partitions) == 0 {
		outcome = jobruntime.DailyMetricsDiscoveryOutcomeNoRepositories
	}
	store.observeDiscovery(trigger, outcome)
	return true, nil
}

func (store *PostgresStore) DispatchablePartitions(ctx context.Context, runID string) ([]Partition, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	rows, err := store.pool.Query(ctx, `
SELECT partition.id::text, partition.run_id::text, partition.repo_ids::text FROM public.daily_metrics_partitions AS partition
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
		var repoIDs string
		if err := rows.Scan(&partition.ID, &partition.RunID, &repoIDs); err != nil {
			return nil, ErrUnavailable
		}
		if partition.RepoIDs, err = parsePartitionRepoIDs(repoIDs); err != nil {
			return nil, ErrInvalidState
		}
		result = append(result, partition)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return result, nil
}

// parsePartitionRepoIDs decodes daily_metrics_partitions.repo_ids's JSONB
// array (a plain []string of repos.id values, written by
// MaterializeScheduledFanout/normalizeStartRunRequest) into RepositoryID.
func parsePartitionRepoIDs(raw string) ([]RepositoryID, error) {
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, err
	}
	result := make([]RepositoryID, len(ids))
	for index, id := range ids {
		result[index] = RepositoryID(id)
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
	var repoIDs string
	// failure_reason = NULL (CHAOS-4316 codex review, P1): a partition
	// reclaimed here can arrive from status='failed' carrying a
	// failure_reason from a previous liveness kill (ReleasePartitionWithReason).
	// Migration 0113's ck_daily_metrics_partition_failure_reason_scope
	// constraint only permits a non-NULL reason when status is 'failed' or
	// 'failed_permanent' -- moving to 'running' while leaving a stale reason
	// in place violates that constraint and this UPDATE would fail outright,
	// permanently stranding the exact "stays retryable" partition this
	// ticket's whole design depends on. Clearing it here keeps the row
	// consistent with the constraint on every claim, reclaimed or fresh.
	err = tx.QueryRow(ctx, `
UPDATE public.daily_metrics_partitions
SET status = 'running', claim_token = $2, lease_expires_at = $3,
    attempt_count = attempt_count + 1, updated_at = $1,
    failure_reason = NULL
WHERE id = $4::uuid
RETURNING id::text, run_id::text, claim_token::text, repo_ids::text`,
		now, token, now.Add(store.lease), partitionID,
	).Scan(&claim.Partition.ID, &claim.Partition.RunID, &claim.Token, &repoIDs)
	if err != nil {
		return nil, ErrUnavailable
	}
	if claim.Partition.RepoIDs, err = parsePartitionRepoIDs(repoIDs); err != nil {
		return nil, ErrInvalidState
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
		// CHAOS-4459 (codex review, P1): a run RedrivePartitionsForRange reset
		// already consumed the ordinary "metrics.daily_finalize:"+run.ID
		// outbox key once, the first time this run finalized, before it was
		// ever reset. PublishFinalizeTx's insert would be a silent
		// ON-CONFLICT-DO-NOTHING no-op here, leaving the run stuck
		// status='running' forever -- the exact CHAOS-4389 stranding shape,
		// self-inflicted. Detect a recompute-reset run by its generation
		// prefix and publish under a fresh redrive-scoped key instead, via
		// an optional capability (a type assertion, not a Publisher
		// interface change) so every existing Publisher fake/implementer is
		// unaffected.
		if nonce, ok := recomputeNonce(run.Generation); ok {
			redrivePublisher, canRedrive := publisher.(redriveFinalizePublisher)
			if !canRedrive {
				return ErrUnavailable
			}
			if err := redrivePublisher.PublishRedriveFinalizeTx(ctx, tx, run, nonce); err != nil {
				return ErrUnavailable
			}
		} else if err := publisher.PublishFinalizeTx(ctx, tx, run); err != nil {
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
	err := store.transitionPartition(ctx, claim, "failed", "")
	if errors.Is(err, ErrLeaseLost) {
		store.observeLease(
			jobruntime.DailyMetricsLeaseStagePartition,
			jobruntime.DailyMetricsLeaseResultReleaseLost,
		)
	}
	return err
}

// ReleasePartitionWithReason is ReleasePartition plus a bounded
// failure_reason (CHAOS-4316): the partition stays 'failed' -- silently
// re-dispatchable by DispatchablePartitions, exactly like an ordinary
// release -- unlike FailPartitionPermanently's 'failed_permanent', because a
// liveness kill (the runner reported no progress, or the Go-side backstop's
// own ceiling fired) is not a claim this row can never satisfy: a fresh
// attempt might simply not hang. reason must be in
// dailyMetricsPartitionFailureReasons, the same closed vocabulary
// FailPartitionPermanently already validates against -- failure_reason is a
// shared column, not owned exclusively by either status.
func (store *PostgresStore) ReleasePartitionWithReason(ctx context.Context, claim PartitionClaim, reason string) error {
	if _, ok := dailyMetricsPartitionFailureReasons[reason]; !ok {
		return ErrInvalidState
	}
	err := store.transitionPartition(ctx, claim, "failed", reason)
	if errors.Is(err, ErrLeaseLost) {
		store.observeLease(
			jobruntime.DailyMetricsLeaseStagePartition,
			jobruntime.DailyMetricsLeaseResultReleaseLost,
		)
	}
	return err
}

// transitionPartition sets status and, when reason is non-empty, the shared
// failure_reason column (CHAOS-4316/CHAOS-4319) in the same statement --
// this path only ever fires from status='running' (the WHERE clause below),
// so failure_reason is always NULL going in; an empty reason explicitly
// clears it rather than leaving a stale value from a hypothetical future
// caller that reuses a row.
func (store *PostgresStore) transitionPartition(ctx context.Context, claim PartitionClaim, status string, reason string) error {
	if !store.valid() || !validUUID(claim.Partition.ID) || !validUUID(claim.Partition.RunID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	var reasonArg *string
	if reason != "" {
		reasonArg = &reason
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_partitions
SET status = $1::varchar, claim_token = NULL, lease_expires_at = NULL,
    failure_reason = $6::varchar,
    completed_at = CASE WHEN $1 = 'succeeded' THEN $2 ELSE completed_at END,
    updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running' AND claim_token = $5::uuid
  AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_runs AS run
      WHERE run.id = daily_metrics_partitions.run_id AND run.status = 'running'
  )`,
		status, store.now().UTC(), claim.Partition.ID, claim.Partition.RunID, claim.Token, reasonArg)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// dailyMetricsPartitionFailureReasons is the closed, bounded vocabulary
// FailPartitionPermanently and ReleasePartitionWithReason accept (CHAOS-4319,
// CHAOS-4316, CHAOS-4317) -- daily_metrics_partitions.failure_reason rows
// must stay safe for logs, dashboards, and telemetry labels, mirroring
// jobruntime.Reason's own compile-time-fixed catalog. An unrecognized reason
// is a caller bug, not a value worth persisting.
var dailyMetricsPartitionFailureReasons = map[string]struct{}{
	"ambiguous_refused":  {},
	"progress_stalled":   {},
	"capacity_exhausted": {},
	// CHAOS-4543: resource_exhausted (the runner's RSS watchdog, RLIMIT_AS
	// backstop, or a loader row-cap guard) and process_signaled (an external
	// kill) are both non-terminal, retryable classes -- ReleasePartitionWithReason
	// now persists them alongside the three reasons above instead of silently
	// dropping the reason on the generic releasePartition path.
	"resource_exhausted": {},
	"process_signaled":   {},
}

// FailPartitionPermanently durably terminalizes a partition whose
// compatibility-bridge ledger row is stuck ambiguous (CHAOS-4319): status
// moves to 'failed_permanent' with a bounded failure_reason, fenced by the
// same live-lease check every other partition transition uses. Unlike
// ReleasePartition's 'failed' (silently re-dispatchable by
// DispatchablePartitions), 'failed_permanent' is deliberately excluded from
// that reclaim set -- retrying a ledger row that can never move again
// without a human /repair call would only reproduce the same 409 forever.
func (store *PostgresStore) FailPartitionPermanently(ctx context.Context, claim PartitionClaim, reason string) error {
	if !store.valid() || !validUUID(claim.Partition.ID) || !validUUID(claim.Partition.RunID) || !validUUID(claim.Token) {
		return ErrUnavailable
	}
	if _, ok := dailyMetricsPartitionFailureReasons[reason]; !ok {
		return ErrInvalidState
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_partitions
SET status = 'failed_permanent', failure_reason = $1, claim_token = NULL, lease_expires_at = NULL,
    updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running' AND claim_token = $5::uuid
  AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_runs AS run
      WHERE run.id = daily_metrics_partitions.run_id AND run.status = 'running'
  )`,
		reason, store.now().UTC(), claim.Partition.ID, claim.Partition.RunID, claim.Token)
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
	// CHAOS-4405 (team-lead escalation on conditions (2)/(3)): close this
	// run's own OPEN finalize-redrive event, if any -- 'closed_succeeded'
	// is moot for FindStrandedFinalizeRuns's own exclusion (status='running'
	// above already took this run out of that scan's WHERE clause the
	// moment it committed), but keeps the audit trail accurate: an event
	// left permanently 'open' after its run visibly succeeded would read as
	// "still in flight" forever to anyone inspecting the table directly.
	// Unconditional and cheap: the overwhelming majority of CompleteFinalize
	// calls have no redrive event at all, and this UPDATE affects 0 rows
	// for every one of them.
	if _, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_finalize_redrive_events
SET status = 'closed_succeeded', closed_at = $1
WHERE run_id = $2::uuid AND status = 'open'`, now, claim.Run.ID); err != nil {
		return ErrUnavailable
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
SET finalization_status = $1::varchar, finalization_claim_token = NULL,
    finalization_lease_expires_at = NULL,
    finalized_at = CASE WHEN $1 = 'succeeded' THEN $2 ELSE finalized_at END,
    status = CASE WHEN $1 = 'succeeded' THEN 'succeeded' ELSE status END,
    updated_at = $2
WHERE id = $3::uuid AND finalization_status = 'running'
  AND finalization_claim_token = $4::uuid AND status = 'running'
  AND finalization_lease_expires_at > $2`, status, now, claim.Run.ID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	// CHAOS-4405 (team-lead escalation on conditions (2)/(3)): a redriven
	// finalize that fails must not leave the run running-with-failed-
	// finalize invisible to every recovery tool forever -- the original
	// (pre-escalation) design permanently excluded any run finalize-redrive
	// had ever touched from FindStrandedFinalizeRuns, which was exactly
	// that silent-failure shape. Closing this run's OPEN event as
	// 'closed_failed' the instant THIS SPECIFIC claim's failure commits
	// (same transaction, so it can never land without the other) makes the
	// run reappear in FindStrandedFinalizeRuns immediately -- an ordinary
	// CHAOS-4389 sweep or `daily-finalize --run` can recover it exactly
	// like any other stranded run. Never fires for status='succeeded' (this
	// function's only other possible caller shape, though today only
	// ReleaseFinalize calls it, always with "failed") -- a successful
	// transition already goes through CompleteFinalize's own close-out
	// above, never this one.
	var redriveEventsClosed int64
	if status != "succeeded" {
		closedCommand, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_finalize_redrive_events
SET status = 'closed_failed', closed_at = $1
WHERE run_id = $2::uuid AND status = 'open'`, now, claim.Run.ID)
		if err != nil {
			return ErrUnavailable
		}
		redriveEventsClosed = closedCommand.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	// Telemetry only after a successful commit: a redrive event actually
	// closed as failed is real, durable state this run's operator-facing
	// recovery story changed for -- see dailyMetricsFinalizeRedriveOutcomes'
	// own doc comment for why "touched nothing" and "never wired" must
	// still both read as a present, zero-valued series rather than being
	// conflated with each other.
	store.observeFinalizeRedrive("redriven_failed", int(redriveEventsClosed))
	return nil
}

func (store *PostgresStore) valid() bool {
	return store != nil && store.pool != nil && store.now != nil && store.lease >= time.Second && store.lease <= time.Hour
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}
