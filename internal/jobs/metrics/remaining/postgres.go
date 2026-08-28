package remaining

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidState = errors.New("remaining metrics durable state is invalid")
	ErrLeaseLost    = errors.New("remaining metrics execution lease was lost")
	ErrLeaseActive  = errors.New("remaining metrics execution lease is still active")
	ErrUnavailable  = errors.New("remaining metrics durable state is unavailable")
)

// LeaseActiveError reports that the partition is held by a lease that has not
// expired yet, and carries how long is left on it.
//
// The claim reaches "held by a live lease" and "nothing left to do" by the same
// route -- a conditional UPDATE matching no row -- but they are not the same
// answer. Reporting a live lease as nothing-to-do retires the job, and that job
// is the only thing that would have returned to reclaim the lease once it
// expired; the retry budget is spent in tens of seconds against a ten-minute
// lease. Since this run's completion is the fence key for the handoffs gated on
// it, a partition abandoned that way strands them all (CHAOS-3991).
type LeaseActiveError struct {
	RetryAfter time.Duration
}

func (err *LeaseActiveError) Error() string { return ErrLeaseActive.Error() }
func (err *LeaseActiveError) Unwrap() error { return ErrLeaseActive }

const defaultLease = 10 * time.Minute

const (
	maxGenerationLength = 128
	maxScopeKeyLength   = 512
	maxScopesPerRun     = 1024
)

// StartRunRequest is the immutable, persisted input for a remaining-metrics
// generation. Scopes are ordered deliberately: their ordinal is the durable
// work identity, not an implementation detail of a dispatcher.
type StartRunRequest struct {
	OrganizationID            string
	Family                    string
	Generation                string
	ScopeKey                  string
	GenerationSeed            *int64
	Scopes                    []json.RawMessage
	PrerequisiteCompletionKey string
}

type Run struct {
	ID             string
	OrganizationID string
	Family         string
	Generation     string
	ScopeKey       string
	Status         string
	Seed           *int64
}

type Partition struct {
	ID      string
	RunID   string
	Ordinal int
	Scope   json.RawMessage
}

type Claim struct {
	Partition     Partition
	Token         string
	LeaseDuration time.Duration
}

type PostgresStore struct {
	pool                   *pgxpool.Pool
	lease                  time.Duration
	now                    func() time.Time
	leaseObserver          jobruntime.RemainingMetricsLeaseObserver
	openDayZeroRowObserver OpenDayZeroRowObserver
}

type PartitionPublisher interface {
	PublishPartitionTx(context.Context, pgx.Tx, Run, Partition, string) error
}

// OpenDayZeroRowObserver reports a remaining-metrics dora partition that
// completed with rows_written=0 while its day was still open (CHAOS-4384):
// the exact prod shape (org c6a38355, 08-26/27/28) where the first post-sync
// of a UTC day computes that day before any deployments/incidents exist,
// succeeds with nothing to write, and every later same-day trigger would
// have reused that empty coverage forever without this fix. An unmoving
// dora_metrics_daily is otherwise indistinguishable from a genuinely quiet
// day, so this is the counter an alert binds to.
type OpenDayZeroRowObserver interface {
	ObserveRemainingMetricsOpenDayZeroRow(family string) error
}

// SetOpenDayZeroRowObserver wires the optional CHAOS-4384 signal. A nil
// observer (the default) means telemetry never gates the dedup decision --
// observeOpenDayZeroRow is always a safe no-op.
func (store *PostgresStore) SetOpenDayZeroRowObserver(observer OpenDayZeroRowObserver) {
	store.openDayZeroRowObserver = observer
}

func (store *PostgresStore) observeOpenDayZeroRow(family string) {
	if store.openDayZeroRowObserver != nil {
		_ = store.openDayZeroRowObserver.ObserveRemainingMetricsOpenDayZeroRow(family)
	}
}

// observeReleaseLost records a durably resolved release-lost outcome. Metric
// failures are dropped: telemetry must never decide whether a run can make
// progress.
func (store *PostgresStore) observeReleaseLost() {
	if store.leaseObserver != nil {
		_ = store.leaseObserver.ObserveRemainingMetricsLeaseReleaseLost()
	}
}

func NewPostgresStore(
	pool *pgxpool.Pool,
	observers ...jobruntime.RemainingMetricsLeaseObserver,
) (*PostgresStore, error) {
	if pool == nil {
		return nil, ErrUnavailable
	}
	var observer jobruntime.RemainingMetricsLeaseObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &PostgresStore{pool: pool, lease: defaultLease, now: time.Now, leaseObserver: observer}, nil
}

// StartRun atomically persists a deterministic generation and every partition
// it owns. Retried queue deliveries are only accepted when their immutable
// seed, count, and ordered scopes exactly match the original request.
func (store *PostgresStore) StartRun(ctx context.Context, request StartRunRequest) (Run, error) {
	if !store.valid() {
		return Run{}, ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return Run{}, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	run, err := store.StartRunTx(ctx, tx, request, nil)
	if err != nil {
		return Run{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Run{}, ErrUnavailable
	}
	return run, nil
}

// StartRunTx creates or verifies a deterministic run, every ordered
// partition, and each optional outbox handoff inside the caller's transaction.
// This is the post-sync fanout seam: the source transition cannot commit while
// any remaining-metrics domain row or deferred/executable handoff is missing.
// It never commits. Run identity is derived from organization, family,
// generation, and scope key; partition identity is derived from that run ID
// and the one-based scope ordinal. The returned Run carries the canonical ID.
func (store *PostgresStore) StartRunTx(
	ctx context.Context,
	tx pgx.Tx,
	request StartRunRequest,
	publisher PartitionPublisher,
) (Run, error) {
	if !store.valid() || tx == nil {
		return Run{}, ErrUnavailable
	}
	request.Scopes = cloneScopes(request.Scopes)
	if err := validateStartRunRequest(request); err != nil {
		return Run{}, ErrInvalidState
	}
	request.OrganizationID = uuid.MustParse(request.OrganizationID).String()
	for ordinal := range request.Scopes {
		canonical, err := validateFamilyScope(request.Family, request.Scopes[ordinal])
		if err != nil {
			return Run{}, ErrInvalidState
		}
		request.Scopes[ordinal], err = canonicalJSON(canonical)
		if err != nil {
			return Run{}, ErrInvalidState
		}
	}

	// CHAOS-4242 round 2 (codex): dora is started by two independent
	// triggers carrying two independent generation strings
	// (post-sync:<sync_run_id> vs fixed-schedule:dora_daily_fanout:<time>),
	// so the (org,family,generation,scope_key) uniqueness constraint below
	// -- which makes ONE trigger idempotent against ITS OWN replays -- does
	// nothing to stop the OTHER trigger from inserting a second, genuinely
	// different run for the same org+day. RemainingMetricsCoverageStore's
	// pre-flight in the fixed-schedule producer narrows the window but does
	// not close it: that read happens on the outer occurrence transaction,
	// before this call even begins, so a post-sync run committing in
	// between is invisible to it (and post-sync's own writer never checked
	// coverage at all). This block is the actual serialization point, on
	// every StartRunTx call for family "dora" regardless of caller: take a
	// transaction-scoped advisory lock keyed on (org, day) -- so a
	// concurrent StartRunTx for the SAME org+day, from EITHER trigger,
	// blocks until this one commits or rolls back -- then look for an
	// already-succeeded partition for that day under ANY generation. If one
	// exists (created by this call, a prior call, or a call that was
	// blocked on the same lock and just committed), return THAT run instead
	// of inserting a new one: there is nothing left to compute, and the
	// caller (post-sync's writer) only needs a valid run ID to derive its
	// own completion key from -- correct regardless of which generation
	// actually did the work.
	if request.Family == "dora" && len(request.Scopes) == 1 {
		if day, backfillDays, ok := doraScopeDayAndBackfill(request.Scopes[0]); ok {
			if _, err := tx.Exec(ctx,
				"SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
				"remaining_metrics_dora_day", request.OrganizationID+":"+day,
			); err != nil {
				return Run{}, ErrUnavailable
			}
			covering, outputEvidence, found, err := store.loadRunCoveringDay(
				ctx, tx, request.OrganizationID, request.Family, day, backfillDays,
			)
			if err != nil {
				return Run{}, err
			}
			// CHAOS-4384: a 0-row partition that succeeded for a day which has
			// not closed yet is post-sync's own earliest trigger of the UTC
			// day computing before that day has any deployments/incidents at
			// all -- not real coverage. Treating it as terminal froze
			// dora_metrics_daily at 0 forever, because every later same-day
			// trigger (org c6a38355 syncs hourly, ~23/day) reused this same
			// empty run instead of recomputing. Once the day CLOSES, a
			// genuine 0-row day (nothing happened) is real coverage again --
			// this only widens the window while the day can still gain rows.
			if found && isZeroRowEvidence(outputEvidence) && dayIsOpen(day, store.now()) {
				store.observeOpenDayZeroRow(request.Family)
				found = false
			}
			if found {
				completionKey, keyErr := joboutbox.CompletionKey("remaining_metric_run", covering.ID)
				if keyErr != nil {
					return Run{}, ErrInvalidState
				}
				if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
					return Run{}, ErrUnavailable
				}
				return covering, nil
			}
			// Not found: the lock is still held for the rest of this
			// function, so a concurrent caller for the same org+day now
			// blocks behind whichever insert happens below.
		}
	}

	runID := deterministicRunID(request)
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
INSERT INTO public.remaining_metric_runs
    (id, org_id, family, generation, scope_key, generation_seed, status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, 'pending', $7, $7)
ON CONFLICT DO NOTHING`,
		runID, request.OrganizationID, request.Family, request.Generation, request.ScopeKey, request.GenerationSeed, now)
	if err != nil {
		return Run{}, ErrUnavailable
	}
	if command.RowsAffected() == 0 {
		run, err := loadStartedRun(ctx, tx, runID)
		if err != nil {
			return Run{}, err
		}
		if !sameRunSeed(run, request) || !sameRunIdentity(run, request) {
			return Run{}, ErrInvalidState
		}
		if run.Status == "succeeded" {
			completionKey, keyErr := joboutbox.CompletionKey("remaining_metric_run", run.ID)
			if keyErr != nil {
				return Run{}, ErrInvalidState
			}
			if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
				return Run{}, ErrUnavailable
			}
		}
		if err := verifyStartedPartitions(ctx, tx, runID, request.Scopes); err != nil {
			return Run{}, err
		}
		if err := publishStartedPartitions(
			ctx, tx, publisher, run, request.Scopes, request.PrerequisiteCompletionKey,
		); err != nil {
			return Run{}, err
		}
		return run, nil
	}

	run := Run{
		ID:             runID,
		OrganizationID: request.OrganizationID,
		Family:         request.Family,
		Generation:     request.Generation,
		ScopeKey:       request.ScopeKey,
		Status:         "pending",
		Seed:           request.GenerationSeed,
	}
	for index, scope := range request.Scopes {
		ordinal := index + 1
		partition := Partition{
			ID: deterministicPartitionID(runID, ordinal), RunID: runID,
			Ordinal: ordinal, Scope: scope,
		}
		_, err := tx.Exec(ctx, `
INSERT INTO public.remaining_metric_partitions
    (id, run_id, ordinal, scope, status, attempt_count, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending', 0, $5, $5)`,
			partition.ID, runID, ordinal, scope, now)
		if err != nil {
			return Run{}, ErrUnavailable
		}
		if publisher != nil {
			if err := publisher.PublishPartitionTx(
				ctx, tx, run, partition, request.PrerequisiteCompletionKey,
			); err != nil {
				return Run{}, err
			}
		}
	}
	return run, nil
}

func (store *PostgresStore) LoadRun(ctx context.Context, runID string) (Run, error) {
	if !store.valid() || !validUUID(runID) {
		return Run{}, ErrUnavailable
	}
	var run Run
	err := store.pool.QueryRow(ctx, `
SELECT id::text, org_id::text, family, generation, scope_key, status, generation_seed
FROM public.remaining_metric_runs WHERE id = $1::uuid`, runID).Scan(
		&run.ID, &run.OrganizationID, &run.Family, &run.Generation, &run.ScopeKey, &run.Status, &run.Seed,
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
SELECT partition.id::text, partition.run_id::text, partition.ordinal, partition.scope
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
		if err := rows.Scan(&partition.ID, &partition.RunID, &partition.Ordinal, &partition.Scope); err != nil {
			return nil, ErrUnavailable
		}
		result = append(result, partition)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return result, nil
}

// ClaimPartition MUST return the partition's scope.
//
// CHAOS-4242: HTTPCompatibilityExecutor.ComputePartition never reads
// Partition.Scope at all -- it posts run_id/partition_id to the Python
// bridge, which re-loads scope itself. DORAExecutor and CapacityExecutor
// (CHAOS-3092 R1 / CUT-20 R2) are the first two callers of ComputePartition
// that decode Partition.Scope directly, and this RETURNING clause was never
// updated for them: it claimed the row but always handed back a zero-value
// json.RawMessage, so every native-executor partition failed
// json.Unmarshal in under a millisecond, before any ClickHouse read or
// write -- on every attempt, for every partition, in both environments.
// Dropping the scope column here again would silently resurrect that exact
// regression for these two kinds while leaving every bridge kind green.
func (store *PostgresStore) ClaimPartition(ctx context.Context, partitionID string) (*Claim, error) {
	if !store.valid() || !validUUID(partitionID) {
		return nil, ErrUnavailable
	}
	now, token := store.now().UTC(), uuid.New()
	var claim Claim
	err := store.pool.QueryRow(ctx, `
WITH claimed AS (
UPDATE public.remaining_metric_partitions AS partition
SET status = 'running', claim_token = $2, lease_expires_at = $3,
    attempt_count = attempt_count + 1, updated_at = $1
WHERE partition.id = $4::uuid AND (
    status IN ('pending', 'failed') OR
    (status = 'running' AND lease_expires_at <= $1)
  )
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = partition.run_id AND run.status IN ('pending', 'running')
  )
RETURNING partition.id::text, partition.run_id::text, partition.ordinal, partition.scope, partition.claim_token::text
), activated_run AS (
    UPDATE public.remaining_metric_runs AS run
    SET status = 'running', updated_at = $1
    WHERE run.id = (SELECT run_id::uuid FROM claimed) AND run.status = 'pending'
)
SELECT id, run_id, ordinal, scope, claim_token FROM claimed`,
		now, token, now.Add(store.lease), partitionID,
	).Scan(
		&claim.Partition.ID, &claim.Partition.RunID, &claim.Partition.Ordinal,
		&claim.Partition.Scope, &claim.Token,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.unclaimableReason(ctx, partitionID, now)
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	claim.LeaseDuration = store.lease
	return &claim, nil
}

// unclaimableReason explains why the conditional claim matched no row. Only a
// live lease is reported; everything else is a genuine no-op and stays nil.
// The read is deliberately advisory rather than locked: the claim above has
// already failed, so the worst case of a stale answer is one extra wake-up,
// never a lost reclaim.
func (store *PostgresStore) unclaimableReason(ctx context.Context, partitionID string, now time.Time) error {
	var status string
	var leaseExpiresAt *time.Time
	err := store.pool.QueryRow(ctx, `
SELECT partition.status, partition.lease_expires_at
FROM public.remaining_metric_partitions AS partition
JOIN public.remaining_metric_runs AS run ON run.id = partition.run_id
WHERE partition.id = $1::uuid AND run.status IN ('pending', 'running')`,
		partitionID).Scan(&status, &leaseExpiresAt)
	if err != nil {
		// A missing row, or a run that is no longer live, means there is nothing
		// for this job to come back for.
		return nil
	}
	if status != "running" || leaseExpiresAt == nil {
		return nil
	}
	if remaining := leaseExpiresAt.Sub(now); remaining > 0 {
		return &LeaseActiveError{RetryAfter: remaining}
	}
	return nil
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
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	command, err := tx.Exec(ctx, `
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
	runTransition, err := tx.Exec(ctx, `
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
  )`, now, claim.Partition.RunID)
	if err != nil {
		return ErrUnavailable
	}
	if runTransition.RowsAffected() == 1 {
		completionKey, keyErr := joboutbox.CompletionKey(
			"remaining_metric_run", claim.Partition.RunID,
		)
		if keyErr != nil {
			return ErrInvalidState
		}
		if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
			return ErrUnavailable
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}

// ReleasePartition stands a claimed partition back down. Release is fenced on
// a live lease, so a claimant that has already outlived its lease cannot
// release it; that outcome is recorded rather than left for the caller to
// discard (CHAOS-4002).
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
		store.observeReleaseLost()
		return ErrLeaseLost
	}
	return nil
}

// HasSucceededPartition reports whether ANY run of this organization/family
// -- regardless of which trigger created it or what generation it carries
// -- already has a succeeded partition whose scope covers exactly this day.
//
// CHAOS-4242: dora is dispatched by two independent triggers with two
// independent generation formats (post-sync's "post-sync:<sync_run_id>" vs
// the fixed schedule's "fixed-schedule:dora_daily_fanout:<time>"), so the
// (org_id, family, generation, scope_key) uniqueness constraint that makes
// replaying ONE trigger's own occurrence idempotent cannot stop the OTHER
// trigger from creating a second, independent run for the same org+day.
// dora_metrics_daily is a plain MergeTree with no dedup on replay (matching
// Python's own append-only job_dora.py -- a parity-correct characteristic
// of the table, not a defect this store papers over by writing
// differently), so a second run is a real duplicate-rows outcome, not a
// merely wasted claim. This is the pre-flight check the fixed schedule's
// dora binding uses to skip an organization the other trigger already
// covered, rather than create that duplicate.
//
// Scoped to (org_id, family) plus an exact `scope->>'day'` match rather than
// a generation/scope_key comparison, because the two triggers' scope_key
// FORMATS also differ (post-sync: the full serialized scope; fixed
// schedule: the bare day string) -- day is the one field both agree on.
func (store *PostgresStore) HasSucceededPartition(
	ctx context.Context, tx pgx.Tx, organizationID, family, day string,
) (bool, error) {
	if !store.valid() || tx == nil || !validUUID(organizationID) ||
		family == "" || day == "" {
		return false, ErrUnavailable
	}
	// run.status = 'succeeded', not just partition.status -- codex round 3:
	// CompletePartition and CancelRun are separate statements, so a
	// partition already marked succeeded whose run was (or is
	// concurrently being) canceled must never read back as coverage.
	var exists bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM public.remaining_metric_partitions AS partition
    JOIN public.remaining_metric_runs AS run ON run.id = partition.run_id
    WHERE run.org_id = $1::uuid AND run.family = $2
      AND run.status = 'succeeded'
      AND partition.status = 'succeeded'
      AND partition.scope->>'day' = $3
)`, organizationID, family, day).Scan(&exists)
	if err != nil {
		return false, ErrUnavailable
	}
	return exists, nil
}

// loadRunCoveringDay finds the run (any generation) that already carries a
// succeeded partition for this exact organization/family/day, if any. Used
// by StartRunTx's cross-trigger dora lock (see there) -- callers other than
// StartRunTx should have no reason to call this directly.
// loadRunCoveringDay requires (codex round 3, both CONFIRMED):
//
//   - run.status = 'succeeded', not just partition.status = 'succeeded'.
//     CompletePartition and CancelRun are separate statements; a partition
//     already marked succeeded whose run was (or is concurrently being)
//     canceled must never be read back as valid coverage -- that would let
//     joboutbox.MarkCompletionTx mint a completion fence for work the
//     domain itself disowned.
//   - the covering partition's OWN backfill_days is >= the requesting
//     scope's backfill_days. The fixed schedule always requests
//     backfill_days=1; post-sync can request up to 90 for a real gap
//     catch-up (postSyncRemainingScope). Comparing day alone let a
//     backfill_days=1 run satisfy a later backfill_days=30 request for the
//     same anchor day, silently leaving the 29 days behind the anchor
//     uncomputed while the wider request was told "already covered".
//
// loadRunCoveringDay also returns the covering partition's output_evidence
// (CHAOS-4384) so the caller can tell a genuine day-closed 0-row completion
// apart from an open-day one that must not be terminal -- see StartRunTx's
// dora block and isZeroRowEvidence/dayIsOpen. output_evidence is nullable at
// the schema level (migration 0058: `output_evidence IS NULL OR length(...)
// BETWEEN 1 AND 4096`) -- every partition CompletePartition itself writes
// carries a non-empty value, but a succeeded row from before this evidence
// format existed (CHAOS-4242's diagnosis found 144 such rows) can still be
// NULL, so this scans a nullable pointer rather than failing the whole call
// with ErrUnavailable on a perfectly valid legacy row (codex round 1, P2).
//
// The ORDER BY prefers a partition that is NOT a 0-row completion over one
// that is, before preferring recency: dora's two independent triggers
// (post-sync, fixed-schedule) can both complete a run for the same org+day
// while neither had yet succeeded when the other started (the advisory lock
// only serializes the START of StartRunTx, not completion), so an earlier
// run with real rows and a later run that legitimately wrote 0 can both
// exist. Picking "most recently completed" alone would let CHAOS-4384's
// open-day exception fire against the 0-row run even though the earlier
// non-zero run is right there -- and since dora_metrics_daily is append-only
// with no dedup on replay (CHAOS-4242), that would insert a genuine
// duplicate rather than a real recompute (codex round 1, P1).
func (store *PostgresStore) loadRunCoveringDay(
	ctx context.Context, tx pgx.Tx, organizationID, family, day string, minBackfillDays int,
) (Run, *string, bool, error) {
	var run Run
	var outputEvidence *string
	err := tx.QueryRow(ctx, `
SELECT run.id::text, run.org_id::text, run.family, run.generation, run.scope_key, run.status, run.generation_seed,
       partition.output_evidence
FROM public.remaining_metric_partitions AS partition
JOIN public.remaining_metric_runs AS run ON run.id = partition.run_id
WHERE run.org_id = $1::uuid AND run.family = $2
  AND run.status = 'succeeded'
  AND partition.status = 'succeeded'
  AND partition.scope->>'day' = $3
  AND coalesce((partition.scope->>'backfill_days')::int, 1) >= $4
ORDER BY coalesce(partition.output_evidence LIKE '%:rows_written=0', false) ASC, partition.completed_at DESC
LIMIT 1`, organizationID, family, day, minBackfillDays).Scan(
		&run.ID, &run.OrganizationID, &run.Family, &run.Generation, &run.ScopeKey, &run.Status, &run.Seed,
		&outputEvidence,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, nil, false, nil
	}
	if err != nil {
		return Run{}, nil, false, ErrUnavailable
	}
	return run, outputEvidence, true, nil
}

// isZeroRowEvidence reports whether outputEvidence records an explicit
// rows_written=0 completion, in exactly the format
// compatibilityCompletionResult (handler.go) writes it. CHAOS-4243 is what
// guarantees a zero write is never indistinguishable from a plain,
// unqualified success string here. A nil evidence (a legacy row predating
// this format, or any other family that never reports rows_written) is
// never treated as a zero-row completion (codex round 1, P2).
func isZeroRowEvidence(outputEvidence *string) bool {
	return outputEvidence != nil && strings.HasSuffix(*outputEvidence, ":rows_written=0")
}

// dayIsOpen reports whether day (a canonical "2006-01-02" UTC date) has not
// fully elapsed as of now -- i.e. it is today or later. ISO-8601 dates
// compare correctly as plain strings, so no time.Parse round-trip is needed.
func dayIsOpen(day string, now time.Time) bool {
	return day >= now.UTC().Format("2006-01-02")
}

// doraScopeDay extracts the "day" field from an already-canonicalized dora
// scope. ok is false for any family whose scope has no such field, or a
// malformed one -- callers treat that as "the cross-trigger lock does not
// apply", never as an error, since only dora needs it today.
func doraScopeDay(raw json.RawMessage) (day string, ok bool) {
	var scope doraScope
	if err := json.Unmarshal(raw, &scope); err != nil || scope.Day == "" {
		return "", false
	}
	return scope.Day, true
}

// doraScopeDayAndBackfill is doraScopeDay plus the scope's backfill_days --
// CHAOS-4242 round 3 (codex): the cross-trigger lock's coverage check must
// not compare on day alone. The fixed schedule always requests
// backfill_days=1; post-sync can request up to 90 (a real gap catch-up,
// postSyncRemainingScope in cmd/dev-health-worker/sync_dispatch.go). A
// day-only match let a backfill_days=1 run (say, the fixed schedule)
// falsely satisfy a LATER backfill_days=30 request for the same anchor
// day: the wider request would be told "already covered" and silently
// skip computing the 29 days behind the anchor.
func doraScopeDayAndBackfill(raw json.RawMessage) (day string, backfillDays int, ok bool) {
	var scope doraScope
	if err := json.Unmarshal(raw, &scope); err != nil || scope.Day == "" || scope.BackfillDays < 1 {
		return "", 0, false
	}
	return scope.Day, scope.BackfillDays, true
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

func validateStartRunRequest(request StartRunRequest) error {
	if !validUUID(request.OrganizationID) ||
		utf8.RuneCountInString(request.Generation) < 1 || utf8.RuneCountInString(request.Generation) > maxGenerationLength ||
		utf8.RuneCountInString(request.ScopeKey) < 1 || utf8.RuneCountInString(request.ScopeKey) > maxScopeKeyLength ||
		len(request.Scopes) < 1 || len(request.Scopes) > maxScopesPerRun ||
		len(request.PrerequisiteCompletionKey) > 256 {
		return ErrInvalidState
	}
	inventory, err := Load()
	if err != nil {
		return err
	}
	found := false
	for _, family := range inventory.Families {
		if request.Family == family.Name {
			found = true
			break
		}
	}
	if !found || (request.Family == "capacity") != (request.GenerationSeed != nil) {
		return ErrInvalidState
	}
	return nil
}

func deterministicRunID(request StartRunRequest) string {
	identity, err := json.Marshal([]string{
		"remaining-metrics-run", request.OrganizationID, request.Family, request.Generation, request.ScopeKey,
	})
	if err != nil {
		panic("remaining metrics run identity cannot be encoded")
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, identity).String()
}

func deterministicPartitionID(runID string, ordinal int) string {
	runUUID := uuid.MustParse(runID)
	return uuid.NewSHA1(runUUID, []byte("remaining-metrics-partition/"+strconv.Itoa(ordinal))).String()
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return nil, ErrInvalidState
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidState
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalidState
	}
	return canonical, nil
}

func cloneScopes(scopes []json.RawMessage) []json.RawMessage {
	cloned := make([]json.RawMessage, len(scopes))
	for index, scope := range scopes {
		cloned[index] = append(json.RawMessage(nil), scope...)
	}
	return cloned
}

func loadStartedRun(ctx context.Context, tx pgx.Tx, runID string) (Run, error) {
	var run Run
	err := tx.QueryRow(ctx, `
SELECT id::text, org_id::text, family, generation, scope_key, status, generation_seed
FROM public.remaining_metric_runs WHERE id = $1::uuid`, runID).Scan(
		&run.ID, &run.OrganizationID, &run.Family, &run.Generation, &run.ScopeKey, &run.Status, &run.Seed,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrInvalidState
	}
	if err != nil {
		return Run{}, ErrUnavailable
	}
	return run, nil
}

func sameRunIdentity(run Run, request StartRunRequest) bool {
	return run.OrganizationID == request.OrganizationID && run.Family == request.Family &&
		run.Generation == request.Generation && run.ScopeKey == request.ScopeKey
}

func sameRunSeed(run Run, request StartRunRequest) bool {
	if run.Seed == nil || request.GenerationSeed == nil {
		return run.Seed == nil && request.GenerationSeed == nil
	}
	return *run.Seed == *request.GenerationSeed
}

func verifyStartedPartitions(ctx context.Context, tx pgx.Tx, runID string, scopes []json.RawMessage) error {
	rows, err := tx.Query(ctx, `
SELECT id::text, ordinal, scope
FROM public.remaining_metric_partitions
WHERE run_id = $1::uuid ORDER BY ordinal`, runID)
	if err != nil {
		return ErrUnavailable
	}
	defer rows.Close()
	expectedCount := 0
	expectedOrdinal := 1
	for rows.Next() {
		var id string
		var persistedOrdinal int
		var persisted json.RawMessage
		if err := rows.Scan(&id, &persistedOrdinal, &persisted); err != nil {
			return ErrUnavailable
		}
		canonical, err := canonicalJSON(persisted)
		if err != nil {
			return ErrInvalidState
		}
		if persistedOrdinal != expectedOrdinal || expectedCount >= len(scopes) ||
			id != deterministicPartitionID(runID, expectedOrdinal) || !bytes.Equal(canonical, scopes[expectedCount]) {
			return ErrInvalidState
		}
		expectedCount++
		expectedOrdinal++
	}
	if err := rows.Err(); err != nil {
		return ErrUnavailable
	}
	if expectedCount != len(scopes) {
		return fmt.Errorf("%w: partition count mismatch", ErrInvalidState)
	}
	return nil
}

func publishStartedPartitions(
	ctx context.Context,
	tx pgx.Tx,
	publisher PartitionPublisher,
	run Run,
	scopes []json.RawMessage,
	prerequisiteCompletionKey string,
) error {
	if publisher == nil {
		return nil
	}
	for index, scope := range scopes {
		ordinal := index + 1
		if err := publisher.PublishPartitionTx(ctx, tx, run, Partition{
			ID:      deterministicPartitionID(run.ID, ordinal),
			RunID:   run.ID,
			Ordinal: ordinal,
			Scope:   scope,
		}, prerequisiteCompletionKey); err != nil {
			return err
		}
	}
	return nil
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}
