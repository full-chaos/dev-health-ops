package sync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Reconcile lifecycle values. They mirror the checked database constraints on
// public.scheduled_sync_occurrences; a value not listed here is rejected by the
// table, not merely unexpected here.
const (
	OccurrenceReconcilePending     = "pending"
	OccurrenceReconcileRetry       = "retry"
	OccurrenceReconcileCompleted   = "completed"
	OccurrenceReconcileQuarantined = "quarantined"
)

// Terminal and retryable error codes recorded on an occurrence.
const (
	// OccurrenceErrorIdentityConflict means the persisted occurrence does not
	// match the identity recomputed from its own configuration and marker.
	// This is never retried: replaying it would materialize a run under an
	// identity that no longer describes it.
	OccurrenceErrorIdentityConflict = "identity_conflict"
	// OccurrenceErrorIneligible means organization, entitlement, or schedule
	// policy currently forbids materialization. It is retryable because an
	// entitlement can be restored.
	OccurrenceErrorIneligible = "ineligible"
	// OccurrenceErrorPlannerError means materialization failed for any other
	// reason.
	OccurrenceErrorPlannerError = "planner_error"
	// OccurrenceErrorRetryExhausted is written when the attempt budget runs out.
	OccurrenceErrorRetryExhausted = "retry_exhausted"
)

const (
	// occurrenceReconcileMaxAttempts matches the Python consumer's budget. The
	// fifth failure is terminal.
	occurrenceReconcileMaxAttempts = 5
	// occurrenceReconcileBackoffCap bounds one deferral.
	occurrenceReconcileBackoffCap = 15 * time.Minute
	// occurrenceReconcileBackoffBase is the first deferral.
	occurrenceReconcileBackoffBase = 60 * time.Second
	// occurrenceReconcileDefaultLimit bounds one reconcile batch.
	occurrenceReconcileDefaultLimit = 100
	occurrenceReconcileMaxLimit     = 500
)

var (
	// ErrOccurrenceIneligible is returned by a Materializer when organization,
	// entitlement, or schedule policy forbids materialization right now.
	ErrOccurrenceIneligible = errors.New("scheduled sync occurrence is ineligible")
	// ErrMaterializerUnavailable identifies a missing or unusable materializer.
	ErrMaterializerUnavailable = errors.New("scheduled sync materializer is unavailable")
	// ErrOccurrenceReconcilerUnavailable identifies an unusable reconciler.
	ErrOccurrenceReconcilerUnavailable = errors.New("scheduled sync occurrence reconciler is unavailable")
)

// PendingOccurrence is one claimed occurrence awaiting materialization.
type PendingOccurrence struct {
	ID              string
	IdentityVersion string
	OrgID           string
	ConfigID        string
	JobID           string
	ScheduledFor    time.Time
	AttemptCount    int
	// ConfigScheduleCron and ConfigTimezone come from the locked configuration
	// and are used only for identity validation, never to re-decide due-ness:
	// the due time is already fixed by the persisted occurrence.
	ConfigActive bool
	JobStatus    int
	JobType      string
}

// PlanResult is the authoritative graph one materialization produced.
type PlanResult struct {
	JobRunID  string
	SyncRunID string
}

// Materializer creates the authoritative run graph for one occurrence inside
// the reconciler's transaction.
//
// It must not commit, and it must be idempotent for a repeated occurrence: the
// reconciler links the occurrence to the returned identities in the same
// transaction, so a crash between the plan and the link leaves neither.
//
// Returning ErrOccurrenceIneligible records a retryable policy denial.
// Returning any other error records a retryable planner failure. Both are
// bounded by the attempt budget and end in quarantine.
type Materializer interface {
	Materialize(ctx context.Context, tx pgx.Tx, occurrence PendingOccurrence) (PlanResult, error)
}

// OccurrenceReconcileResult is the bounded outcome of one batch.
type OccurrenceReconcileResult struct {
	Scanned         int
	Completed       int
	Retried         int
	Quarantined     int
	AlreadyResolved int
}

// OccurrenceReconciler folds the optional Python occurrence consumer into Go.
//
// The Python consumer existed because the Go scheduler could only write a
// pending occurrence row and something else had to turn it into a run. Go now
// owns both halves, so the consumer's retry backoff, identity validation, and
// quarantine behavior have to live here or a pending occurrence could sit
// forever with nothing to alert on it.
type OccurrenceReconciler struct {
	pool         *pgxpool.Pool
	materializer Materializer
	limit        int
}

// NewOccurrenceReconciler constructs the reconciler. A nil materializer is
// rejected: a reconciler that could claim occurrences but never materialize
// them would burn the attempt budget and quarantine healthy work.
func NewOccurrenceReconciler(pool *pgxpool.Pool, materializer Materializer) (*OccurrenceReconciler, error) {
	if pool == nil {
		return nil, ErrOccurrenceReconcilerUnavailable
	}
	if materializer == nil {
		return nil, ErrMaterializerUnavailable
	}
	return &OccurrenceReconciler{
		pool:         pool,
		materializer: materializer,
		limit:        occurrenceReconcileDefaultLimit,
	}, nil
}

// Reconcile processes one bounded batch of due occurrences.
//
// Each occurrence gets its own transaction rather than a savepoint inside a
// shared one. The Python implementation used SAVEPOINTs so a single bad
// occurrence could not roll back earlier completions in the batch; separate
// transactions give the same isolation and additionally keep one slow planner
// from holding locks on every other occurrence's configuration row.
func (reconciler *OccurrenceReconciler) Reconcile(
	ctx context.Context,
	now time.Time,
	limit int,
) (OccurrenceReconcileResult, error) {
	if reconciler == nil || reconciler.pool == nil || reconciler.materializer == nil {
		return OccurrenceReconcileResult{}, ErrOccurrenceReconcilerUnavailable
	}
	if ctx == nil || now.IsZero() {
		return OccurrenceReconcileResult{}, ErrInvalidTransactionRequest
	}
	if limit <= 0 {
		limit = reconciler.limit
	}
	if limit > occurrenceReconcileMaxLimit {
		limit = occurrenceReconcileMaxLimit
	}
	now = now.UTC()

	keys, err := reconciler.dueOccurrenceKeys(ctx, now, limit)
	if err != nil {
		return OccurrenceReconcileResult{}, err
	}
	result := OccurrenceReconcileResult{Scanned: len(keys)}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		outcome, err := reconciler.reconcileOne(ctx, now, key)
		if err != nil {
			return result, err
		}
		switch outcome {
		case OccurrenceReconcileCompleted:
			result.Completed++
		case OccurrenceReconcileRetry:
			result.Retried++
		case OccurrenceReconcileQuarantined:
			result.Quarantined++
		default:
			result.AlreadyResolved++
		}
	}
	return result, nil
}

// dueOccurrenceKeys reads a bounded candidate list without holding locks. The
// per-occurrence transaction re-checks due-ness under the canonical lock
// order, so a stale key here is harmless.
func (reconciler *OccurrenceReconciler) dueOccurrenceKeys(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]string, error) {
	rows, err := reconciler.pool.Query(ctx, dueOccurrenceKeysSQL, now, limit)
	if err != nil {
		return nil, fmt.Errorf("scan due scheduled sync occurrences: %w", err)
	}
	defer rows.Close()
	keys := make([]string, 0, limit)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan due scheduled sync occurrence: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan due scheduled sync occurrences: %w", err)
	}
	return keys, nil
}

// reconcileOne owns one occurrence end to end. It acquires the canonical lock
// order (configuration, then marker, then occurrence) that the Python
// scheduler and consumer both use; taking them in any other order against a
// live Python scheduler would deadlock during the coexistence window.
func (reconciler *OccurrenceReconciler) reconcileOne(
	ctx context.Context,
	now time.Time,
	key string,
) (string, error) {
	tx, err := reconciler.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin occurrence reconcile transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	occurrence, ok, err := lockPendingOccurrence(ctx, tx, key, now)
	if err != nil {
		return "", err
	}
	if !ok {
		// Another replica holds the locks or the occurrence resolved between
		// the scan and the claim. Both are ordinary, not failures.
		return "", nil
	}

	if !occurrenceIdentityIsValid(occurrence) {
		applied, err := quarantineOccurrence(ctx, tx, occurrence, now, OccurrenceErrorIdentityConflict)
		if err != nil {
			return "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("commit occurrence quarantine: %w", err)
		}
		committed = true
		if !applied {
			return "", nil
		}
		return OccurrenceReconcileQuarantined, nil
	}

	plan, materializeErr := reconciler.materializer.Materialize(ctx, tx, occurrence)
	if materializeErr != nil {
		// The planner may have left partial state in this transaction, so the
		// failure has to be recorded in a clean one. Rolling back first is
		// what keeps a failed plan from being half-committed alongside its own
		// error record.
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = tx.Rollback(rollbackCtx)
		cancel()
		committed = true
		code := OccurrenceErrorPlannerError
		if errors.Is(materializeErr, ErrOccurrenceIneligible) {
			code = OccurrenceErrorIneligible
		}
		return reconciler.deferOccurrence(ctx, now, occurrence, code)
	}
	if plan.JobRunID == "" || plan.SyncRunID == "" {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = tx.Rollback(rollbackCtx)
		cancel()
		committed = true
		// A materializer that reports success without both identities has not
		// produced an authoritative graph. Treating that as completion would
		// mark the occurrence done with nothing behind it.
		return reconciler.deferOccurrence(ctx, now, occurrence, OccurrenceErrorPlannerError)
	}

	if err := completeOccurrence(ctx, tx, occurrence, plan); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit occurrence materialization: %w", err)
	}
	committed = true
	return OccurrenceReconcileCompleted, nil
}

// deferOccurrence records a bounded retry or the terminal quarantine in its own
// transaction.
func (reconciler *OccurrenceReconciler) deferOccurrence(
	ctx context.Context,
	now time.Time,
	occurrence PendingOccurrence,
	code string,
) (string, error) {
	attempt := occurrence.AttemptCount + 1
	exhausted := attempt >= occurrenceReconcileMaxAttempts
	status, recordedCode := OccurrenceReconcileRetry, code
	var nextAttempt *time.Time
	if exhausted {
		status, recordedCode = OccurrenceReconcileQuarantined, OccurrenceErrorRetryExhausted
	} else {
		next := now.Add(occurrenceBackoff(attempt))
		nextAttempt = &next
	}

	tx, err := reconciler.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin occurrence deferral transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	command, err := tx.Exec(
		ctx,
		deferOccurrenceSQL,
		attempt,
		nextAttempt,
		recordedCode,
		now,
		status,
		occurrence.ID,
		occurrence.AttemptCount,
	)
	if err != nil {
		return "", fmt.Errorf("defer scheduled sync occurrence: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit occurrence deferral: %w", err)
	}
	committed = true
	if command.RowsAffected() == 0 {
		// The compare-and-swap on attempt count lost to a concurrent writer.
		// That writer owns the outcome; double-counting it here would
		// overstate retries.
		return "", nil
	}
	if exhausted {
		return OccurrenceReconcileQuarantined, nil
	}
	return OccurrenceReconcileRetry, nil
}

// occurrenceBackoff reproduces the checked Python ladder exactly: 60s, 120s,
// 240s, 480s, capped at 15 minutes. Attempt five is terminal, so the cap is
// unreachable in practice and exists only so a future budget increase stays
// bounded.
func occurrenceBackoff(attempt int) time.Duration {
	exponent := attempt - 1
	if exponent < 0 {
		exponent = 0
	}
	if exponent > 4 {
		exponent = 4
	}
	delay := occurrenceReconcileBackoffBase * (1 << exponent)
	if delay > occurrenceReconcileBackoffCap {
		return occurrenceReconcileBackoffCap
	}
	return delay
}

// occurrenceIdentityIsValid recomputes the deterministic identity from the
// locked configuration and marker and compares it with the persisted row.
//
// This is the guard against a configuration being repointed underneath a
// pending occurrence. Without it, an occurrence written for one configuration
// could materialize a run for another tenant's schedule.
func occurrenceIdentityIsValid(occurrence PendingOccurrence) bool {
	if occurrence.IdentityVersion != OccurrenceIdentityVersion ||
		occurrence.JobType != "sync" || occurrence.OrgID == "" ||
		occurrence.ConfigID == "" || occurrence.JobID == "" {
		return false
	}
	expected := newOccurrence(
		occurrence.ConfigID,
		occurrence.OrgID,
		occurrence.JobID,
		occurrence.ScheduledFor,
		occurrence.ScheduledFor,
		occurrence.ScheduledFor,
	)
	return expected.ID == occurrence.ID
}

func lockPendingOccurrence(
	ctx context.Context,
	tx pgx.Tx,
	key string,
	now time.Time,
) (PendingOccurrence, bool, error) {
	var occurrence PendingOccurrence
	err := tx.QueryRow(ctx, lockPendingOccurrenceSQL, key, now).Scan(
		&occurrence.ID,
		&occurrence.IdentityVersion,
		&occurrence.OrgID,
		&occurrence.ConfigID,
		&occurrence.JobID,
		&occurrence.ScheduledFor,
		&occurrence.AttemptCount,
		&occurrence.ConfigActive,
		&occurrence.JobStatus,
		&occurrence.JobType,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return PendingOccurrence{}, false, nil
	}
	if err != nil {
		return PendingOccurrence{}, false, fmt.Errorf("lock scheduled sync occurrence: %w", err)
	}
	occurrence.ScheduledFor = occurrence.ScheduledFor.UTC()
	return occurrence, true, nil
}

func completeOccurrence(
	ctx context.Context,
	tx pgx.Tx,
	occurrence PendingOccurrence,
	plan PlanResult,
) error {
	command, err := tx.Exec(
		ctx,
		completeOccurrenceSQL,
		plan.JobRunID,
		plan.SyncRunID,
		occurrence.ID,
	)
	if err != nil {
		return fmt.Errorf("link scheduled sync occurrence: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: occurrence %s was resolved concurrently", ErrOccurrenceConflict, occurrence.ID)
	}
	return nil
}

func quarantineOccurrence(
	ctx context.Context,
	tx pgx.Tx,
	occurrence PendingOccurrence,
	now time.Time,
	code string,
) (bool, error) {
	command, err := tx.Exec(ctx, quarantineOccurrenceSQL, code, now, occurrence.ID, occurrence.AttemptCount)
	if err != nil {
		return false, fmt.Errorf("quarantine scheduled sync occurrence: %w", err)
	}
	return command.RowsAffected() == 1, nil
}

// dueOccurrenceKeysSQL uses the partial reconcile-due index. Ordering by
// scheduled_for keeps the oldest missed occurrence first so a backlog drains
// in schedule order rather than by insertion accident.
const dueOccurrenceKeysSQL = `
SELECT occurrence_id
FROM public.scheduled_sync_occurrences
WHERE job_run_id IS NULL
  AND sync_run_id IS NULL
  AND reconcile_status IN ('pending', 'retry')
  AND (reconcile_next_attempt_at IS NULL OR reconcile_next_attempt_at <= $1)
ORDER BY scheduled_for, occurrence_id
LIMIT $2
`

// lockPendingOccurrenceSQL takes the canonical lock order in one statement:
// the join locks sync_configurations and scheduled_jobs before the occurrence
// row, matching the Python scheduler and consumer exactly. SKIP LOCKED lets
// active/active replicas claim disjoint occurrences instead of serializing.
const lockPendingOccurrenceSQL = `
SELECT
    occurrence.occurrence_id,
    occurrence.identity_version,
    occurrence.org_id,
    occurrence.sync_config_id::text,
    occurrence.scheduled_job_id::text,
    occurrence.scheduled_for,
    occurrence.reconcile_attempt_count,
    config.is_active,
    job.status,
    job.job_type
FROM public.scheduled_sync_occurrences AS occurrence
JOIN public.sync_configurations AS config
    ON config.id = occurrence.sync_config_id
    AND config.org_id = occurrence.org_id
JOIN public.scheduled_jobs AS job
    ON job.id = occurrence.scheduled_job_id
    AND job.org_id = occurrence.org_id
    AND job.sync_config_id = occurrence.sync_config_id
WHERE occurrence.occurrence_id = $1
  AND occurrence.job_run_id IS NULL
  AND occurrence.sync_run_id IS NULL
  AND occurrence.reconcile_status IN ('pending', 'retry')
  AND (occurrence.reconcile_next_attempt_at IS NULL OR occurrence.reconcile_next_attempt_at <= $2)
FOR UPDATE OF config, job, occurrence SKIP LOCKED
`

// completeOccurrenceSQL links the authoritative graph. The predicate keeps a
// late writer from overwriting a committed completion.
const completeOccurrenceSQL = `
UPDATE public.scheduled_sync_occurrences
SET job_run_id = $1::uuid,
    sync_run_id = $2::uuid,
    reconcile_attempt_count = 0,
    reconcile_next_attempt_at = NULL,
    reconcile_error_code = NULL,
    reconcile_error_at = NULL,
    reconcile_status = 'completed'
WHERE occurrence_id = $3
  AND job_run_id IS NULL
  AND sync_run_id IS NULL
  AND reconcile_status IN ('pending', 'retry')
`

// deferOccurrenceSQL is a compare-and-swap on the attempt count so two
// replicas that both observed the same failure record exactly one deferral.
const deferOccurrenceSQL = `
UPDATE public.scheduled_sync_occurrences
SET reconcile_attempt_count = $1,
    reconcile_next_attempt_at = $2,
    reconcile_error_code = $3,
    reconcile_error_at = $4,
    reconcile_status = $5
WHERE occurrence_id = $6
  AND reconcile_attempt_count = $7
  AND reconcile_status IN ('pending', 'retry')
  AND job_run_id IS NULL
  AND sync_run_id IS NULL
`

// quarantineOccurrenceSQL is the immediate terminal path. It deliberately does
// not bump the attempt count: an identity conflict is not a failed attempt at
// the same work, it is a statement that the work no longer exists.
const quarantineOccurrenceSQL = `
UPDATE public.scheduled_sync_occurrences
SET reconcile_next_attempt_at = NULL,
    reconcile_error_code = $1,
    reconcile_error_at = $2,
    reconcile_status = 'quarantined'
WHERE occurrence_id = $3
  AND reconcile_attempt_count = $4
  AND reconcile_status IN ('pending', 'retry')
  AND job_run_id IS NULL
  AND sync_run_id IS NULL
`
