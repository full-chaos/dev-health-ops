package report

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRunStore struct {
	pool              *pgxpool.Pool
	now               func() time.Time
	executionLease    time.Duration
	notificationLease time.Duration
	leaseObserver     jobruntime.ReportRunLeaseObserver
}

const (
	defaultExecutionLease    = 5 * time.Minute
	defaultNotificationLease = 5 * time.Minute
	maxExecutionReclaims     = 2
	reclaimExhaustedCode     = "report_run_execution_reclaim_exhausted"
)

func NewPostgresRunStore(
	pool *pgxpool.Pool,
	observers ...jobruntime.ReportRunLeaseObserver,
) (*PostgresRunStore, error) {
	if pool == nil || len(observers) > 1 {
		return nil, ErrDependencyUnavailable
	}
	var observer jobruntime.ReportRunLeaseObserver
	if len(observers) == 1 {
		observer = observers[0]
	}
	return &PostgresRunStore{
		pool: pool, now: time.Now, executionLease: defaultExecutionLease,
		notificationLease: defaultNotificationLease, leaseObserver: observer,
	}, nil
}

func (store *PostgresRunStore) Claim(ctx context.Context, runID, reportID string) (*RunClaim, error) {
	if !store.available() || runID == "" || reportID == "" {
		return nil, ErrDependencyUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin report claim: %w", ErrDependencyUnavailable)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := store.now().UTC()
	var status, actualReportID string
	var leaseExpiresAt *time.Time
	var errorCode *string
	var reclaimCount int
	err = tx.QueryRow(ctx, `
SELECT status, report_id::text, execution_lease_expires_at,
       execution_reclaim_count, error
FROM public.report_runs
WHERE id = $1::uuid
FOR UPDATE`, runID).Scan(
		&status, &actualReportID, &leaseExpiresAt, &reclaimCount, &errorCode,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load report claim: %w", ErrDependencyUnavailable)
	}
	if actualReportID != reportID {
		return nil, nil
	}

	reclaimed := false
	switch status {
	case "pending":
	case "failed":
		if errorCode != nil && *errorCode == reclaimExhaustedCode {
			return nil, ErrRunReclaimExhausted
		}
	case "running":
		if leaseExpiresAt != nil && leaseExpiresAt.After(now) {
			return nil, &RunLeaseActiveError{RetryAfter: leaseExpiresAt.Sub(now)}
		}
		if reclaimCount >= maxExecutionReclaims {
			if _, err := tx.Exec(ctx, `
UPDATE public.report_runs
SET status = 'failed', completed_at = $2,
    duration_seconds = CASE WHEN started_at IS NULL THEN NULL
        ELSE GREATEST(0, EXTRACT(EPOCH FROM ($2 - started_at))) END,
    execution_claim_token = NULL, execution_lease_expires_at = NULL,
    error = $3, error_traceback = NULL
WHERE id = $1::uuid AND status = 'running'`, runID, now, reclaimExhaustedCode); err != nil {
				return nil, fmt.Errorf("terminalize report reclaim: %w", ErrDependencyUnavailable)
			}
			if _, err := tx.Exec(ctx, `
UPDATE public.saved_reports
SET last_run_at = $2, last_run_status = 'failed', updated_at = $2
WHERE id = $1::uuid`, reportID, now); err != nil {
				return nil, fmt.Errorf("advance exhausted report schedule: %w", ErrDependencyUnavailable)
			}
			if err := invalidateScheduledRunMarker(ctx, tx, runID, now); err != nil {
				return nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit exhausted report reclaim: %w", ErrDependencyUnavailable)
			}
			store.observeRunLease(jobruntime.ReportRunLeaseResultFailed)
			return nil, ErrRunReclaimExhausted
		}
		reclaimed = true
		reclaimCount++
	default:
		return nil, nil
	}

	token := uuid.New()
	command, err := tx.Exec(ctx, `
UPDATE public.report_runs
SET status = 'running', started_at = $3, completed_at = NULL,
    duration_seconds = NULL, error = NULL, error_traceback = NULL,
	    attempt_count = attempt_count + 1,
	    execution_claim_token = $4, execution_lease_expires_at = $5,
	    execution_reclaim_count = $6
WHERE id = $1::uuid AND report_id = $2::uuid`,
		runID, reportID, now, token, now.Add(store.executionLease), reclaimCount)
	if err != nil || command.RowsAffected() != 1 {
		return nil, fmt.Errorf("claim report run: %w", ErrDependencyUnavailable)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit report claim: %w", ErrDependencyUnavailable)
	}
	if reclaimed {
		store.observeRunLease(jobruntime.ReportRunLeaseResultRetrying)
	}
	return &RunClaim{
		Token: token.String(), LeaseDuration: store.executionLease, Reclaimed: reclaimed,
	}, nil
}

func (store *PostgresRunStore) Renew(ctx context.Context, runID string, claim RunClaim) error {
	if !store.available() || runID == "" || claim.Token == "" {
		return ErrDependencyUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.report_runs
SET execution_lease_expires_at = $3
WHERE id = $1::uuid AND status = 'running'
  AND execution_claim_token = $2::uuid
  AND execution_lease_expires_at > $4`,
		runID, claim.Token, now.Add(store.executionLease), now)
	if err != nil {
		return fmt.Errorf("renew report run: %w", ErrDependencyUnavailable)
	}
	if command.RowsAffected() != 1 {
		return ErrRunLeaseLost
	}
	return nil
}

func (store *PostgresRunStore) Complete(
	ctx context.Context,
	runID string,
	claim RunClaim,
	artifact Artifact,
) (bool, error) {
	if !store.available() || runID == "" || claim.Token == "" || artifact.Fingerprint == "" {
		return false, ErrDependencyUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin report completion: %w", ErrDependencyUnavailable)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status, reportID string
	var existingFingerprint *string
	var startedAt *time.Time
	var claimToken *string
	var leaseExpiresAt *time.Time
	err = tx.QueryRow(ctx, `
SELECT status, report_id::text, artifact_fingerprint, started_at,
       execution_claim_token::text, execution_lease_expires_at
FROM public.report_runs WHERE id = $1::uuid FOR UPDATE`, runID).
		Scan(&status, &reportID, &existingFingerprint, &startedAt, &claimToken, &leaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load report completion: %w", ErrDependencyUnavailable)
	}
	if status == "canceled" {
		return false, nil
	}
	if status == "success" {
		if existingFingerprint == nil || *existingFingerprint != artifact.Fingerprint {
			return false, ErrArtifactConflict
		}
		return false, nil
	}
	if status != "running" {
		return false, nil
	}
	now := store.now().UTC()
	if claimToken == nil || *claimToken != claim.Token || leaseExpiresAt == nil || !leaseExpiresAt.After(now) {
		return false, ErrRunLeaseLost
	}

	provenance, err := json.Marshal(artifact.Provenance)
	if err != nil {
		return false, fmt.Errorf("encode report provenance: %w", err)
	}
	var duration *float64
	if startedAt != nil {
		value := max(0, now.Sub(startedAt.UTC()).Seconds())
		duration = &value
	}
	artifactURL := artifact.Metadata["artifact_url"]
	command, err := tx.Exec(ctx, `
UPDATE public.report_runs
SET status = 'success', completed_at = $2, duration_seconds = $3,
    rendered_markdown = $4, artifact_url = NULLIF($5, ''),
    provenance_records = $6::json, artifact_fingerprint = $7,
    notification_key = 'report.ready:' || id::text,
    notification_status = 'pending', notification_sent_at = NULL,
    execution_claim_token = NULL, execution_lease_expires_at = NULL,
    error = NULL, error_traceback = NULL
WHERE id = $1::uuid AND status = 'running'
  AND execution_claim_token = $8::uuid
  AND execution_lease_expires_at > $2`,
		runID, now, duration, artifact.Markdown, artifactURL, string(provenance), artifact.Fingerprint, claim.Token)
	if err != nil || command.RowsAffected() != 1 {
		return false, ErrRunLeaseLost
	}
	command, err = tx.Exec(ctx, `
UPDATE public.saved_reports
SET last_run_at = $2, last_run_status = 'success', updated_at = $2
WHERE id = $1::uuid`, reportID, now)
	if err != nil || command.RowsAffected() != 1 {
		return false, fmt.Errorf("persist saved report completion: %w", ErrDependencyUnavailable)
	}
	if err := invalidateScheduledRunMarker(ctx, tx, runID, now); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit report completion: %w", ErrDependencyUnavailable)
	}
	return true, nil
}

func (store *PostgresRunStore) Fail(ctx context.Context, runID string, claim RunClaim, code string) error {
	if !store.available() || runID == "" || claim.Token == "" || code == "" {
		return ErrDependencyUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin report failure: %w", ErrDependencyUnavailable)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now := store.now().UTC()
	var status, reportID string
	var claimToken *string
	var leaseExpiresAt *time.Time
	err = tx.QueryRow(ctx, `
SELECT status, report_id::text, execution_claim_token::text,
       execution_lease_expires_at
FROM public.report_runs
WHERE id = $1::uuid
FOR UPDATE`, runID).Scan(&status, &reportID, &claimToken, &leaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load report failure: %w", ErrDependencyUnavailable)
	}
	if status != "running" {
		return nil
	}
	if claimToken == nil || *claimToken != claim.Token || leaseExpiresAt == nil || !leaseExpiresAt.After(now) {
		return ErrRunLeaseLost
	}
	command, err := tx.Exec(ctx, `
UPDATE public.report_runs
SET status = 'failed', completed_at = $2,
    duration_seconds = CASE WHEN started_at IS NULL THEN NULL
        ELSE GREATEST(0, EXTRACT(EPOCH FROM ($2 - started_at))) END,
	    execution_claim_token = NULL, execution_lease_expires_at = NULL,
	    error = $3, error_traceback = NULL
WHERE id = $1::uuid AND status = 'running'
  AND execution_claim_token = $4::uuid
  AND execution_lease_expires_at > $2`, runID, now, code, claim.Token)
	if err != nil || command.RowsAffected() != 1 {
		return fmt.Errorf("persist report failure: %w", ErrDependencyUnavailable)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.saved_reports
SET last_run_at = $2, last_run_status = 'failed', updated_at = $2
WHERE id = $1::uuid`, reportID, now); err != nil {
		return fmt.Errorf("persist saved report failure: %w", ErrDependencyUnavailable)
	}
	if err := invalidateScheduledRunMarker(ctx, tx, runID, now); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit report failure: %w", ErrDependencyUnavailable)
	}
	return nil
}

// invalidateScheduledRunMarker clears only the marker reached through this
// run's immutable scheduled occurrence. A manual run can target a saved report
// that has a schedule, but it has no occurrence and must not disturb that
// schedule's next due instant.
func invalidateScheduledRunMarker(
	ctx context.Context,
	tx pgx.Tx,
	runID string,
	at time.Time,
) error {
	if _, err := tx.Exec(ctx, `
UPDATE public.scheduled_jobs AS job
SET next_run_at = NULL, updated_at = $2
FROM public.scheduled_report_occurrences AS occurrence
JOIN public.report_runs AS run
  ON run.scheduled_occurrence_id = occurrence.occurrence_id
WHERE run.id = $1::uuid
  AND job.id = occurrence.scheduled_job_id`, runID, at); err != nil {
		return fmt.Errorf("invalidate scheduled report marker: %w", ErrDependencyUnavailable)
	}
	return nil
}

func (store *PostgresRunStore) ClaimNotification(ctx context.Context, runID string) (*NotificationClaim, error) {
	if !store.available() || runID == "" || store.notificationLease < time.Second || store.notificationLease > time.Hour {
		return nil, ErrDependencyUnavailable
	}
	now := store.now().UTC()
	token := uuid.New()
	var claim NotificationClaim
	err := store.pool.QueryRow(ctx, `
UPDATE public.report_runs
SET notification_status = 'delivering', notification_claim_token = $2,
    notification_lease_expires_at = $3
WHERE id = $1::uuid AND status = 'success'
  AND notification_key IS NOT NULL
  AND (notification_status = 'pending' OR (
      notification_status = 'delivering'
      AND notification_lease_expires_at IS NOT NULL
      AND notification_lease_expires_at <= $4
  ))
RETURNING notification_key, notification_claim_token::text`,
		runID, token, now.Add(store.notificationLease), now).Scan(&claim.Key, &claim.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim report notification: %w", ErrDependencyUnavailable)
	}
	return &claim, nil
}

func (store *PostgresRunStore) CompleteNotification(ctx context.Context, runID string, claim NotificationClaim) error {
	return store.transitionNotification(ctx, runID, claim, "delivered", true)
}

func (store *PostgresRunStore) ReleaseNotification(ctx context.Context, runID string, claim NotificationClaim) error {
	return store.transitionNotification(ctx, runID, claim, "pending", false)
}

func (store *PostgresRunStore) transitionNotification(
	ctx context.Context,
	runID string, claim NotificationClaim, to string,
	sent bool,
) error {
	if !store.available() || runID == "" || claim.Token == "" {
		return ErrDependencyUnavailable
	}
	var command pgconnCommandTag
	var err error
	if sent {
		command, err = store.pool.Exec(ctx, `
UPDATE public.report_runs
SET notification_status = $3, notification_sent_at = $4,
    notification_claim_token = NULL, notification_lease_expires_at = NULL
WHERE id = $1::uuid AND notification_status = 'delivering'
  AND notification_claim_token = $2::uuid`,
			runID, claim.Token, to, store.now().UTC())
	} else {
		command, err = store.pool.Exec(ctx, `
UPDATE public.report_runs
SET notification_status = $3, notification_claim_token = NULL,
    notification_lease_expires_at = NULL
WHERE id = $1::uuid AND notification_status = 'delivering'
  AND notification_claim_token = $2::uuid`,
			runID, claim.Token, to)
	}
	if err != nil || command.RowsAffected() != 1 {
		return fmt.Errorf("transition report notification: %w", ErrDependencyUnavailable)
	}
	return nil
}

type pgconnCommandTag interface {
	RowsAffected() int64
}

func (store *PostgresRunStore) available() bool {
	return store != nil && store.pool != nil && store.now != nil &&
		store.executionLease >= time.Second && store.executionLease <= time.Hour
}

func (store *PostgresRunStore) observeRunLease(result jobruntime.ReportRunLeaseResult) {
	if store.leaseObserver != nil {
		_ = store.leaseObserver.ObserveReportRunLeaseExpired(result)
	}
}
