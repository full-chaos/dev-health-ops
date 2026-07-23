package report

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRunStore struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewPostgresRunStore(pool *pgxpool.Pool) (*PostgresRunStore, error) {
	if pool == nil {
		return nil, ErrDependencyUnavailable
	}
	return &PostgresRunStore{pool: pool, now: time.Now}, nil
}

func (store *PostgresRunStore) Claim(ctx context.Context, runID, reportID string) (bool, error) {
	if !store.available() || runID == "" || reportID == "" {
		return false, ErrDependencyUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.report_runs
SET status = 'running', started_at = $3, completed_at = NULL,
    duration_seconds = NULL, error = NULL, error_traceback = NULL,
    attempt_count = attempt_count + 1
WHERE id = $1::uuid AND report_id = $2::uuid AND status IN ('pending', 'failed')`,
		runID, reportID, now)
	if err != nil {
		return false, fmt.Errorf("claim report run: %w", ErrDependencyUnavailable)
	}
	return command.RowsAffected() == 1, nil
}

func (store *PostgresRunStore) Complete(ctx context.Context, runID string, artifact Artifact) (bool, error) {
	if !store.available() || runID == "" || artifact.Fingerprint == "" {
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
	err = tx.QueryRow(ctx, `
SELECT status, report_id::text, artifact_fingerprint, started_at
FROM public.report_runs WHERE id = $1::uuid FOR UPDATE`, runID).
		Scan(&status, &reportID, &existingFingerprint, &startedAt)
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

	provenance, err := json.Marshal(artifact.Provenance)
	if err != nil {
		return false, fmt.Errorf("encode report provenance: %w", err)
	}
	now := store.now().UTC()
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
    error = NULL, error_traceback = NULL
WHERE id = $1::uuid AND status = 'running'`,
		runID, now, duration, artifact.Markdown, artifactURL, string(provenance), artifact.Fingerprint)
	if err != nil || command.RowsAffected() != 1 {
		return false, fmt.Errorf("persist report completion: %w", ErrDependencyUnavailable)
	}
	command, err = tx.Exec(ctx, `
UPDATE public.saved_reports
SET last_run_at = $2, last_run_status = 'success', updated_at = $2
WHERE id = $1::uuid`, reportID, now)
	if err != nil || command.RowsAffected() != 1 {
		return false, fmt.Errorf("persist saved report completion: %w", ErrDependencyUnavailable)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit report completion: %w", ErrDependencyUnavailable)
	}
	return true, nil
}

func (store *PostgresRunStore) Fail(ctx context.Context, runID, code string) error {
	if !store.available() || runID == "" || code == "" {
		return ErrDependencyUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin report failure: %w", ErrDependencyUnavailable)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now := store.now().UTC()
	var reportID string
	err = tx.QueryRow(ctx, `
UPDATE public.report_runs
SET status = 'failed', completed_at = $2,
    duration_seconds = CASE WHEN started_at IS NULL THEN NULL
        ELSE GREATEST(0, EXTRACT(EPOCH FROM ($2 - started_at))) END,
    error = $3, error_traceback = NULL
WHERE id = $1::uuid AND status = 'running'
RETURNING report_id::text`, runID, now, code).Scan(&reportID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("persist report failure: %w", ErrDependencyUnavailable)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.saved_reports
SET last_run_at = $2, last_run_status = 'failed', updated_at = $2
WHERE id = $1::uuid`, reportID, now); err != nil {
		return fmt.Errorf("persist saved report failure: %w", ErrDependencyUnavailable)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit report failure: %w", ErrDependencyUnavailable)
	}
	return nil
}

func (store *PostgresRunStore) ClaimNotification(ctx context.Context, runID string) (string, bool, error) {
	if !store.available() || runID == "" {
		return "", false, ErrDependencyUnavailable
	}
	var key string
	err := store.pool.QueryRow(ctx, `
UPDATE public.report_runs
SET notification_status = 'delivering'
WHERE id = $1::uuid AND status = 'success'
  AND notification_status = 'pending' AND notification_key IS NOT NULL
RETURNING notification_key`, runID).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("claim report notification: %w", ErrDependencyUnavailable)
	}
	return key, true, nil
}

func (store *PostgresRunStore) CompleteNotification(ctx context.Context, runID string) error {
	return store.transitionNotification(ctx, runID, "delivering", "delivered", true)
}

func (store *PostgresRunStore) ReleaseNotification(ctx context.Context, runID string) error {
	return store.transitionNotification(ctx, runID, "delivering", "pending", false)
}

func (store *PostgresRunStore) transitionNotification(
	ctx context.Context,
	runID, from, to string,
	sent bool,
) error {
	if !store.available() || runID == "" {
		return ErrDependencyUnavailable
	}
	var command pgconnCommandTag
	var err error
	if sent {
		command, err = store.pool.Exec(ctx, `
UPDATE public.report_runs
SET notification_status = $3, notification_sent_at = $4
WHERE id = $1::uuid AND notification_status = $2`,
			runID, from, to, store.now().UTC())
	} else {
		command, err = store.pool.Exec(ctx, `
UPDATE public.report_runs
SET notification_status = $3
WHERE id = $1::uuid AND notification_status = $2`,
			runID, from, to)
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
	return store != nil && store.pool != nil && store.now != nil
}
