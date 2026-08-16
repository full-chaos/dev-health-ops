//go:build integration

package report

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRunStorePreservesArtifactCancellationAndNotificationSemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createReportTables(t, ctx, pool)

	const (
		reportID     = "00000000-0000-4000-8000-000000000002"
		runID        = "00000000-0000-4000-8000-000000000001"
		jobID        = "00000000-0000-4000-8000-000000000005"
		occurrenceID = "scheduled-success"
	)
	if _, err := pool.Exec(ctx, `
INSERT INTO saved_reports (id, org_id, report_plan, parameters, is_active, last_run_status, updated_at)
VALUES ($1, 'org-1',
 '{"plan_id":"plan-1","report_type":"weekly_health","org_id":"org-1","sections":["summary"]}'::jsonb,
 '{}'::jsonb, TRUE, NULL, NOW())`, reportID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO scheduled_jobs (id, next_run_at, updated_at)
VALUES ($1, NOW() + INTERVAL '1 day', NOW())`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO scheduled_report_occurrences (
    occurrence_id, report_id, scheduled_job_id, scheduled_for
) VALUES ($1, $2, $3, NOW())`, occurrenceID, reportID, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, attempt_count, notification_status)
VALUES ($1, $2, 'pending', 0, 'pending')`, runID, reportID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE report_runs SET scheduled_occurrence_id = $2 WHERE id = $1::uuid`, runID, occurrenceID); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresRunStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	claim, err := store.Claim(ctx, runID, reportID)
	if err != nil || claim == nil {
		t.Fatalf("first claim = %#v, %v", claim, err)
	}
	duplicateClaim, err := store.Claim(ctx, runID, reportID)
	if !errors.Is(err, ErrRunLeaseActive) || duplicateClaim != nil {
		t.Fatalf("duplicate claim = %#v, %v", duplicateClaim, err)
	}
	loader, err := NewPostgresReportLoader(pool)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := loader.Load(ctx, QueryInput{ReportID: reportID, RunID: runID})
	if err != nil || definition.Plan.PlanID != "plan-1" || definition.Plan.OrganizationID != "org-1" {
		t.Fatalf("loaded definition = %#v, %v", definition, err)
	}
	artifact := Artifact{
		Markdown: "# report\n", Fingerprint: "sha256:stable",
		Provenance: []ProvenanceRecord{{
			ProvenanceID: "proof-1", ArtifactType: "report", ArtifactID: "plan-1",
		}},
	}
	completed, err := store.Complete(ctx, runID, *claim, artifact)
	if err != nil || !completed {
		t.Fatalf("complete = %v, %v", completed, err)
	}
	if next := readReportNextRunAt(t, ctx, pool, jobID); next != nil {
		t.Fatalf("scheduled completion left next_run_at = %v, want NULL", next)
	}
	completed, err = store.Complete(ctx, runID, *claim, artifact)
	if err != nil || completed {
		t.Fatalf("identical retry = %v, %v", completed, err)
	}
	conflict := artifact
	conflict.Fingerprint = "sha256:different"
	if _, err := store.Complete(ctx, runID, *claim, conflict); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting retry error = %v", err)
	}
	firstNotificationClaim, err := store.ClaimNotification(ctx, runID)
	if err != nil || firstNotificationClaim == nil || firstNotificationClaim.Key != "report.ready:"+runID {
		t.Fatalf("notification claim = %#v, %v", firstNotificationClaim, err)
	}
	if duplicate, err := store.ClaimNotification(ctx, runID); err != nil || duplicate != nil {
		t.Fatalf("unexpired notification lease = %#v, %v", duplicate, err)
	}

	// A process can die after the durable pending -> delivering transition.
	// Only an expired lease is reclaimable; the old token is fenced from
	// completing or releasing the newer delivery attempt.
	now = now.Add(store.notificationLease + time.Second)
	reclaimedNotification, err := store.ClaimNotification(ctx, runID)
	if err != nil || reclaimedNotification == nil || reclaimedNotification.Token == firstNotificationClaim.Token {
		t.Fatalf("reclaimed notification = %#v, %v", reclaimedNotification, err)
	}
	if err := store.CompleteNotification(ctx, runID, *firstNotificationClaim); err == nil {
		t.Fatal("stale notification claimant completed a reclaimed delivery")
	}
	if err := store.ReleaseNotification(ctx, runID, *firstNotificationClaim); err == nil {
		t.Fatal("stale notification claimant released a reclaimed delivery")
	}
	if err := store.CompleteNotification(ctx, runID, *reclaimedNotification); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := store.ClaimNotification(ctx, runID); err != nil || duplicate != nil {
		t.Fatalf("delivered notification was reclaimed = %#v, %v", duplicate, err)
	}

	const canceledRunID = "00000000-0000-4000-8000-000000000003"
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, attempt_count, notification_status)
VALUES ($1, $2, 'canceled', 0, 'pending')`, canceledRunID, reportID); err != nil {
		t.Fatal(err)
	}
	canceledClaim, err := store.Claim(ctx, canceledRunID, reportID)
	if err != nil || canceledClaim != nil {
		t.Fatalf("canceled claim = %#v, %v", canceledClaim, err)
	}

	const retryRunID = "00000000-0000-4000-8000-000000000004"
	manualMarker := now.Add(24 * time.Hour)
	if _, err := pool.Exec(ctx, `
UPDATE scheduled_jobs SET next_run_at = $2 WHERE id = $1::uuid`, jobID, manualMarker); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, attempt_count, notification_status)
VALUES ($1, $2, 'pending', 0, 'pending')`, retryRunID, reportID); err != nil {
		t.Fatal(err)
	}
	retryClaim, err := store.Claim(ctx, retryRunID, reportID)
	if err != nil || retryClaim == nil {
		t.Fatalf("retry run first claim = %#v, %v", retryClaim, err)
	}
	if err := store.Fail(ctx, retryRunID, *retryClaim, "query_failed"); err != nil {
		t.Fatal(err)
	}
	if next := readReportNextRunAt(t, ctx, pool, jobID); next == nil || !next.Equal(manualMarker) {
		t.Fatalf("manual failure changed next_run_at = %v, want %s", next, manualMarker)
	}
	failedRetryClaim, err := store.Claim(ctx, retryRunID, reportID)
	if err != nil || failedRetryClaim == nil {
		t.Fatalf("failed run retry claim = %#v, %v", failedRetryClaim, err)
	}

	const (
		failedScheduledRunID = "00000000-0000-4000-8000-000000000006"
		failedOccurrenceID   = "scheduled-failure"
	)
	if _, err := pool.Exec(ctx, `
INSERT INTO scheduled_report_occurrences (
    occurrence_id, report_id, scheduled_job_id, scheduled_for
) VALUES ($1, $2, $3, NOW())`, failedOccurrenceID, reportID, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (
    id, report_id, scheduled_occurrence_id, status, attempt_count, notification_status
) VALUES ($1, $2, $3, 'pending', 0, 'pending')`, failedScheduledRunID, reportID, failedOccurrenceID); err != nil {
		t.Fatal(err)
	}
	failedScheduledClaim, err := store.Claim(ctx, failedScheduledRunID, reportID)
	if err != nil || failedScheduledClaim == nil {
		t.Fatalf("scheduled failure claim = %#v, %v", failedScheduledClaim, err)
	}
	if err := store.Fail(ctx, failedScheduledRunID, *failedScheduledClaim, "render_failed"); err != nil {
		t.Fatal(err)
	}
	if next := readReportNextRunAt(t, ctx, pool, jobID); next != nil {
		t.Fatalf("scheduled failure left next_run_at = %v, want NULL", next)
	}
}

func TestPostgresRunStoreReclaimsAnExpiredRunningRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createReportTables(t, ctx, pool)

	const (
		reportID = "00000000-0000-4000-8000-000000000012"
		runID    = "00000000-0000-4000-8000-000000000011"
		jobID    = "00000000-0000-4000-8000-000000000014"
	)
	now := time.Date(2026, 8, 13, 18, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO saved_reports (id, org_id, report_plan, parameters, is_active, updated_at)
VALUES ($1, 'org-1', '{}'::jsonb, '{}'::jsonb, TRUE, $2)`, reportID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO scheduled_jobs (id, next_run_at, updated_at)
VALUES ($1, $2, $2)`, jobID, now.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (
    id, report_id, status, started_at, attempt_count, notification_status,
    execution_claim_token, execution_lease_expires_at, execution_reclaim_count
) VALUES ($1, $2, 'running', $3, 1, 'pending', $4, $5, 0)`,
		runID, reportID, now.Add(-time.Minute),
		"00000000-0000-4000-8000-000000000099", now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresRunStore(pool, collector)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	claim, err := store.Claim(ctx, runID, reportID)
	if err != nil || claim == nil || !claim.Reclaimed {
		t.Fatalf("expired running claim = %#v, %v; want reclaimed", claim, err)
	}
	if claim.Token == "00000000-0000-4000-8000-000000000099" {
		t.Fatal("reclaim reused the stale execution fence")
	}
	if duplicate, err := store.Claim(ctx, runID, reportID); !errors.Is(err, ErrRunLeaseActive) || duplicate != nil {
		t.Fatalf("unexpired running claim = %#v, %v; want attempt-neutral delay", duplicate, err)
	} else {
		var active *RunLeaseActiveError
		if !errors.As(err, &active) || active.RetryAfter <= 0 {
			t.Fatalf("active lease delay = %#v, want positive", active)
		}
	}
	artifact := Artifact{Markdown: "# recovered\n", Fingerprint: "sha256:recovered"}
	stale := RunClaim{
		Token: "00000000-0000-4000-8000-000000000099", LeaseDuration: store.executionLease,
	}
	if err := store.Renew(ctx, runID, stale); !errors.Is(err, ErrRunLeaseLost) {
		t.Fatalf("stale execution renewal error = %v", err)
	}
	if _, err := store.Complete(ctx, runID, stale, artifact); !errors.Is(err, ErrRunLeaseLost) {
		t.Fatalf("stale execution completion error = %v", err)
	}
	if err := store.Fail(ctx, runID, stale, "stale_worker_failed"); !errors.Is(err, ErrRunLeaseLost) {
		t.Fatalf("stale execution failure error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE report_runs SET execution_lease_expires_at = $2
WHERE id = $1::uuid`, runID, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(ctx, runID, *claim, artifact); !errors.Is(err, ErrRunLeaseLost) {
		t.Fatalf("expired current execution completion error = %v", err)
	}
	claim, err = store.Claim(ctx, runID, reportID)
	if err != nil || claim == nil || !claim.Reclaimed {
		t.Fatalf("second recovery claim = %#v, %v", claim, err)
	}
	now = now.Add(store.executionLease / 2)
	if err := store.Renew(ctx, runID, *claim); err != nil {
		t.Fatalf("renew reclaimed execution: %v", err)
	}
	if completed, err := store.Complete(ctx, runID, *claim, artifact); err != nil || !completed {
		t.Fatalf("complete recovered execution = %t, %v", completed, err)
	}
	var status string
	var reclaimCount int
	var claimToken *string
	var leaseExpiresAt *time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, execution_reclaim_count, execution_claim_token, execution_lease_expires_at
FROM report_runs WHERE id = $1::uuid`, runID).Scan(
		&status, &reclaimCount, &claimToken, &leaseExpiresAt,
	); err != nil {
		t.Fatal(err)
	}
	if status != "success" || reclaimCount != 2 || claimToken != nil || leaseExpiresAt != nil {
		t.Fatalf("recovered state status=%s reclaims=%d token=%v lease=%v",
			status, reclaimCount, claimToken, leaseExpiresAt)
	}

	// A different run proves the persisted ceiling. The first two expired
	// holders are replaced. The third expiry becomes a loud terminal failure,
	// and later automatic attempts cannot restart the cycle.
	const exhaustedRunID = "00000000-0000-4000-8000-000000000013"
	if _, err := pool.Exec(ctx, `
INSERT INTO scheduled_report_occurrences (
    occurrence_id, report_id, scheduled_job_id, scheduled_for
) VALUES ('scheduled-exhaustion', $1, $2, $3)`, reportID, jobID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (
    id, report_id, scheduled_occurrence_id, status, attempt_count, notification_status
) VALUES ($1, $2, 'scheduled-exhaustion', 'pending', 0, 'pending')`, exhaustedRunID, reportID); err != nil {
		t.Fatal(err)
	}
	initial, err := store.Claim(ctx, exhaustedRunID, reportID)
	if err != nil || initial == nil || initial.Reclaimed {
		t.Fatalf("initial bounded claim = %#v, %v", initial, err)
	}
	for expected := 1; expected <= maxExecutionReclaims; expected++ {
		if _, err := pool.Exec(ctx, `
UPDATE report_runs SET execution_lease_expires_at = $2
WHERE id = $1::uuid`, exhaustedRunID, now.Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		reclaimed, err := store.Claim(ctx, exhaustedRunID, reportID)
		if err != nil || reclaimed == nil || !reclaimed.Reclaimed {
			t.Fatalf("reclaim %d = %#v, %v", expected, reclaimed, err)
		}
	}
	if _, err := pool.Exec(ctx, `
UPDATE report_runs SET execution_lease_expires_at = $2
WHERE id = $1::uuid`, exhaustedRunID, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if exhausted, err := store.Claim(ctx, exhaustedRunID, reportID); exhausted != nil || !errors.Is(err, ErrRunReclaimExhausted) {
		t.Fatalf("exhausted claim = %#v, %v", exhausted, err)
	}
	var errorCode, savedStatus string
	if err := pool.QueryRow(ctx, `
SELECT run.status, run.error, run.execution_reclaim_count, report.last_run_status
FROM report_runs AS run
JOIN saved_reports AS report ON report.id = run.report_id
WHERE run.id = $1::uuid`, exhaustedRunID).Scan(
		&status, &errorCode, &reclaimCount, &savedStatus,
	); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || errorCode != reclaimExhaustedCode ||
		reclaimCount != maxExecutionReclaims || savedStatus != "failed" {
		t.Fatalf("exhausted state status=%s error=%s reclaims=%d saved=%s",
			status, errorCode, reclaimCount, savedStatus)
	}
	if next := readReportNextRunAt(t, ctx, pool, jobID); next != nil {
		t.Fatalf("scheduled exhaustion left next_run_at = %v, want NULL", next)
	}
	if repeated, err := store.Claim(ctx, exhaustedRunID, reportID); repeated != nil || !errors.Is(err, ErrRunReclaimExhausted) {
		t.Fatalf("repeated exhausted claim = %#v, %v", repeated, err)
	}
	metrics := collector.PrometheusText()
	for _, sample := range []string{
		`worker_report_run_lease_expired_total{result="retrying"} 4`,
		`worker_report_run_lease_expired_total{result="failed"} 1`,
	} {
		if !strings.Contains(metrics, sample) {
			t.Fatalf("missing reclaim metric %q:\n%s", sample, metrics)
		}
	}
}

func createReportTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE scheduled_jobs (
	id uuid PRIMARY KEY,
	next_run_at timestamptz NULL,
	updated_at timestamptz NOT NULL
);
CREATE TABLE saved_reports (
	id uuid PRIMARY KEY,
	org_id text NOT NULL,
	report_plan jsonb NULL,
	parameters jsonb NULL,
	is_active boolean NOT NULL,
	last_run_at timestamptz NULL,
	last_run_status text NULL,
	updated_at timestamptz NOT NULL
);
CREATE TABLE scheduled_report_occurrences (
	occurrence_id text PRIMARY KEY,
	report_id uuid NOT NULL REFERENCES saved_reports(id),
	scheduled_job_id uuid NOT NULL REFERENCES scheduled_jobs(id),
	scheduled_for timestamptz NOT NULL
);
CREATE TABLE report_runs (
	id uuid PRIMARY KEY,
	report_id uuid NOT NULL REFERENCES saved_reports(id),
	scheduled_occurrence_id text NULL REFERENCES scheduled_report_occurrences(occurrence_id),
	status text NOT NULL,
	started_at timestamptz NULL,
	completed_at timestamptz NULL,
	duration_seconds double precision NULL,
	rendered_markdown text NULL,
	artifact_url text NULL,
	provenance_records json NULL,
	error text NULL,
	error_traceback text NULL,
	attempt_count integer NOT NULL,
	artifact_fingerprint text NULL,
	execution_claim_token uuid NULL,
	execution_lease_expires_at timestamptz NULL,
	execution_reclaim_count integer NOT NULL DEFAULT 0,
	notification_key text NULL UNIQUE,
	notification_status text NOT NULL,
	notification_sent_at timestamptz NULL,
	notification_claim_token uuid NULL,
	notification_lease_expires_at timestamptz NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

func readReportNextRunAt(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	jobID string,
) *time.Time {
	t.Helper()
	var next *time.Time
	if err := pool.QueryRow(ctx, `
SELECT next_run_at FROM scheduled_jobs WHERE id = $1::uuid`, jobID).Scan(&next); err != nil {
		t.Fatal(err)
	}
	return next
}
