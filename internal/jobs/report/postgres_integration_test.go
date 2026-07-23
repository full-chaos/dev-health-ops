//go:build integration

package report

import (
	"context"
	"errors"
	"testing"
	"time"

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
		reportID = "00000000-0000-4000-8000-000000000002"
		runID    = "00000000-0000-4000-8000-000000000001"
	)
	if _, err := pool.Exec(ctx, `
INSERT INTO saved_reports (id, org_id, report_plan, parameters, is_active, last_run_status, updated_at)
VALUES ($1, 'org-1',
 '{"plan_id":"plan-1","report_type":"weekly_health","org_id":"org-1","sections":["summary"]}'::jsonb,
 '{}'::jsonb, TRUE, NULL, NOW())`, reportID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, attempt_count, notification_status)
VALUES ($1, $2, 'pending', 0, 'pending')`, runID, reportID); err != nil {
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
	claimed, err := store.Claim(ctx, runID, reportID)
	if err != nil || !claimed {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	claimed, err = store.Claim(ctx, runID, reportID)
	if err != nil || claimed {
		t.Fatalf("duplicate claim = %v, %v", claimed, err)
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
	completed, err := store.Complete(ctx, runID, artifact)
	if err != nil || !completed {
		t.Fatalf("complete = %v, %v", completed, err)
	}
	completed, err = store.Complete(ctx, runID, artifact)
	if err != nil || completed {
		t.Fatalf("identical retry = %v, %v", completed, err)
	}
	conflict := artifact
	conflict.Fingerprint = "sha256:different"
	if _, err := store.Complete(ctx, runID, conflict); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting retry error = %v", err)
	}
	key, notify, err := store.ClaimNotification(ctx, runID)
	if err != nil || !notify || key != "report.ready:"+runID {
		t.Fatalf("notification claim = %q, %v, %v", key, notify, err)
	}
	if err := store.CompleteNotification(ctx, runID); err != nil {
		t.Fatal(err)
	}
	_, notify, err = store.ClaimNotification(ctx, runID)
	if err != nil || notify {
		t.Fatalf("notification duplicate = %v, %v", notify, err)
	}

	const canceledRunID = "00000000-0000-4000-8000-000000000003"
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, attempt_count, notification_status)
VALUES ($1, $2, 'canceled', 0, 'pending')`, canceledRunID, reportID); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.Claim(ctx, canceledRunID, reportID)
	if err != nil || claimed {
		t.Fatalf("canceled claim = %v, %v", claimed, err)
	}

	const retryRunID = "00000000-0000-4000-8000-000000000004"
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, attempt_count, notification_status)
VALUES ($1, $2, 'pending', 0, 'pending')`, retryRunID, reportID); err != nil {
		t.Fatal(err)
	}
	if claimed, err = store.Claim(ctx, retryRunID, reportID); err != nil || !claimed {
		t.Fatalf("retry run first claim = %v, %v", claimed, err)
	}
	if err := store.Fail(ctx, retryRunID, "query_failed"); err != nil {
		t.Fatal(err)
	}
	if claimed, err = store.Claim(ctx, retryRunID, reportID); err != nil || !claimed {
		t.Fatalf("failed run retry claim = %v, %v", claimed, err)
	}
}

func createReportTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
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
CREATE TABLE report_runs (
	id uuid PRIMARY KEY,
	report_id uuid NOT NULL REFERENCES saved_reports(id),
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
	notification_key text NULL UNIQUE,
	notification_status text NOT NULL,
	notification_sent_at timestamptz NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}
