//go:build integration

package report

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	goldenReportID         = "00000000-0000-4000-8000-000000000002"
	goldenOnDemandRunID    = "00000000-0000-4000-8000-000000000011"
	goldenScheduledRunID   = "00000000-0000-4000-8000-000000000012"
	goldenOrganizationID   = "org-1"
	goldenCorrelationID    = "00000000-0000-4000-8000-0000000000c0"
	goldenOnDemandIdemKey  = "report.execute_on_demand:00000000-0000-4000-8000-000000000011"
	goldenScheduledIdemKey = "report.execute_scheduled:00000000-0000-4000-8000-000000000012"
)

// goldenReportPlan is the saved_reports.report_plan that reproduces the Python
// golden artifact: the loader plus the ClickHouse adapter must rebuild exactly
// the QueryResult asserted by TestRendererAndArtifactMatchPythonGoldens. The
// pinned created_at keeps the rendered footer deterministic, and the chart spec
// deliberately carries no window of its own so the seeded rows alone decide the
// series.
const goldenReportPlan = `{
	"plan_id": "plan-1",
	"report_type": "weekly_health",
	"audience": "team_lead",
	"scope_teams": ["team-a"],
	"scope_repos": ["repo-a"],
	"time_range_start": "2026-01-01",
	"time_range_end": "2026-01-07",
	"comparison_period": "prior_week",
	"sections": ["summary", "quality", "testops"],
	"requested_metrics": ["success_rate"],
	"confidence_threshold": "direct_fact",
	"created_at": "2026-01-08T00:00:00Z",
	"org_id": "org-1",
	"chart_specs": [{
		"chart_id": "chart-1",
		"plan_id": "plan-1",
		"chart_type": "line",
		"metric": "success_rate",
		"group_by": "day",
		"title": "Success rate",
		"org_id": "org-1"
	}]
}`

// TestReportRuntimeExecutesBothKindsAgainstRealStores drives the production
// dependency graph — PostgreSQL run store and loader, ClickHouse query adapter,
// deterministic renderer, SHA-256 artifact adapter — through both independently
// routed report kinds, and proves the runtime reproduces the Python goldens and
// stays a bounded no-op on replay.
func TestReportRuntimeExecutesBothKindsAgainstRealStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool := startReportPostgres(t, ctx)
	conn := startReportClickHouse(t, ctx)
	createReportTables(t, ctx, pool)
	seedGoldenReport(t, ctx, pool)
	seedGoldenMetrics(t, ctx, conn)

	dependencies, err := NewProductionDependencies(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	// The run row records only the final notification state, so a repeated
	// delivery is invisible in PostgreSQL alone; counting wraps the real
	// adapter without weakening its contract checks.
	notifications := &countingNotificationAdapter{delegate: dependencies.Notifications}
	dependencies.Notifications = notifications

	onDemand := NewOnDemandHandler(dependencies)
	scheduled := NewScheduledHandler(dependencies)
	golden := loadReportGolden(t)

	if err := onDemand.Work(ctx, onDemandExecution(goldenOnDemandRunID)); err != nil {
		t.Fatalf("on-demand execution: %v", err)
	}
	if err := scheduled.Work(ctx, scheduledExecution(goldenScheduledRunID)); err != nil {
		t.Fatalf("scheduled execution: %v", err)
	}

	firstOnDemand := loadReportRun(t, ctx, pool, goldenOnDemandRunID)
	firstScheduled := loadReportRun(t, ctx, pool, goldenScheduledRunID)
	assertMatchesGolden(t, "report.execute_on_demand", goldenOnDemandRunID, firstOnDemand, golden)
	assertMatchesGolden(t, "report.execute_scheduled", goldenScheduledRunID, firstScheduled, golden)
	if notifications.calls != 2 {
		t.Fatalf("notifications after first execution = %d, want 2", notifications.calls)
	}

	// A redelivered job must not re-render, re-persist, or re-notify: the claim
	// CAS rejects a completed run and the delivered notification is unclaimable.
	if err := onDemand.Work(ctx, onDemandExecution(goldenOnDemandRunID)); err != nil {
		t.Fatalf("on-demand replay: %v", err)
	}
	if err := scheduled.Work(ctx, scheduledExecution(goldenScheduledRunID)); err != nil {
		t.Fatalf("scheduled replay: %v", err)
	}
	if notifications.calls != 2 {
		t.Fatalf("notifications after replay = %d, want 2", notifications.calls)
	}
	assertUnchanged(t, "report.execute_on_demand", firstOnDemand,
		loadReportRun(t, ctx, pool, goldenOnDemandRunID))
	assertUnchanged(t, "report.execute_scheduled", firstScheduled,
		loadReportRun(t, ctx, pool, goldenScheduledRunID))
}

func startReportPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func startReportClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close ClickHouse connection: %v", err)
		}
	})
	return conn
}

func seedGoldenReport(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO saved_reports (id, org_id, report_plan, parameters, is_active, last_run_status, updated_at)
VALUES ($1::uuid, $2, $3::jsonb, '{}'::jsonb, TRUE, NULL, NOW())`,
		goldenReportID, goldenOrganizationID, goldenReportPlan); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, attempt_count, notification_status)
VALUES ($1::uuid, $3::uuid, 'pending', 0, 'pending'),
       ($2::uuid, $3::uuid, 'pending', 0, 'pending')`,
		goldenOnDemandRunID, goldenScheduledRunID, goldenReportID); err != nil {
		t.Fatal(err)
	}
}

func seedGoldenMetrics(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	if err := conn.Exec(ctx, `
CREATE TABLE cicd_metrics_daily (
	org_id String,
	repo_id String,
	day Date,
	success_rate Nullable(Float64)
) ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`); err != nil {
		t.Fatal(err)
	}
	// Only the first row may reach the golden series; the null sample and the
	// foreign tenant prove the adapter's IS NOT NULL and org_id clauses are
	// what keep the rendered chart to a single point.
	if err := conn.Exec(ctx, `
INSERT INTO cicd_metrics_daily VALUES
('org-1', 'repo-a', '2026-01-01', 0.95),
('org-1', 'repo-a', '2026-01-02', NULL),
('other-org', 'repo-a', '2026-01-03', 0.10)`); err != nil {
		t.Fatal(err)
	}
}

type reportGolden struct {
	markdown    string
	fingerprint string
	provenance  []ProvenanceRecord
}

func loadReportGolden(t *testing.T) reportGolden {
	t.Helper()
	markdown, err := os.ReadFile(filepath.Join("testdata", "weekly_health.golden.md"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join("testdata", "weekly_health.metadata.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var metadata struct {
		Fingerprint string             `json:"fingerprint"`
		Provenance  []ProvenanceRecord `json:"provenance"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	return reportGolden{
		markdown: string(markdown), fingerprint: metadata.Fingerprint, provenance: metadata.Provenance,
	}
}

type persistedReportRun struct {
	status             string
	markdown           string
	fingerprint        string
	provenance         []ProvenanceRecord
	attempts           int
	completedAt        time.Time
	notificationKey    string
	notificationStatus string
	notificationSentAt time.Time
}

func loadReportRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) persistedReportRun {
	t.Helper()
	var run persistedReportRun
	var markdown, fingerprint, notificationKey *string
	var provenance []byte
	var completedAt, notificationSentAt *time.Time
	err := pool.QueryRow(ctx, `
SELECT status, rendered_markdown, artifact_fingerprint, provenance_records, attempt_count,
       completed_at, notification_key, notification_status, notification_sent_at
FROM report_runs WHERE id = $1::uuid`, runID).
		Scan(&run.status, &markdown, &fingerprint, &provenance, &run.attempts,
			&completedAt, &notificationKey, &run.notificationStatus, &notificationSentAt)
	if err != nil {
		t.Fatal(err)
	}
	if markdown == nil || fingerprint == nil || notificationKey == nil ||
		completedAt == nil || notificationSentAt == nil {
		t.Fatalf("run %s left required artifact columns unset: %#v", runID, run)
	}
	if err := json.Unmarshal(provenance, &run.provenance); err != nil {
		t.Fatal(err)
	}
	run.markdown, run.fingerprint, run.notificationKey = *markdown, *fingerprint, *notificationKey
	run.completedAt, run.notificationSentAt = *completedAt, *notificationSentAt
	return run
}

func assertMatchesGolden(t *testing.T, kind, runID string, run persistedReportRun, golden reportGolden) {
	t.Helper()
	if run.status != "success" {
		t.Fatalf("%s status = %s, want success", kind, run.status)
	}
	if run.markdown != golden.markdown {
		t.Fatalf("%s markdown drifted from Python golden:\n%s", kind, run.markdown)
	}
	if run.fingerprint != golden.fingerprint {
		t.Fatalf("%s fingerprint = %s, want %s", kind, run.fingerprint, golden.fingerprint)
	}
	if got, want := mustJSON(t, run.provenance), mustJSON(t, golden.provenance); got != want {
		t.Fatalf("%s provenance drifted:\ngot  %s\nwant %s", kind, got, want)
	}
	if run.notificationStatus != "delivered" {
		t.Fatalf("%s notification status = %s, want delivered", kind, run.notificationStatus)
	}
	// One claim per run: the artifact was produced by exactly one execution.
	if run.attempts != 1 {
		t.Fatalf("%s attempt_count = %d, want 1", kind, run.attempts)
	}
	if run.notificationKey != "report.ready:"+runID {
		t.Fatalf("%s notification key = %s", kind, run.notificationKey)
	}
}

func assertUnchanged(t *testing.T, kind string, before, after persistedReportRun) {
	t.Helper()
	if before.fingerprint != after.fingerprint || before.markdown != after.markdown {
		t.Fatalf("%s replay rewrote the stored artifact", kind)
	}
	if !before.completedAt.Equal(after.completedAt) {
		t.Fatalf("%s replay re-completed the run: %s -> %s", kind, before.completedAt, after.completedAt)
	}
	if before.attempts != after.attempts {
		t.Fatalf("%s replay re-claimed the run: attempts %d -> %d", kind, before.attempts, after.attempts)
	}
	if !before.notificationSentAt.Equal(after.notificationSentAt) ||
		after.notificationStatus != "delivered" {
		t.Fatalf("%s replay redelivered the notification: %s at %s",
			kind, after.notificationStatus, after.notificationSentAt)
	}
}

func onDemandExecution(runID string) *jobruntime.Execution[jobruntime.OnDemandReportExecutionArgs] {
	args := jobruntime.OnDemandReportExecutionArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.OnDemandReportExecutionPayload]{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  organizationScope(),
			CorrelationID:   goldenCorrelationID,
			IdempotencyKey:  goldenOnDemandIdemKey,
			Domain:          jobcontract.DomainLink{Type: "report_run", ID: runID},
			Payload:         jobcontract.OnDemandReportExecutionPayload{ReportID: goldenReportID},
		},
	}
	return &jobruntime.Execution[jobruntime.OnDemandReportExecutionArgs]{
		Args: args, Envelope: args.ContractEnvelope(), CorrelationID: goldenCorrelationID,
	}
}

func scheduledExecution(runID string) *jobruntime.Execution[jobruntime.ScheduledReportExecutionArgs] {
	args := jobruntime.ScheduledReportExecutionArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ScheduledReportExecutionPayload]{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  organizationScope(),
			CorrelationID:   goldenCorrelationID,
			IdempotencyKey:  goldenScheduledIdemKey,
			Domain:          jobcontract.DomainLink{Type: "report_run", ID: runID},
			Payload:         jobcontract.ScheduledReportExecutionPayload{ReportID: goldenReportID},
		},
	}
	return &jobruntime.Execution[jobruntime.ScheduledReportExecutionArgs]{
		Args: args, Envelope: args.ContractEnvelope(), CorrelationID: goldenCorrelationID,
	}
}

func organizationScope() *string {
	organization := goldenOrganizationID
	return &organization
}

type countingNotificationAdapter struct {
	delegate NotificationAdapter
	calls    int
}

func (adapter *countingNotificationAdapter) Notify(ctx context.Context, reportID, key string) error {
	adapter.calls++
	return adapter.delegate.Notify(ctx, reportID, key)
}
