//go:build integration

package daily

// The daily-side readers of the four pooled daily tables read a retired repository-day (newest row retracted = 1) as absent:
// the zero-row check counts it as no output of deploy and incident, and the compounding risk read of repo_metrics_daily
// gives no row for it (never the marker row's column defaults as a measurement).

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/compoundingrisk"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestDailyReadersReadARetiredPooledDayAsAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	const (
		org  = "14148000-0000-4000-8000-0000000000c0"
		repo = "14148000-0000-4000-8000-0000000000c1"
		day  = "2026-01-10"
	)
	repoID := uuid.MustParse(repo)

	checker := &ClickHouseSourceDataChecker{conn: conn}
	flagged := func(label string) []string {
		t.Helper()
		families, err := checker.ZeroRowFamilies(ctx, SourceSnapshot{
			orgID: org, day: day, repositoryIDs: []uuid.UUID{repoID}, deploy: true, incident: true,
		})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		t.Logf("ZEROROW14148 %-11s flagged=%v", label, families)
		return families
	}
	exec(`INSERT INTO deploy_metrics_daily (org_id, repo_id, day, deployments_count, computed_at) VALUES (?, ?, ?, 1, '2026-01-10 12:00:00')`, org, repo, day)
	exec(`INSERT INTO incident_metrics_daily (org_id, repo_id, day, incidents_count, computed_at) VALUES (?, ?, ?, 1, '2026-01-10 12:00:00')`, org, repo, day)
	if got := flagged("live rows"); len(got) != 0 {
		t.Fatalf("live rows: flagged %v, want none", got)
	}
	exec(`INSERT INTO deploy_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES (?, ?, ?, '2026-01-10 12:00:01', 1)`, org, repo, day)
	exec(`INSERT INTO incident_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES (?, ?, ?, '2026-01-10 12:00:01', 1)`, org, repo, day)
	if got := flagged("retired day"); !slices.Contains(got, "deploy") || !slices.Contains(got, "incident") {
		t.Errorf("retired day (newest rows retracted, older live rows under them): flagged %v, want deploy and incident", got)
	}

	loader, err := compoundingrisk.NewClickHouseLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	dayTime, _ := time.Parse(time.DateOnly, day)
	exec(`INSERT INTO repo_metrics_daily (org_id, repo_id, day, rework_churn_ratio_30d, bus_factor, computed_at)
VALUES (?, ?, ?, 0.5, 3, '2026-01-10 12:00:00')`, org, repo, day)
	readBoth := func(label string) (partition, orgWide int) {
		t.Helper()
		rows, err := loader.LoadRepoMetrics(ctx, org, []uuid.UUID{repoID}, dayTime)
		if err != nil {
			t.Fatal(err)
		}
		all, err := loader.LoadRepoMetricsForOrgDay(ctx, org, dayTime)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RISK14148 %-11s partition rows=%d org rows=%d (%+v)", label, len(rows), len(all), rows)
		return len(rows), len(all)
	}
	if partition, orgWide := readBoth("live row"); partition != 1 || orgWide != 1 {
		t.Fatalf("live row: %d / %d rows, want 1 / 1", partition, orgWide)
	}
	exec(`INSERT INTO repo_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES (?, ?, ?, '2026-01-10 12:00:01', 1)`, org, repo, day)
	if partition, orgWide := readBoth("retired day"); partition != 0 || orgWide != 0 {
		t.Errorf("retired day: compounding risk reads %d / %d rows, want none (the marker row is not a measurement)", partition, orgWide)
	}
}
