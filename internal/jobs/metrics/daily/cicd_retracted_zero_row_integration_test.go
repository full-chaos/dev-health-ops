//go:build integration

package daily

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The zero-row check reads a cicd repository-day as output only when the newest row of its key is live: a key whose
// newest row holds retracted = 1 is no output, so a day with source runs and only a retired row is flagged. An older
// live row under the retraction row does not count, and a live row newer than the retraction row counts again.
func TestZeroRowCheckReadsARetiredCICDDayAsNoOutput(t *testing.T) {
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
		org  = "14016000-0000-4000-8000-0000000000f0"
		repo = "14016000-0000-4000-8000-0000000000f1"
		day  = "2026-01-10"
	)
	checker := &ClickHouseSourceDataChecker{conn: conn}
	flagged := func(label string) bool {
		t.Helper()
		families, err := checker.ZeroRowFamilies(ctx, SourceSnapshot{
			orgID: org, day: day, repositoryIDs: []uuid.UUID{uuid.MustParse(repo)}, cicd: true,
		})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return slices.Contains(families, "cicd")
	}
	if !flagged("no row") {
		t.Fatal("no row: cicd not flagged, want flagged")
	}
	exec(`INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, computed_at)
VALUES (?, ?, ?, 1, 1, '2026-01-10 12:00:00')`, org, repo, day)
	if flagged("live row") {
		t.Error("live row: cicd flagged, want not flagged")
	}
	exec(`INSERT INTO cicd_metrics_daily (org_id, repo_id, day, computed_at, retracted)
VALUES (?, ?, ?, '2026-01-10 12:00:01', 1)`, org, repo, day)
	if !flagged("retired day") {
		t.Error("retired day (newest row retracted, an older live row under it): cicd not flagged, want flagged")
	}
	exec(`INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, computed_at)
VALUES (?, ?, ?, 2, 1, '2026-01-10 12:00:02')`, org, repo, day)
	if flagged("live again") {
		t.Error("a live row newer than the retraction row: cicd flagged, want not flagged")
	}
}
