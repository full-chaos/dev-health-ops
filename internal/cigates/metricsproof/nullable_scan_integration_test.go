//go:build integration

package metricsproof_test

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cigates/metricsproof"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL; v2.49 left the Go zero value. work_item_completion_daily.repo_id is
// Nullable: the unscoped id read keeps the v2.49 empty id, and the scoped reads
// never meet the NULL.
func TestReadbacksOfANullRepoIDKeepTheZeroValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const org = "00000000-0000-4000-8000-000000014301"
	const repo = "00000000-0000-4000-8000-000000014302"
	runStart := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	for _, statement := range []string{
		`INSERT INTO work_item_completion_daily (org_id, repo_id, day, computed_at) VALUES ('` + org + `', NULL, '2026-11-19', '2026-11-19 12:00:00')`,
		`INSERT INTO work_item_completion_daily (org_id, repo_id, day, computed_at) VALUES ('` + org + `', '` + repo + `', '2026-11-19', '2026-11-19 12:00:00')`,
		`INSERT INTO team_metrics_daily (org_id, team_id, day, computed_at) VALUES ('` + org + `', 'team-a', '2026-11-19', '2026-11-19 12:00:00')`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}

	t.Run("UnscopedRepoIDs", func(t *testing.T) {
		ids, err := metricsproof.UnscopedRepoIDs(ctx, conn, "work_item_completion_daily", runStart)
		if err != nil {
			t.Fatal(err)
		}
		if _, empty := ids[""]; !empty || len(ids) != 2 {
			t.Fatalf("ids = %v, want the repo id and the empty id of the NULL row", ids)
		}
	})
	t.Run("FamilyReadback", func(t *testing.T) {
		rows, err := metricsproof.FamilyReadback(ctx, conn, "work_item_completion_daily", org, metricsproof.IDSet{repo: {}}, runStart)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[repo].Rows != 1 {
			t.Fatalf("rows = %v, want one row for the repo", rows)
		}
	})
	t.Run("TeamReadback", func(t *testing.T) {
		rows, err := metricsproof.TeamReadback(ctx, conn, "team_metrics_daily", org, runStart)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows["team-a"].Rows != 1 {
			t.Fatalf("rows = %v, want one row for team-a", rows)
		}
	})
}
