//go:build integration

package icfinalize

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL; v2.49 left the Go zero value. Both read-backs keep that zero.
func TestReadBacksScanNullColumnsAsTheZeroValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	const org = "00000000-0000-4000-8000-000000014218"
	day := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	computedAt := day.Add(30 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000142b1")
	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, 'a@example.com', 'a@example.com', NULL, NULL, 1, 5, 1, ?, ?)`, repo, day, computedAt, org); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
    (day, provider, work_scope_id, user_identity, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
     cycle_time_p50_hours, cycle_time_p90_hours, computed_at, org_id)
    VALUES (?, 'jira', 'ENGPROJ', 'a@example.com', 'ENG', 'Eng', 1, 1, 1, NULL, NULL, ?, ?)`, day, computedAt, org); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(conn)

	t.Run("loadGitMetrics", func(t *testing.T) {
		rows, err := executor.loadGitMetrics(ctx, org, day, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].TeamID != "" || rows[0].TeamName != "" {
			t.Fatalf("rows = %+v, want one row with an empty team id and name", rows)
		}
	})
	t.Run("loadWorkItemMetrics", func(t *testing.T) {
		rows, err := executor.loadWorkItemMetrics(ctx, org, day, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].CycleTimeP50Hrs != 0 || rows[0].CycleTimeP90Hrs != 0 {
			t.Fatalf("rows = %+v, want one row with zero cycle time p50 and p90", rows)
		}
	})
}
