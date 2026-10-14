//go:build integration

package fixturescli

import (
	"context"
	"encoding/json"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// runRepoComplexity runs the complexity job (remaining.ComplexityExecutor, the executor the worker runs)
// over the files the world's files syncs wrote, for the as-of day, on the world's clock: it writes
// file_complexity_snapshots and that day's repo_complexity_daily row of every repository. The days before
// it hold no tree to scan (the world syncs the files once), so each repository's earlier history is
// seeded to drift into the level the job measured (demoRepoComplexity).
func runRepoComplexity(ctx context.Context, t *testing.T, conn driver.Conn, world providerWorld) {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	executor, err := remaining.NewComplexityExecutorAt(ctx, conn, filepath.Join(root, "src", "dev_health_ops", "config", "complexity.yaml"), nil,
		func() time.Time { return world.FrozenAt })
	if err != nil {
		t.Fatal(err)
	}
	asOf := world.FrozenAt.UTC().Format(time.DateOnly)
	scope, err := json.Marshal(map[string]any{"version": 1, "day": asOf, "backfill_days": 1})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := executor.ComputePartition(ctx, remaining.Run{OrganizationID: world.OrgID}, remaining.Partition{ID: "demo-complexity", Scope: scope})
	if err != nil {
		t.Fatalf("complexity: %v", err)
	}
	rows, err := conn.Query(ctx, `SELECT r.id, r.repo, c.loc_total, c.cyclomatic_total, c.cyclomatic_per_kloc, c.high_complexity_functions, c.very_high_complexity_functions
		FROM repos AS r LEFT JOIN (SELECT * FROM repo_complexity_daily WHERE org_id = ? AND day = toDate(?)) AS c ON c.repo_id = r.id
		WHERE r.org_id = ? ORDER BY r.id`, world.OrgID, asOf, world.OrgID)
	if err != nil {
		t.Fatal(err)
	}
	measured := map[uuid.UUID]demoComplexityDay{}
	repoNames := map[uuid.UUID]string{}
	var repoIDs []uuid.UUID
	for rows.Next() {
		var repoID uuid.UUID
		var repoName string
		day := demoComplexityDay{Day: asOf}
		if err := rows.Scan(&repoID, &repoName, &day.LOCTotal, &day.CyclomaticTotal, &day.CyclomaticPerKLOC, &day.HighComplexityFunctions, &day.VeryHighComplexityFunc); err != nil {
			t.Fatal(err)
		}
		if day.LOCTotal == 0 {
			t.Fatalf("complexity: repository %s has no measured complexity on %s: its files sync wrote no Go file", repoID, asOf)
		}
		repoIDs = append(repoIDs, repoID)
		measured[repoID] = day
		repoNames[repoID] = repoName
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(repoIDs) == 0 {
		t.Fatal("repo_complexity: the world's syncs wrote no repository")
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO repo_complexity_daily (
		repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc,
		high_complexity_functions, very_high_complexity_functions, computed_at, org_id
	)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, repoID := range repoIDs {
		for _, row := range demoRepoComplexity(path.Base(repoNames[repoID]), measured[repoID], demoDepthDays-1) {
			day, err := time.Parse(time.DateOnly, row.Day)
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Append(repoID, day, row.LOCTotal, row.CyclomaticTotal, row.CyclomaticPerKLOC,
				row.HighComplexityFunctions, row.VeryHighComplexityFunc, world.FrozenAt, world.OrgID); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	t.Logf("complexity: %+v; repo_complexity: %d repositories, %d seeded days each before %s", outcome, len(repoIDs), demoDepthDays-1, asOf)
}

// TestDemoWorldHoldsComplexityHistoryForEveryRepository: a risk score's complexity part is the change
// of cyclomatic_per_kloc over the last 30 days, which needs a row in both halves of that window. A
// repository without them has no complexity part, which is what the world held before.
func TestDemoWorldHoldsComplexityHistoryForEveryRepository(t *testing.T) {
	world, err := LoadFrozenWorld(demoWorld.Params)
	if err != nil {
		t.Fatal(err)
	}
	var repos, complexity *WorldTable
	for index := range world.Tables {
		switch world.Tables[index].Name {
		case "repos":
			repos = &world.Tables[index]
		case "repo_complexity_daily":
			complexity = &world.Tables[index]
		}
	}
	if repos == nil || complexity == nil {
		t.Fatalf("the demo world holds repos=%v repo_complexity_daily=%v", repos != nil, complexity != nil)
	}
	frozenAt, err := time.Parse(time.RFC3339Nano, world.FrozenAt)
	if err != nil {
		t.Fatal(err)
	}
	last := frozenAt.UTC().Truncate(24 * time.Hour)
	midpoint := last.AddDate(0, 0, -29+15).Format("2006-01-02")
	start := last.AddDate(0, 0, -29).Format("2006-01-02")
	firstHalf, secondHalf := map[string]int{}, map[string]int{}
	for _, row := range complexity.Rows {
		repoID, day := row[0].(string), row[1].(string)
		switch {
		case day >= midpoint:
			secondHalf[repoID]++
		case day >= start:
			firstHalf[repoID]++
		}
	}
	for _, row := range repos.Rows {
		repoID := row[0].(string)
		if firstHalf[repoID] == 0 || secondHalf[repoID] == 0 {
			t.Errorf("repository %s holds %d days in the first and %d in the second half of the last 30 days", repoID, firstHalf[repoID], secondHalf[repoID])
		}
	}
}
