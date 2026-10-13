//go:build integration

package fixturescli

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// runRepoComplexity writes the daily complexity history of every repository the world's syncs wrote
// (demoRepoComplexity), for the days the world's syncs read. The world holds no file text, so there is no
// complexity job to run over it.
func runRepoComplexity(ctx context.Context, t *testing.T, conn driver.Conn, world providerWorld) {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT id FROM repos WHERE org_id = ? ORDER BY id", world.OrgID)
	if err != nil {
		t.Fatal(err)
	}
	var repoIDs []uuid.UUID
	for rows.Next() {
		var repoID uuid.UUID
		if err := rows.Scan(&repoID); err != nil {
			t.Fatal(err)
		}
		repoIDs = append(repoIDs, repoID)
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
		for _, row := range demoRepoComplexity(repoID.String(), world.FrozenAt, demoDepthDays) {
			day, err := time.Parse("2006-01-02", row.Day)
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
	t.Logf("repo_complexity: %d repositories, %d days each", len(repoIDs), demoDepthDays)
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
	last := demoInstant.UTC().Truncate(24 * time.Hour)
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
