//go:build integration

package fixturescli

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// o4WindowDays is the fact oracle's analytics window: 31 days that end on the capture day.
const o4WindowDays = 31

// TestDemoWorldHoldsTheO4OracleShapes loads the demo world, categorizes it with the mock categorizer and
// projects the work unit membership with the production jobs (the venue's derive chain plus the daily
// membership job), then reads the shapes the acr O4 fact oracle needs inside its 31-day window.
func TestDemoWorldHoldsTheO4OracleShapes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ch := startClickHouse(t)
	requireUTCServer(t, ch.httpDSN)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	world, err := LoadFrozenWorld(demoWorld.Params)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := LoadWorld(ctx, conn, world, demoWorld.OrgID, now); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	prCommitLoader, err := prcommit.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	prCommitWriter, err := prcommit.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	prCommitService, err := prcommit.NewService(prCommitLoader, prCommitWriter, conn, logger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prCommitService.ProduceLinks(ctx, demoWorld.OrgID, prcommit.Window{}); err != nil {
		t.Fatal(err)
	}
	if _, err := prCommitService.ProduceEdges(ctx, demoWorld.OrgID, prcommit.Window{}); err != nil {
		t.Fatal(err)
	}
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := investment.NewMaterializer(reader, writer, categorize.MockProvider{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializer.Run(ctx, investment.Config{
		OrgID: demoWorld.OrgID, FromTS: now.AddDate(0, 0, -demoDepthDays), ToTS: now,
		RunID: "demo-world-o4-shapes", ComputedAt: now, ProviderName: "mock",
	}); err != nil {
		t.Fatalf("materializer: %v", err)
	}
	membershipWriter, err := remaining.NewMembershipClickHouseWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := remaining.NewMembershipExecutor(ctx, conn, membershipWriter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := membership.ComputeOrg(ctx, demoWorld.OrgID, nil, now); err != nil {
		t.Fatalf("membership: %v", err)
	}
	var runID string
	if err := conn.QueryRow(ctx, `SELECT argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id = ?`, demoWorld.OrgID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if runID == "" {
		t.Fatal("the membership job published no run")
	}
	windowStart := now.AddDate(0, 0, -(o4WindowDays - 1))
	units := func(statement string, args ...any) []string {
		t.Helper()
		rows, err := conn.Query(ctx, statement, args...)
		if err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(out)
		return out
	}
	const inScope = `work_unit_id IN (SELECT work_unit_id FROM work_unit_membership WHERE org_id = ? AND run_id = ?)`
	const sharedOwner = `repo_id IN (SELECT repo_id FROM team_repo_ownership FINAL WHERE org_id = ? AND valid_to IS NULL AND repo_id IS NOT NULL AND share IS NOT NULL)`

	t.Run("ScopedUnitOnAnOwnedRepository", func(t *testing.T) {
		found := units(`SELECT DISTINCT work_unit_id FROM work_unit_repo_effort FINAL
WHERE org_id = ? AND effort_value > 0 AND `+sharedOwner+` AND `+inScope+`
  AND work_unit_id IN (SELECT work_unit_id FROM work_unit_investments FINAL WHERE org_id = ? AND to_ts >= ?)`,
			demoWorld.OrgID, demoWorld.OrgID, demoWorld.OrgID, runID, demoWorld.OrgID, windowStart)
		t.Logf("%d in-scope work units with effort on a team-owned repository in the last %d days", len(found), o4WindowDays)
		if len(found) == 0 {
			t.Errorf("no work unit of the membership run has effort on a team-owned repository in the last %d days", o4WindowDays)
		}
	})

	t.Run("EvidenceGatedUnitInScope", func(t *testing.T) {
		found := units(`SELECT DISTINCT work_unit_id FROM work_unit_investments FINAL
WHERE org_id = ? AND categorization_status IN (?, ?) AND to_ts >= ? AND `+sharedOwner+` AND `+inScope,
			demoWorld.OrgID, categorize.StatusInsufficientChars, categorize.StatusNoTextSources, windowStart,
			demoWorld.OrgID, demoWorld.OrgID, runID)
		t.Logf("%d evidence-gated in-scope work units on a team-owned repository in the last %d days: %v", len(found), o4WindowDays, found)
		if len(found) == 0 {
			t.Errorf("no evidence-gated work unit of the membership run is on a team-owned repository in the last %d days", o4WindowDays)
		}
	})
}

// A team has a work scope whose newest estimate-coverage day is older than the team's latest day. A
// scope (here: a repository and the work items on it) has a coverage row on a day while one of its items
// is open on that day or completes on it (jobs/metrics/daily work_item_scope_read.go), so its newest day
// is the as-of day while any item is open, else the day its last item completed. The scope must end
// inside the oracle's window, and before the team's latest day.
func TestDemoGateATeamHasACoverageKeyThatEndsBeforeItsLatestDay(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	asOf := gateDay(frozenAt)
	windowStart := gateDay(frozenAt.AddDate(0, 0, -(o4WindowDays - 1)))
	latest := map[string]map[string]any{}
	for _, row := range demoGateRows(world, "work_items") {
		id := gateText(row["work_item_id"])
		if held, ok := latest[id]; ok && gateText(held["last_synced"]) >= gateText(row["last_synced"]) {
			continue
		}
		latest[id] = row
	}
	newest := map[string]string{}
	for _, row := range latest {
		repoID := gateText(row["repo_id"])
		if repoID == "" {
			continue
		}
		day := asOf
		if gateText(row["status"]) == "done" {
			completed, ok := gateTime(row["completed_at"])
			if !ok {
				continue
			}
			day = gateDay(completed)
		}
		if day > newest[repoID] {
			newest[repoID] = day
		}
	}
	teamRepos := map[string][]string{}
	for repoID, owners := range demoGateRepoOwners(world) {
		for team := range owners {
			teamRepos[team] = append(teamRepos[team], repoID)
		}
	}
	found := 0
	for team, repos := range teamRepos {
		teamLatest := ""
		for _, repoID := range repos {
			if newest[repoID] > teamLatest {
				teamLatest = newest[repoID]
			}
		}
		for _, repoID := range repos {
			day := newest[repoID]
			if day != "" && day >= windowStart && day < teamLatest {
				t.Logf("team %s: repository %s ends on %s, the team's latest day is %s", team, repoID, day, teamLatest)
				found++
			}
		}
	}
	if found == 0 {
		t.Errorf("no team has a repository whose newest coverage day is inside the last %d days and older than the team's latest day", o4WindowDays)
	}
}
