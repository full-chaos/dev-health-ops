//go:build integration

package investment

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTeamOwnershipCascadeEndToEnd is the RED/GREEN proof for CHAOS-5459, and
// it is deliberately written against the PUBLIC path (Materializer.Run over a
// real engine) rather than the pure function, so it compiles and RUNS against
// the pre-fix tree: before this change it fails with
// `allocation_source = "empty"` and a NULL repo_id, because no attribution
// tier in Go -- or in the Python it was ported from
// (materialize.py:1011-1069) -- ever read team_repo_ownership.
//
// The fixture is the exact shape measured in production on org
// 70d529e0-3c06-4597-8480-794fd02328b6 (run c8065d4a8c1648568d369519045747f2,
// 2026-09-07T12:01Z): a Linear issue on a team that owns repositories, with
//
//	no PR or commit edge      -> commit_churn and pr_churn tiers find nothing
//	no parent_id              -> the ancestor walk has nothing to climb
//	no child naming it parent -> the children tier has nothing to collect
//
// 767 of 1365 units in that run ended exactly here, 767/767 of them Linear
// issues on one team owning six repositories. The data held the link; the
// computation never read it.
func TestTeamOwnershipCascadeEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

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

	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	const teamID = "CHAOS"
	// ownedRepo is the MANUAL primary row. broaderRepo is an `inferred` row on
	// the same team -- present so the pick is a real contest, not a
	// single-candidate walkover: a bug that returned "whatever row came back
	// first" would flip between them run to run and this test would catch it.
	ownedRepo := "11111111-1111-4111-8111-111111111111"
	broaderRepo := "22222222-2222-4222-8222-222222222222"

	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	within := windowStart.Add(24 * time.Hour)

	seedTeam(t, ctx, conn, teamID, "linear", teamID, within)
	seedTeamRepoOwnership(t, ctx, conn, teamID, broaderRepo, 0, 10, 0, within)
	seedTeamRepoOwnership(t, ctx, conn, teamID, ownedRepo, 1, 100, 0, within)

	// S1/S1b: the component under test. Their only edge is issue->issue with
	// NO repo_id, so collectSingleRepoID resolves nothing, and neither carries
	// a parent_id.
	seedTeamWorkItem(t, ctx, conn, "S1", "", teamID, within)
	seedTeamWorkItem(t, ctx, conn, "S1b", "", teamID, within)
	seedIssueEdge(t, ctx, conn, "S1", "S1b", "", within)

	stats, err := materializer.Run(ctx, Config{
		OrgID: hierarchyCascadeTestOrg, FromTS: windowStart, ToTS: windowEnd,
		RunID: "team-ownership-e2e-run", ComputedAt: within, ProviderName: "mock",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.RepoCascadeTeamOwnership == 0 {
		t.Errorf("stats.RepoCascadeTeamOwnership = 0, want at least 1 (S1/S1b resolved from team %s); stats = %+v",
			teamID, stats)
	}
	if stats.RepoCascadeAncestor != 0 || stats.RepoCascadeChildren != 0 {
		t.Errorf("a hierarchy tier fired on a fixture with no hierarchy at all "+
			"(ancestor=%d children=%d); the team result would be attributed to the wrong tier",
			stats.RepoCascadeAncestor, stats.RepoCascadeChildren)
	}

	unitID := units.WorkUnitID([]units.NodeKey{
		{Type: "issue", ID: "S1"}, {Type: "issue", ID: "S1b"},
	})

	effortRepoID, effortSource, allocationSource := fetchRepoEffort(t, ctx, conn, unitID)
	if allocationSource != units.AllocationSourceTeamOwnership {
		t.Errorf("work_unit_repo_effort.allocation_source = %q, want %q -- "+
			"this is the pre-fix failure: the empty tier was never overridden",
			allocationSource, units.AllocationSourceTeamOwnership)
	}
	if effortRepoID != ownedRepo {
		t.Errorf("work_unit_repo_effort.repo_id = %q, want %q (the is_primary/specificity=100 row, "+
			"not the broader inferred %q)", effortRepoID, ownedRepo, broaderRepo)
	}
	if effortSource != TeamOwnershipSource(teamID) {
		t.Errorf("work_unit_repo_effort.repo_source = %q, want %q", effortSource, TeamOwnershipSource(teamID))
	}

	investmentRepoID, investmentRepoSource := fetchInvestmentRepo(t, ctx, conn, unitID)
	if investmentRepoID != ownedRepo {
		t.Errorf("work_unit_investments.repo_id = %q, want %q", investmentRepoID, ownedRepo)
	}
	if investmentRepoSource != TeamOwnershipSource(teamID) {
		t.Errorf("work_unit_investments.repo_source = %q, want %q", investmentRepoSource, TeamOwnershipSource(teamID))
	}

	// The product-facing proof, stated in the CONSUMER's terms rather than as
	// a row count: evaluate sankeycoverage.go's own repoAssignedCol expression
	// (cmd/query-api/internal/analytics/sankeycoverage.go:190) against the
	// written rows. This is the expression that decides whether the Investment
	// Sankey's "Repo coverage" card counts this unit as assigned.
	var sankeyResolvedRepo string
	if err := conn.QueryRow(ctx, `
		SELECT if(wure.work_unit_id != '', toString(wure.repo_id), toString(wui.repo_id))
		FROM work_unit_investments AS wui
		LEFT JOIN work_unit_repo_effort AS wure
			ON wure.org_id = wui.org_id AND wure.work_unit_id = wui.work_unit_id
		WHERE wui.org_id = ? AND wui.work_unit_id = ?
	`, hierarchyCascadeTestOrg, unitID).Scan(&sankeyResolvedRepo); err != nil {
		t.Fatalf("sankeycoverage-shape query: %v", err)
	}
	if sankeyResolvedRepo != ownedRepo {
		t.Errorf("sankeycoverage.go's repo-resolution expression resolves to %q, want %q -- "+
			"the Sankey would still count this unit as unassigned", sankeyResolvedRepo, ownedRepo)
	}
}

// TestTeamOwnershipCascadeLeavesATeamlessIssueUnassigned is the NEGATIVE
// control for the test above, against the SAME engine and the SAME code path.
// Without it, an implementation that attributed every unresolved unit to some
// arbitrary repo would pass the positive test and silently inflate coverage to
// 100% -- which is exactly the failure mode a coverage metric cannot survive.
func TestTeamOwnershipCascadeLeavesATeamlessIssueUnassigned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

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

	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	within := windowStart.Add(24 * time.Hour)

	// A team that owns a repo EXISTS -- so the loader has data to return --
	// but these issues carry a different native_team_key that owns nothing.
	seedTeam(t, ctx, conn, "CHAOS", "linear", "CHAOS", within)
	seedTeamRepoOwnership(t, ctx, conn, "CHAOS", "11111111-1111-4111-8111-111111111111", 1, 100, 0, within)

	seedTeamWorkItem(t, ctx, conn, "N1", "", "NOTATEAM", within)
	seedTeamWorkItem(t, ctx, conn, "N1b", "", "NOTATEAM", within)
	seedIssueEdge(t, ctx, conn, "N1", "N1b", "", within)

	stats, err := materializer.Run(ctx, Config{
		OrgID: hierarchyCascadeTestOrg, FromTS: windowStart, ToTS: windowEnd,
		RunID: "team-ownership-negative-run", ComputedAt: within, ProviderName: "mock",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.RepoCascadeTeamOwnership != 0 {
		t.Errorf("stats.RepoCascadeTeamOwnership = %d, want 0 -- an issue whose team owns nothing "+
			"was attributed anyway; stats = %+v", stats.RepoCascadeTeamOwnership, stats)
	}
	if stats.RepoCascadeUnassigned == 0 {
		t.Errorf("stats.RepoCascadeUnassigned = 0; the fixture has no resolvable repo at all, so the "+
			"unassigned bucket must be non-empty; stats = %+v", stats)
	}

	unitID := units.WorkUnitID([]units.NodeKey{
		{Type: "issue", ID: "N1"}, {Type: "issue", ID: "N1b"},
	})
	repoID, repoSource, allocationSource := fetchRepoEffort(t, ctx, conn, unitID)
	if allocationSource != units.AllocationSourceEmpty {
		t.Errorf("work_unit_repo_effort.allocation_source = %q, want %q", allocationSource, units.AllocationSourceEmpty)
	}
	if repoSource != "" {
		t.Errorf("work_unit_repo_effort.repo_source = %q, want empty", repoSource)
	}
	// `toString` of a NULL Nullable(UUID) is the EMPTY string, not the zero
	// UUID -- measured, not assumed (the first version of this assert expected
	// the zero UUID and failed). The distinction matters downstream:
	// sankeycoverage.go tests `repo_id IS NOT NULL` on the raw column, so a
	// genuine NULL is what keeps this unit OUT of the assigned numerator,
	// whereas a zero UUID would have counted as assigned while pointing at no
	// repository.
	if repoID != "" {
		t.Errorf("work_unit_repo_effort.repo_id = %q, want an empty string (a real SQL NULL)", repoID)
	}
}

// seedTeam inserts one row into `teams`. Separate from seedWorkItem's fixture
// helpers because the team catalog is a different sync's output entirely.
func seedTeam(t *testing.T, ctx context.Context, conn driver.Conn, teamID, provider, nativeTeamKey string, at time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `
		INSERT INTO teams (
			id, team_uuid, name, members, manual_members, updated_at, last_synced,
			org_id, provider, native_team_key, project_keys, repo_patterns, is_active
		) VALUES (?, generateUUIDv4(), ?, [], [], ?, ?, ?, ?, ?, [], [], 1)
	`, teamID, teamID, at, at, hierarchyCascadeTestOrg, provider, nativeTeamKey); err != nil {
		t.Fatalf("seed team %s: %v", teamID, err)
	}
}

// seedTeamRepoOwnership inserts one LIVE ownership row (valid_to NULL). The
// is_primary/specificity/priority triple is the ordering the loader ranks by,
// so a caller can build a real contest between candidate repositories.
func seedTeamRepoOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	teamID, repoID string, isPrimary uint8, specificity uint16, priority int32, at time.Time,
) {
	t.Helper()
	source := "inferred"
	if isPrimary == 1 {
		source = "manual"
	}
	if err := conn.Exec(ctx, `
		INSERT INTO team_repo_ownership (
			org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
			is_primary, specificity, priority, valid_from, valid_to, updated_at
		) VALUES (?, 'github', ?, ?, ?, 'exact', ?, ?, ?, ?, ?, NULL, ?)
	`, hierarchyCascadeTestOrg, teamID, repoID, "full-chaos/"+repoID[:8], source,
		isPrimary, specificity, priority, at, at); err != nil {
		t.Fatalf("seed team_repo_ownership %s/%s: %v", teamID, repoID, err)
	}
}

// seedTeamWorkItem is seedWorkItem plus a native_team_key.
//
// A separate helper rather than a widened seedWorkItem: the existing
// hierarchy-cascade test's fixtures deliberately carry NO team, and giving
// them one would make that test's ancestor result ambiguous about which tier
// produced it.
func seedTeamWorkItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	workItemID, parentID, nativeTeamKey string, at time.Time,
) {
	t.Helper()
	if err := conn.Exec(ctx, `
		INSERT INTO work_items (
			repo_id, work_item_id, provider, title, type, status, status_raw,
			project_key, project_id, native_team_key, project_name, assignees, reporter,
			created_at, updated_at, labels, sprint_id, sprint_name, parent_id, epic_id,
			url, last_synced, org_id
		) VALUES (
			generateUUIDv4(), ?, 'linear', ?, 'issue', 'open', 'open',
			'', '', ?, '', [], '',
			?, ?, [], '', '', ?, '',
			'', ?, ?
		)
	`, workItemID, workItemID, nativeTeamKey, at, at, parentID, at, hierarchyCascadeTestOrg); err != nil {
		t.Fatalf("seed team work item %s: %v", workItemID, err)
	}
}
