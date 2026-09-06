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

const hierarchyCascadeTestOrg = "70d529e0-3c06-4597-8480-794fd02328b6"

// TestRepoHierarchyCascadeEndToEnd proves the whole CHAOS-5359 pipeline
// against a real engine, not just the pure function
// (hierarchycascade_test.go covers the algorithm's branches directly): a
// pure-issue component with no PR/commit edges of its own -- 76.1% of
// work_unit_investments rows, per the executed diagnosis on the ticket --
// inherits its ancestor's repo through Materializer.Run, and that inherited
// value survives into BOTH tables the Sankey coverage query can read from.
//
// The second half is the actual bug this migration fixes: sankeycoverage.go
// prefers work_unit_repo_effort.repo_id over work_unit_investments.repo_id
// the moment ANY row exists for a work_unit_id -- and AllocateRepoEffort
// always writes one, even its empty tier. A cascade that patched only
// work_unit_investments would be silently shadowed by that empty-tier row.
// This test evaluates the SAME boolean expression compileSankeyCoverage
// builds (cmd/query-api/internal/analytics/sankeycoverage.go:190) directly
// against the written rows, so a regression that reintroduces the shadow
// fails HERE, not only in a manual Sankey read.
func TestRepoHierarchyCascadeEndToEnd(t *testing.T) {
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

	repoA := "11111111-1111-4111-8111-111111111111"
	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	within := windowStart.Add(24 * time.Hour)

	// P: the resolved ancestor. Its OWN edge (to an unrelated anchor issue)
	// carries repo_id directly -- collectSingleRepoID needs only an edge
	// with a repo_id on it, not a real PR/commit row, so an issue-issue edge
	// is enough to build a realistic "this component has exactly one repo"
	// anchor without seeding pull_requests/commits tables this test does not
	// otherwise exercise.
	seedWorkItem(t, ctx, conn, "P", "", within)
	seedWorkItem(t, ctx, conn, "P-anchor", "", within)
	seedIssueEdge(t, ctx, conn, "P", "P-anchor", repoA, within)

	// C1/C1b: the pure-issue component under test. No PR/commit, no repo
	// edge of its own -- C1.parent_id = P is its only path to a repo.
	seedWorkItem(t, ctx, conn, "C1", "P", within)
	seedWorkItem(t, ctx, conn, "C1b", "", within)
	seedIssueEdge(t, ctx, conn, "C1", "C1b", "", within)

	stats, err := materializer.Run(ctx, Config{
		OrgID: hierarchyCascadeTestOrg, FromTS: windowStart, ToTS: windowEnd,
		RunID: "cascade-e2e-run", ComputedAt: within, ProviderName: "mock",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.RepoCascadeAncestor == 0 {
		t.Errorf("stats.RepoCascadeAncestor = 0, want at least 1 (C1/C1b's inheritance from P); stats = %+v", stats)
	}

	unitID := units.WorkUnitID([]units.NodeKey{
		{Type: "issue", ID: "C1"}, {Type: "issue", ID: "C1b"},
	})

	investmentRepoID, investmentRepoSource := fetchInvestmentRepo(t, ctx, conn, unitID)
	if investmentRepoID != repoA {
		t.Errorf("work_unit_investments.repo_id = %q, want %q (inherited from P)", investmentRepoID, repoA)
	}
	if investmentRepoSource != AncestorSource("P") {
		t.Errorf("work_unit_investments.repo_source = %q, want %q", investmentRepoSource, AncestorSource("P"))
	}

	effortRepoID, effortSource, allocationSource := fetchRepoEffort(t, ctx, conn, unitID)
	if effortRepoID != repoA {
		t.Errorf("work_unit_repo_effort.repo_id = %q, want %q -- the empty-tier row was NOT overridden, so sankeycoverage.go's "+
			"wure-first read would still see NULL and shadow the fix", effortRepoID, repoA)
	}
	if allocationSource != units.AllocationSourceHierarchyCascade {
		t.Errorf("work_unit_repo_effort.allocation_source = %q, want %q", allocationSource, units.AllocationSourceHierarchyCascade)
	}
	if effortSource != AncestorSource("P") {
		t.Errorf("work_unit_repo_effort.repo_source = %q, want %q", effortSource, AncestorSource("P"))
	}

	// The actual regression proof: evaluate sankeycoverage.go's own resolution
	// expression (repoAssignedCol, sankeycoverage.go:190) against the written
	// rows. A future change that stops overriding the empty-tier wure row
	// would make wure.repo_id NULL again and this assert would catch it even
	// if the two field-level asserts above were somehow satisfied by a
	// different bug.
	var sankeyResolvedRepo string
	err = conn.QueryRow(ctx, `
		SELECT if(wure.work_unit_id != '', toString(wure.repo_id), toString(wui.repo_id))
		FROM work_unit_investments AS wui
		LEFT JOIN work_unit_repo_effort AS wure
			ON wure.org_id = wui.org_id AND wure.work_unit_id = wui.work_unit_id
		WHERE wui.org_id = ? AND wui.work_unit_id = ?
	`, hierarchyCascadeTestOrg, unitID).Scan(&sankeyResolvedRepo)
	if err != nil {
		t.Fatalf("sankeycoverage-shape query: %v", err)
	}
	if sankeyResolvedRepo != repoA {
		t.Errorf("sankeycoverage.go's own repo-resolution expression resolves to %q, want %q -- "+
			"the Sankey would still show this unit as unassigned", sankeyResolvedRepo, repoA)
	}
}

func seedWorkItem(t *testing.T, ctx context.Context, conn driver.Conn, workItemID, parentID string, at time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `
		INSERT INTO work_items (
			repo_id, work_item_id, provider, title, type, status, status_raw,
			project_key, project_id, assignees, reporter, created_at, updated_at,
			labels, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id
		) VALUES (
			generateUUIDv4(), ?, 'linear', ?, 'issue', 'open', 'open',
			'', '', [], '', ?, ?,
			[], '', '', ?, '', '', ?, ?
		)
	`, workItemID, workItemID, at, at, parentID, at, hierarchyCascadeTestOrg); err != nil {
		t.Fatalf("seed work item %s: %v", workItemID, err)
	}
}

func seedIssueEdge(t *testing.T, ctx context.Context, conn driver.Conn, sourceID, targetID, repoID string, at time.Time) {
	t.Helper()
	var repoArg any
	if repoID != "" {
		repoArg = repoID
	}
	if err := conn.Exec(ctx, `
		INSERT INTO work_graph_edges (
			edge_id, source_type, source_id, target_type, target_id, edge_type,
			repo_id, provider, provenance, confidence, evidence,
			discovered_at, last_synced, event_ts, org_id
		) VALUES (?, 'issue', ?, 'issue', ?, 'relates_to', ?, 'linear', 'native', 1.0, 'seed', ?, ?, ?, ?)
	`, sourceID+"->"+targetID, sourceID, targetID, repoArg, at, at, at, hierarchyCascadeTestOrg); err != nil {
		t.Fatalf("seed edge %s->%s: %v", sourceID, targetID, err)
	}
}

func fetchInvestmentRepo(t *testing.T, ctx context.Context, conn driver.Conn, workUnitID string) (repoID, repoSource string) {
	t.Helper()
	if err := conn.QueryRow(ctx, `
		SELECT toString(repo_id), ifNull(repo_source, '')
		FROM work_unit_investments FINAL
		WHERE org_id = ? AND work_unit_id = ?
	`, hierarchyCascadeTestOrg, workUnitID).Scan(&repoID, &repoSource); err != nil {
		t.Fatalf("fetch work_unit_investments for %s: %v", workUnitID, err)
	}
	return repoID, repoSource
}

func fetchRepoEffort(t *testing.T, ctx context.Context, conn driver.Conn, workUnitID string) (repoID, repoSource, allocationSource string) {
	t.Helper()
	if err := conn.QueryRow(ctx, `
		SELECT toString(repo_id), ifNull(repo_source, ''), allocation_source
		FROM work_unit_repo_effort FINAL
		WHERE org_id = ? AND work_unit_id = ?
	`, hierarchyCascadeTestOrg, workUnitID).Scan(&repoID, &repoSource, &allocationSource); err != nil {
		t.Fatalf("fetch work_unit_repo_effort for %s: %v", workUnitID, err)
	}
	return repoID, repoSource, allocationSource
}
