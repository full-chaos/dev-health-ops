//go:build integration

package remaining

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// TestBothConsumersBuildTheSameUnitsWhenIssueLinksAreReferences runs the two
// live consumers of the unit build -- the investment materializer and the
// membership backfill -- on ONE work_graph_edges input in a real ClickHouse,
// and checks the units each one persists. Issue -> issue links (relates,
// blocks, duplicates, parent/child) are references: they join nothing, and an
// issue whose only links are issue links is still a one-node unit. Both
// tables must hold exactly the expected unit ids; a consumer that grouped
// differently would write membership rows keyed by ids with no investment row.
func TestBothConsumersBuildTheSameUnitsWhenIssueLinksAreReferences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	conn := membershipMigratedClickHouse(t, ctx)
	orgID := uuid.NewString()
	repo := "2a5e0000-0000-4000-8000-000000000001"
	at := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -5)

	issue := func(id string) units.NodeKey { return units.NodeKey{Type: "issue", ID: id} }
	prNode := units.NodeKey{Type: "pr", ID: repo + "#pr1"}
	commitNode := units.NodeKey{Type: "commit", ID: repo + "@c1"}
	// One issue per provider shape, plus a pull request work item.
	ids := []string{"jira:ACME-1", "linear:ENG-2", "gh:acme/web#3", "gitlab:acme/api#4", "ghpr:acme/web#5", "jira:ACME-6", "linear:ENG-7"}
	for _, id := range ids {
		seedIssueLinkWorkItem(t, ctx, conn, orgID, id, at)
	}
	type edge struct {
		source units.NodeKey
		kind   string
		target units.NodeKey
		conf   float32
	}
	edges := []edge{
		{prNode, "implements", issue("ghpr:acme/web#5"), 1.0},
		{prNode, "references", issue("jira:ACME-1"), 0.9},
		{commitNode, "references", issue("linear:ENG-2"), 0.85},
		{issue("jira:ACME-1"), "relates", issue("linear:ENG-2"), 0.9},
		{issue("gh:acme/web#3"), "blocks", issue("gitlab:acme/api#4"), 0.9},
		{issue("jira:ACME-6"), "duplicates", issue("ghpr:acme/web#5"), 0.9},
		{issue("linear:ENG-7"), "parent_of", issue("gh:acme/web#3"), 1.0},
	}
	for _, e := range edges {
		seedIssueLinkEdge(t, ctx, conn, orgID, repo, e.source, e.kind, e.target, e.conf, at)
	}

	want := map[string][]units.NodeKey{}
	for _, nodes := range [][]units.NodeKey{
		{prNode, issue("ghpr:acme/web#5"), issue("jira:ACME-1")},
		{commitNode, issue("linear:ENG-2")},
		{issue("gh:acme/web#3")},
		{issue("gitlab:acme/api#4")},
		{issue("jira:ACME-6")},
		{issue("linear:ENG-7")},
	} {
		want[units.WorkUnitID(nodes)] = nodes
	}

	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := investment.NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := materializer.Run(ctx, investment.Config{
		OrgID: orgID, FromTS: at.AddDate(0, 0, -10), ToTS: at.AddDate(0, 0, 5),
		RunID: "issue-links-are-references", ComputedAt: at.Add(time.Hour), ProviderName: "mock",
	})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if stats.ReferenceLinks != 4 || stats.ReferenceOnlyUnits != 4 {
		t.Errorf("stats reference_links=%d reference_only_units=%d, want 4 and 4", stats.ReferenceLinks, stats.ReferenceOnlyUnits)
	}

	membershipWriter, err := NewMembershipClickHouseWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewMembershipExecutor(ctx, conn, membershipWriter)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := executor.ComputeOrg(ctx, orgID, nil, at.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("membership: %v", err)
	}
	if outcome.Components != len(want) || outcome.Matched != len(want) || outcome.Skipped != 0 {
		t.Errorf("membership outcome = %+v, want %d components, all matched, none skipped", outcome, len(want))
	}

	investmentIDs := distinctUnitIDs(t, ctx, conn, "SELECT DISTINCT work_unit_id FROM work_unit_investments FINAL WHERE org_id = {org_id:String}", orgID)
	membershipIDs := distinctUnitIDs(t, ctx, conn, "SELECT DISTINCT work_unit_id FROM work_unit_membership WHERE org_id = {org_id:String}", orgID)
	for name, got := range map[string][]string{"work_unit_investments": investmentIDs, "work_unit_membership": membershipIDs} {
		if len(got) != len(want) {
			t.Errorf("%s holds %d units, want %d", name, len(got), len(want))
		}
		for _, id := range got {
			if _, ok := want[id]; !ok {
				t.Errorf("%s holds unit %s, which is not one of the expected node sets", name, id)
			}
		}
	}
}

func seedIssueLinkWorkItem(t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID string, at time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `
		INSERT INTO work_items (
			repo_id, work_item_id, provider, title, type, status, status_raw,
			project_key, project_id, assignees, reporter, created_at, updated_at,
			labels, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id
		) VALUES (
			generateUUIDv4(), {id:String}, 'linear', {id:String}, 'issue', 'open', 'open',
			'', '', [], '', {at:DateTime64(3)}, {at:DateTime64(3)},
			[], '', '', '', '', '', {at:DateTime64(3)}, {org_id:String}
		)
	`, clickhouse.Named("id", workItemID), clickhouse.Named("at", at), clickhouse.Named("org_id", orgID)); err != nil {
		t.Fatalf("seed work item %s: %v", workItemID, err)
	}
}

func seedIssueLinkEdge(t *testing.T, ctx context.Context, conn driver.Conn, orgID, repoID string, source units.NodeKey, kind string, target units.NodeKey, confidence float32, at time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `
		INSERT INTO work_graph_edges (
			edge_id, source_type, source_id, target_type, target_id, edge_type,
			repo_id, provider, provenance, confidence, evidence,
			discovered_at, last_synced, event_ts, org_id
		) VALUES (
			{edge_id:String}, {st:String}, {si:String}, {tt:String}, {ti:String}, {kind:String},
			{repo:String}, 'github', 'native', {conf:Float32}, 'seed',
			{at:DateTime64(3)}, {at:DateTime64(3)}, {at:DateTime64(3)}, {org_id:String}
		)
	`, clickhouse.Named("edge_id", source.ID+"|"+kind+"|"+target.ID), clickhouse.Named("st", source.Type), clickhouse.Named("si", source.ID),
		clickhouse.Named("tt", target.Type), clickhouse.Named("ti", target.ID), clickhouse.Named("kind", kind),
		clickhouse.Named("repo", repoID), clickhouse.Named("conf", confidence), clickhouse.Named("at", at), clickhouse.Named("org_id", orgID)); err != nil {
		t.Fatalf("seed edge %s -> %s: %v", source.ID, target.ID, err)
	}
}

func distinctUnitIDs(t *testing.T, ctx context.Context, conn driver.Conn, query, orgID string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", orgID))
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(ids)
	return ids
}
