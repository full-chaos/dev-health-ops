//go:build integration

package issuepredges_test

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFallbackRetractionOnRealClickHouse runs the retraction step against the
// real schema: the link mutation's key expression must match what the planner
// computes, the provenance guard must leave a native row on a shared key, and
// the edge read must bind its arrays. The stored rows are the prod #704 shape
// (a pull request with a native Linear link plus text links to Jira keys its
// body quoted) and the issue side (an issue with a native link plus text and
// heuristic links to other pull requests).
func TestFallbackRetractionOnRealClickHouse(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	org := uuid.NewString()
	repo := uuid.New()
	stamp := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	link := func(pr uint32, workItemID, provenance string) issueprlinks.Link {
		return issueprlinks.Link{OrgID: org, RepoID: repo, PRNumber: pr, WorkItemID: workItemID,
			Provenance: provenance, Confidence: 0.9, Evidence: "fixture", LastSynced: stamp}
	}
	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(ctx, []issueprlinks.Link{
		link(704, "linear:CHAOS-1", "native"),
		link(704, "linear:CHAOS-1", "explicit_text"), // same key as the native row
		link(704, "jira:API-1", "explicit_text"),
		link(704, "jira:BILL-1", "heuristic"),
		link(800, "gh:full-chaos/acr#5", "native"),
		link(706, "gh:full-chaos/acr#5", "explicit_text"),
		link(707, "gh:full-chaos/acr#5", "heuristic"),
		link(705, "gitlab:full-chaos/acr#9", "explicit_text"), // control
	}); err != nil {
		t.Fatal(err)
	}

	edge := func(pr int, edgeType, workItemID, provenance string) edges.Row {
		prID := edges.GeneratePRID(repo, pr)
		repoID := repo
		return edges.Row{
			EdgeID:     edges.EdgeID(edges.NodeTypePR, prID, edgeType, edges.NodeTypeIssue, workItemID),
			SourceType: edges.NodeTypePR, SourceID: prID, TargetType: edges.NodeTypeIssue, TargetID: workItemID,
			EdgeType: edgeType, Provenance: provenance, Confidence: 0.9, Evidence: "fixture",
			DiscoveredAt: stamp, LastSynced: stamp, EventTs: stamp, Day: edges.DayFor(stamp), RepoID: &repoID,
		}
	}
	if _, err := edges.WriteEdges(ctx, conn, org, []edges.Row{
		edge(704, edges.EdgeTypeImplements, "linear:CHAOS-1", "native"),
		edge(704, edges.EdgeTypeReferences, "jira:API-1", "explicit_text"),
		edge(706, edges.EdgeTypeReferences, "gh:full-chaos/acr#5", "explicit_text"),
		edge(705, edges.EdgeTypeReferences, "gitlab:full-chaos/acr#9", "explicit_text"),
	}); err != nil {
		t.Fatal(err)
	}

	service := newRetractionService(t, conn)
	outcome, err := service.RetractOutrankedFallbacks(ctx, org)
	if err != nil {
		t.Fatalf("RetractOutrankedFallbacks: %v", err)
	}
	if outcome.LinksRetracted != 4 || outcome.EdgesRetired != 2 {
		t.Fatalf("outcome = %+v, want 4 links retracted and 2 edges retired", outcome)
	}

	wantLinks := []string{
		"gh:full-chaos/acr#5#800 native",
		"gitlab:full-chaos/acr#9#705 explicit_text",
		"linear:CHAOS-1#704 native",
	}
	if got := readLinks(ctx, t, conn, org, "FINAL"); strings.Join(got, "\n") != strings.Join(wantLinks, "\n") {
		t.Fatalf("links after retraction (FINAL):\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(wantLinks, "\n"))
	}
	// Unmerged parts too: the planned rows are physically gone. (The text row
	// that shares the native row's key is resolved by the version column,
	// migration 084, and is not planned.)
	raw := strings.Join(readLinks(ctx, t, conn, org, ""), "\n")
	for _, retracted := range []string{
		"jira:API-1#704 explicit_text", "jira:BILL-1#704 heuristic",
		"gh:full-chaos/acr#5#706 explicit_text", "gh:full-chaos/acr#5#707 heuristic",
	} {
		if strings.Contains(raw, retracted) {
			t.Fatalf("retracted row %q still stored:\n%s", retracted, raw)
		}
	}

	wantEdges := []string{
		"#pr704 implements linear:CHAOS-1 native",
		"#pr705 references gitlab:full-chaos/acr#9 explicit_text",
	}
	if got := readLiveEdges(ctx, t, conn, org); strings.Join(got, "\n") != strings.Join(wantEdges, "\n") {
		t.Fatalf("live edges after retraction:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(wantEdges, "\n"))
	}

	again, err := service.RetractOutrankedFallbacks(ctx, org)
	if err != nil {
		t.Fatal(err)
	}
	if again.LinksRetracted != 0 || again.EdgesRetired != 0 {
		t.Fatalf("second run = %+v, want nothing left to retract", again)
	}
}

func newRetractionService(t *testing.T, conn driver.Conn) *issuepredges.Service {
	t.Helper()
	loader, err := issuepredges.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	service, err := issuepredges.NewService(loader, conn, writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func readLinks(ctx context.Context, t *testing.T, conn driver.Conn, org, final string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT work_item_id, pr_number, provenance FROM work_graph_issue_pr "+final+
		" WHERE org_id = {org_id:String}", clickhouse.Named("org_id", org))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var (
			workItemID, provenance string
			pr                     uint32
		)
		if err := rows.Scan(&workItemID, &pr, &provenance); err != nil {
			t.Fatal(err)
		}
		out = append(out, workItemID+"#"+strconv.FormatUint(uint64(pr), 10)+" "+provenance)
	}
	sort.Strings(out)
	return out
}

func readLiveEdges(ctx context.Context, t *testing.T, conn driver.Conn, org string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT source_id, edge_type, target_id, argMax(provenance, last_synced), argMax(is_deleted, last_synced)
FROM work_graph_edges
WHERE org_id = {org_id:String} AND source_type = 'pr' AND target_type = 'issue'
GROUP BY source_id, edge_type, target_id`, clickhouse.Named("org_id", org))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var (
			sourceID, edgeType, targetID, provenance string
			deleted                                  uint8
		)
		if err := rows.Scan(&sourceID, &edgeType, &targetID, &provenance, &deleted); err != nil {
			t.Fatal(err)
		}
		if deleted != 0 {
			continue
		}
		out = append(out, sourceID[strings.LastIndex(sourceID, "#"):]+" "+edgeType+" "+targetID+" "+provenance)
	}
	sort.Strings(out)
	return out
}
