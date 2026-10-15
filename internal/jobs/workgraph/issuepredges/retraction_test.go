package issuepredges

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

var otherRepo = uuid.MustParse("00000000-0000-4000-8000-0000000007b0")

func prIssueEdge(repo uuid.UUID, pr int, edgeType, workItemID, provenance string) edges.Row {
	prID := edges.GeneratePRID(repo, pr)
	return edges.Row{
		EdgeID:     edges.EdgeID(edges.NodeTypePR, prID, edgeType, edges.NodeTypeIssue, workItemID),
		SourceType: edges.NodeTypePR, SourceID: prID,
		TargetType: edges.NodeTypeIssue, TargetID: workItemID,
		EdgeType: edgeType, Provenance: provenance, Confidence: 0.9,
	}
}

func issueIssueEdge(source, target, provenance string) edges.Row {
	return edges.Row{
		EdgeID:     edges.EdgeID(edges.NodeTypeIssue, source, edges.EdgeTypeRelates, edges.NodeTypeIssue, target),
		SourceType: edges.NodeTypeIssue, SourceID: source,
		TargetType: edges.NodeTypeIssue, TargetID: target,
		EdgeType: edges.EdgeTypeRelates, Provenance: provenance, Confidence: 0.9,
	}
}

func versionsOf(stored time.Time, deleted map[int]bool, rows ...edges.Row) edges.EdgeVersions {
	versions := edges.EdgeVersions{}
	for index, row := range rows {
		versions.Put(edges.EdgeVersion{Latest: row, IsDeleted: deleted[index], MaxLastSynced: stored})
	}
	return versions
}

func retiredTargets(tombstones []edges.Row) []string {
	var out []string
	for _, row := range tombstones {
		out = append(out, row.SourceID[strings.LastIndex(row.SourceID, "#"):]+" "+row.EdgeType+" "+row.TargetID)
	}
	sort.Strings(out)
	return out
}

func TestPlanOutrankedEdgeTombstonesRetiresOnlyOutrankedFallbackEdges(t *testing.T) {
	stored := []issueprlinks.Link{
		{RepoID: fallbackRepo, PRNumber: 704, WorkItemID: "linear:CHAOS-1", Provenance: "native"},
		{RepoID: fallbackRepo, PRNumber: 704, WorkItemID: "jira:OPS-7", Provenance: "native"},
		{RepoID: fallbackRepo, PRNumber: 705, WorkItemID: "jira:API-1", Provenance: "explicit_text"},
	}
	rows := []edges.Row{
		prIssueEdge(fallbackRepo, 704, edges.EdgeTypeReferences, "jira:API-1", "explicit_text"),     // 0 retire: PR side
		prIssueEdge(fallbackRepo, 704, edges.EdgeTypeRelates, "jira:BILL-1", "heuristic"),           // 1 retire: PR side
		prIssueEdge(fallbackRepo, 706, edges.EdgeTypeReferences, "jira:OPS-7", "explicit_text"),     // 2 retire: issue side
		prIssueEdge(fallbackRepo, 704, edges.EdgeTypeReferences, "linear:CHAOS-1", "explicit_text"), // 3 retire: native pair, not IMPLEMENTS
		prIssueEdge(fallbackRepo, 704, edges.EdgeTypeImplements, "linear:CHAOS-1", "explicit_text"), // 4 keep: fast path rewrites it
		prIssueEdge(fallbackRepo, 704, edges.EdgeTypeImplements, "jira:OPS-7", "native"),            // 5 keep: native
		prIssueEdge(fallbackRepo, 705, edges.EdgeTypeReferences, "jira:API-1", "explicit_text"),     // 6 keep: no native side
		prIssueEdge(otherRepo, 704, edges.EdgeTypeReferences, "jira:API-1", "explicit_text"),        // 7 keep: other repository
		prIssueEdge(fallbackRepo, 704, edges.EdgeTypeReferences, "jira:FC-1", "explicit_text"),      // 8 keep: already retired
		prIssueEdge(fallbackRepo, 704, edges.EdgeTypeRelates, "linear:CHAOS-1", "native"),           // 9 keep: native, not IMPLEMENTS
		issueIssueEdge("jira:API-9", "jira:OPS-7", "explicit_text"),                                 // 10 keep: not a pull request edge
	}
	stamp := time.Date(2026, 10, 15, 14, 0, 0, 0, time.UTC)
	clock := stamp.Add(time.Minute)
	tombstones := PlanOutrankedEdgeTombstones(versionsOf(stamp, map[int]bool{8: true}, rows...), stored, clock)

	got := retiredTargets(tombstones)
	want := []string{
		"#pr704 references jira:API-1",
		"#pr704 references linear:CHAOS-1",
		"#pr704 relates jira:BILL-1",
		"#pr706 references jira:OPS-7",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("retired:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, row := range tombstones {
		if !row.LastSynced.After(stamp) {
			t.Fatalf("tombstone %s is not newer than the stored version", row.EdgeID)
		}
	}
}

// TestRetractOutrankedFallbacksCleansBothTables drives the step end to end:
// the stored #704 shape loses its text links in work_graph_issue_pr (one
// mutation naming exactly those rows) and its text edges in work_graph_edges
// (tombstones), and the control link survives.
func TestRetractOutrankedFallbacksCleansBothTables(t *testing.T) {
	conn := newRouteConn()
	conn.issuePR = [][]any{
		{fallbackRepo, "linear:CHAOS-1", uint32(704), "native"},
		{fallbackRepo, "jira:API-1", uint32(704), "explicit_text"},
		{fallbackRepo, "jira:BILL-1", uint32(704), "explicit_text"},
		{fallbackRepo, "jira:FC-1", uint32(705), "explicit_text"},
	}
	stamp := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	retire := prIssueEdge(fallbackRepo, 704, edges.EdgeTypeReferences, "jira:API-1", "explicit_text")
	keep := prIssueEdge(fallbackRepo, 705, edges.EdgeTypeReferences, "jira:FC-1", "explicit_text")
	for _, row := range []edges.Row{retire, keep} {
		conn.edgeVersions = append(conn.edgeVersions, []any{
			row.SourceType, row.SourceID, row.EdgeType, row.TargetType, row.TargetID,
			row.EdgeID, fallbackRepo, "github", row.Provenance, row.Confidence, row.Evidence,
			stamp, stamp, stamp, uint8(0), stamp,
		})
	}

	service := newFallbackService(t, conn)
	outcome, err := service.RetractOutrankedFallbacks(context.Background(), "org-9821")
	if err != nil {
		t.Fatalf("RetractOutrankedFallbacks: %v", err)
	}
	if outcome.LinksRead != 4 || outcome.LinksRetracted != 2 {
		t.Fatalf("outcome = %+v, want 4 read, 2 retracted", outcome)
	}
	if len(conn.execs) != 1 || !strings.Contains(conn.execs[0], "ALTER TABLE work_graph_issue_pr") {
		t.Fatalf("want one link mutation, got %v", conn.execs)
	}
	keys := conn.execArgs[0][1]
	wantKeys := []string{
		fallbackRepo.String() + "|jira:API-1|704|explicit_text",
		fallbackRepo.String() + "|jira:BILL-1|704|explicit_text",
	}
	if strings.Join(namedStrings(t, keys), ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("mutation keys = %v, want %v", keys, wantKeys)
	}
	tombstones := conn.appended("work_graph_edges")
	if len(tombstones) != 1 || tombstones[0][2] != retire.SourceID || tombstones[0][4] != "jira:API-1" || tombstones[0][16] != uint8(1) {
		t.Fatalf("want one tombstone for %s -> jira:API-1, got %v", retire.SourceID, tombstones)
	}
	if outcome.EdgesRetired != 1 {
		t.Fatalf("outcome.EdgesRetired = %d, want 1", outcome.EdgesRetired)
	}
}

func TestRetractOutrankedFallbacksWithNothingOutrankedWritesNothing(t *testing.T) {
	conn := newRouteConn()
	conn.issuePR = [][]any{
		{fallbackRepo, "linear:CHAOS-1", uint32(704), "native"},
		{fallbackRepo, "jira:FC-1", uint32(705), "explicit_text"},
	}
	service := newFallbackService(t, conn)
	outcome, err := service.RetractOutrankedFallbacks(context.Background(), "org-9821")
	if err != nil {
		t.Fatal(err)
	}
	if outcome.LinksRetracted != 0 || len(conn.execs) != 0 || len(conn.appended("work_graph_edges")) != 0 {
		t.Fatalf("want no writes, got outcome=%+v execs=%v", outcome, conn.execs)
	}
}

func TestRetractOutrankedFallbacksRequiresAnOrganization(t *testing.T) {
	service := newFallbackService(t, newRouteConn())
	if _, err := service.RetractOutrankedFallbacks(context.Background(), ""); err != ErrScopeRequired {
		t.Fatalf("err = %v, want ErrScopeRequired", err)
	}
}
