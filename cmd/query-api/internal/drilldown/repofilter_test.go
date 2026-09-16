package drilldown

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// queryCapturingClient records the last statement it was asked to run
// and answers with a fixed row set -- used by the shape tests below,
// which care about the QUERY TEXT resolveRepoID/resolveRepoIDsForTeams
// build, not the rows they return.
type queryCapturingClient struct {
	rows      [][]any
	lastQuery string
}

func (c *queryCapturingClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.lastQuery = query
	return &fixtureRowScanner{rows: c.rows}, nil
}

// assertSameDepth pins that every marker in markers appears in query AND
// sits at the same parenthesis nesting depth as the first one --
// sqlshape.Depths (the same parenthesis-aware scan internal/providersync,
// internal/syncreconciler and internal/filteroptions already share for
// their own structural SQL guards) rather than a bare substring/"("
// search, so a future edit that nests one predicate inside a subquery
// while leaving the others at the top level is caught even though every
// marker still, individually, appears somewhere in the string.
func assertSameDepth(t *testing.T, query string, markers ...string) {
	t.Helper()
	if len(markers) == 0 {
		return
	}
	depths := sqlshape.Depths(query)
	var wantDepth int
	for i, marker := range markers {
		idx := strings.Index(query, marker)
		if idx == -1 {
			t.Fatalf("query missing %q:\n%s", marker, query)
		}
		depth := depths[idx]
		if i == 0 {
			wantDepth = depth
			continue
		}
		if depth != wantDepth {
			t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d -- both must scope the "+
				"SAME statement\nquery:\n%s", marker, depth, markers[0], wantDepth, query)
		}
	}
}

// TestResolveRepoIDByUUIDQueryShape pins resolveRepoID's UUID-ref
// branch: repos (ReplacingMergeTree) is read with FINAL, never raw, and
// the org filter sits inside this SAME read, at the SAME nesting depth
// as FINAL and the id equality -- not a separate read, and not a join
// or subquery evaluated afterward.
func TestResolveRepoIDByUUIDQueryShape(t *testing.T) {
	client := &queryCapturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, _, err := reader.resolveRepoID(context.Background(), "12345678-1234-5678-1234-567812345678", "org-1"); err != nil {
		t.Fatalf("resolveRepoID: %v", err)
	}
	assertSameDepth(t, client.lastQuery, "FROM repos FINAL", "toString(id) = {repo_id:String}", "org_id = {org_id:String}")
}

// TestResolveRepoIDByNameQueryShape is the name-lookup branch's own copy.
func TestResolveRepoIDByNameQueryShape(t *testing.T) {
	client := &queryCapturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, _, err := reader.resolveRepoID(context.Background(), "acme/webapp", "org-1"); err != nil {
		t.Fatalf("resolveRepoID: %v", err)
	}
	assertSameDepth(t, client.lastQuery, "FROM repos FINAL", "repo = {repo_name:String}", "org_id = {org_id:String}")
}

// TestResolveRepoIDsForTeamsQueryShape pins that user_metrics_daily,
// which migration 096 converts to ReplacingMergeTree(computed_at) with
// team_id outside its sorting key, is read with FINAL (never raw) --
// class ruling requirement (b) -- and that the org filter sits inside
// this same read, at the SAME nesting depth as FINAL and team_id --
// requirement (a). See this function's own doc comment in repofilter.go
// for why team_id being outside the sorting key makes the FINAL dedup
// load-bearing here, not just a style preference.
func TestResolveRepoIDsForTeamsQueryShape(t *testing.T) {
	client := &queryCapturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := reader.resolveRepoIDsForTeams(context.Background(), []string{"team-x"}, "org-1"); err != nil {
		t.Fatalf("resolveRepoIDsForTeams: %v", err)
	}
	assertSameDepth(t, client.lastQuery, "FROM user_metrics_daily FINAL", "team_id IN {team_ids:Array(String)}", "org_id = {org_id:String}")
}
