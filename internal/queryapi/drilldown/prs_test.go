package drilldown

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

func TestScopeClauseRepoEmpty(t *testing.T) {
	sql, bindings := scopeClauseRepo(nil)
	if sql != "" || bindings != nil {
		t.Fatalf("scopeClauseRepo(nil) = %q, %v, want empty", sql, bindings)
	}
	sql, bindings = scopeClauseRepo([]string{})
	if sql != "" || bindings != nil {
		t.Fatalf("scopeClauseRepo([]) = %q, %v, want empty", sql, bindings)
	}
}

func TestScopeClauseRepoNonEmpty(t *testing.T) {
	sql, bindings := scopeClauseRepo([]string{"repo-a", "repo-b"})
	if !strings.Contains(sql, "AND pr.repo_id IN {scope_ids:Array(String)}") {
		t.Fatalf("scopeClauseRepo sql = %q", sql)
	}
	if len(bindings) != 1 || bindings[0].Name != "scope_ids" {
		t.Fatalf("scopeClauseRepo bindings = %+v", bindings)
	}
	got, ok := bindings[0].Value.([]string)
	if !ok || len(got) != 2 || got[0] != "repo-a" || got[1] != "repo-b" {
		t.Fatalf("scopeClauseRepo bindings value = %+v", bindings[0].Value)
	}
}

// TestFetchPullRequestsQueryDedupsBothTables pins that neither
// ReplacingMergeTree table this query reads (git_pull_requests, repos)
// is ever read raw: FINAL appears on both.
func TestFetchPullRequestsQueryDedupsBothTables(t *testing.T) {
	for _, want := range []string{
		"FROM git_pull_requests AS pr FINAL",
		"FROM repos FINAL",
	} {
		if !strings.Contains(fetchPullRequestsQuery, want) {
			t.Fatalf("fetchPullRequestsQuery missing %q:\n%s", want, fetchPullRequestsQuery)
		}
	}
}

// TestFetchPullRequestsQueryOrgFilterInsidePRRead pins the class ruling
// this query follows: git_pull_requests is never merged or scanned
// across tenants before the org boundary applies. Depth-based (via
// sqlshape.Depths, the same parenthesis-aware scan
// internal/providersync, internal/syncreconciler and
// internal/filteroptions already share for their own structural SQL
// guards, reused here rather than a package-local copy), not a bare
// substring search: a filter merely SOMEWHERE in the query string does
// not pass -- the repos FINAL dedup source and its own org_id predicate
// must sit at the SAME nesting depth (the same subquery statement), and
// that depth must be greater than 0 -- confirming the org boundary is
// genuinely inside a subquery bound to pr.repo_id, not the top-level
// statement. Also pins that this route continues to avoid
// git_pull_requests' own (possibly stale-backfilled) org_id column and
// never gates the org scope behind a JOIN evaluated after the FINAL
// merge -- see fetchPullRequestsQuery's own doc comment.
func TestFetchPullRequestsQueryOrgFilterInsidePRRead(t *testing.T) {
	if strings.Contains(fetchPullRequestsQuery, "JOIN") {
		t.Fatalf("fetchPullRequestsQuery must not join repos to gate org scope -- the org filter must sit inside git_pull_requests' own read, not behind a join whose org predicate is only evaluated after the FINAL merge:\n%s", fetchPullRequestsQuery)
	}
	if strings.Contains(fetchPullRequestsQuery, "pr.org_id") {
		t.Fatalf("fetchPullRequestsQuery must not gate org scope via git_pull_requests.org_id directly (see this package's own doc comment):\n%s", fetchPullRequestsQuery)
	}

	const inMarker = "pr.repo_id IN "
	if !strings.Contains(fetchPullRequestsQuery, inMarker) {
		t.Fatalf("fetchPullRequestsQuery does not bind pr.repo_id to an org-scoped subquery:\n%s", fetchPullRequestsQuery)
	}

	const finalMarker = "FROM repos FINAL"
	const orgPredicate = "org_id = {org_id:String}"
	depths := sqlshape.Depths(fetchPullRequestsQuery)

	finalIdx := strings.Index(fetchPullRequestsQuery, finalMarker)
	if finalIdx == -1 {
		t.Fatalf("fetchPullRequestsQuery missing %q:\n%s", finalMarker, fetchPullRequestsQuery)
	}
	orgIdx := strings.Index(fetchPullRequestsQuery, orgPredicate)
	if orgIdx == -1 {
		t.Fatalf("fetchPullRequestsQuery missing %q:\n%s", orgPredicate, fetchPullRequestsQuery)
	}

	finalDepth, orgDepth := depths[finalIdx], depths[orgIdx]
	if orgDepth != finalDepth {
		t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d -- org_id must scope the SAME "+
			"statement as the repos dedup source, not a subquery that dedups the whole table before "+
			"the tenant filter narrows it", orgPredicate, orgDepth, finalMarker, finalDepth)
	}
	if finalDepth == 0 {
		t.Fatalf("%q sits at the top level (depth 0), not nested inside pr.repo_id IN (...) -- org "+
			"scoping must happen in a subquery bound to git_pull_requests' own read, not a top-level "+
			"predicate:\n%s", finalMarker, fetchPullRequestsQuery)
	}
}

func TestBuildPRsResponseNilReader(t *testing.T) {
	_, err := BuildPRsResponse(context.Background(), nil, "org-1", PRParams{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestBuildPRsResponseQueryError(t *testing.T) {
	boom := errors.New("boom")
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return nil, boom
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildPRsResponse(context.Background(), reader, "org-1", PRParams{ScopeLevel: "org", Limit: 50})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want wrapped boom", err)
	}
}

// TestBuildPRsResponseEmptyItemsNotNil matches Python's
// DrilldownResponse(items=[]) -- an empty result must marshal as
// "items": [], never "items": null.
func TestBuildPRsResponseEmptyItemsNotNil(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	resp, err := BuildPRsResponse(context.Background(), reader, "org-1", PRParams{ScopeLevel: "org", Limit: 50})
	if err != nil {
		t.Fatalf("BuildPRsResponse: %v", err)
	}
	if resp.Items == nil {
		t.Fatalf("Items is nil, want empty non-nil slice")
	}
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != `{"items":[]}` {
		t.Fatalf("marshal = %s, want {\"items\":[]}", body)
	}
}

// TestResolveRepoFilterIDsRepoAndWhatCombined ports the "repo" branch's
// ref ordering (api/services/filtering.py:95-110): scope.ids first, then
// what.repos, both resolved -- not deduped (matching Python's own
// resolve_repo_ids, which never dedupes either).
func TestResolveRepoFilterIDsRepoAndWhatCombined(t *testing.T) {
	var seenRefs []string
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM repos") {
			t.Fatalf("unexpected query: %s", query)
		}
		if v, ok := bindingValue(bindings, "repo_name"); ok {
			ref := v.(string)
			seenRefs = append(seenRefs, ref)
			return &fixtureRowScanner{rows: [][]any{{"resolved-" + ref}}}, nil
		}
		t.Fatalf("expected a repo_name binding, got %+v", bindings)
		return nil, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := reader.ResolveRepoFilterIDs(context.Background(), "repo", []string{"scope-repo"}, []string{"what-repo"}, "org-1")
	if err != nil {
		t.Fatalf("ResolveRepoFilterIDs: %v", err)
	}
	wantRefs := []string{"scope-repo", "what-repo"}
	if len(seenRefs) != len(wantRefs) || seenRefs[0] != wantRefs[0] || seenRefs[1] != wantRefs[1] {
		t.Fatalf("resolved refs in order %v, want %v", seenRefs, wantRefs)
	}
	wantIDs := []string{"resolved-scope-repo", "resolved-what-repo"}
	if len(got) != len(wantIDs) || got[0] != wantIDs[0] || got[1] != wantIDs[1] {
		t.Fatalf("ResolveRepoFilterIDs = %v, want %v", got, wantIDs)
	}
}

// TestResolveRepoFilterIDsUnresolvedRefSkipped matches resolve_repo_ids'
// own silent-skip contract: a ref that resolves to no row is dropped,
// not an error.
func TestResolveRepoFilterIDsUnresolvedRefSkipped(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := reader.ResolveRepoFilterIDs(context.Background(), "repo", []string{"missing-repo"}, nil, "org-1")
	if err != nil {
		t.Fatalf("ResolveRepoFilterIDs: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ResolveRepoFilterIDs = %v, want empty", got)
	}
}

// TestResolveRepoFilterIDsOrgScopeNoRefs matches Python's short circuit:
// scope.level="org" with no what.repos issues zero ClickHouse queries.
func TestResolveRepoFilterIDsOrgScopeNoRefs(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("unexpected query for org scope with no refs: %s", query)
		return nil, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := reader.ResolveRepoFilterIDs(context.Background(), "org", nil, nil, "org-1")
	if err != nil {
		t.Fatalf("ResolveRepoFilterIDs: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ResolveRepoFilterIDs = %v, want empty", got)
	}
}
