package investmentexplain

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// repoFilterFixtureOrgID matches
// generate_resolve_repo_filter_ids_golden.py's ORG_ID exactly.
const repoFilterFixtureOrgID = "org-1"

const (
	repoFilterFixtureRepoOneID   = "11111111-1111-4111-8111-111111111111"
	repoFilterFixtureRepoTwoID   = "22222222-2222-4222-8222-222222222222"
	repoFilterFixtureRepoThreeID = "33333333-3333-4333-8333-333333333333"
)

// repoFilterFixtureClient mirrors the Python generator's in-memory fixture
// (REPOS) exactly, dispatching on the same distinguishing SQL substrings
// repofilter.go's own explicit-ref queries produce. ResolveRepoFilterIDs
// issues only these repos lookups, never a user_metrics_daily query (see
// this package's TeamRepoScopeCondition/repofilter.go doc comments: a
// team's repo membership is a condition the CALLER's own statement
// evaluates, never resolved here as a standalone, organization-scale
// query), so this fixture only needs to answer the repos lookups
// explicit refs make.
type repoFilterFixtureClient struct{}

func (repoFilterFixtureClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	binding := func(name string) any {
		for _, b := range bindings {
			if b.Name == name {
				return b.Value
			}
		}
		return nil
	}

	switch {
	case strings.Contains(statement, "FROM repos") && strings.Contains(statement, "WHERE toString(id) ="):
		repoID, _ := binding("repo_id").(string)
		orgID, _ := binding("org_id").(string)
		for _, r := range repoFilterFixtureRepos {
			if r.id == repoID && r.orgID == orgID {
				return &fixtureRowScanner{rows: [][]any{{r.id}}}, nil
			}
		}
		return &fixtureRowScanner{}, nil

	case strings.Contains(statement, "FROM repos") && strings.Contains(statement, "WHERE repo ="):
		repoName, _ := binding("repo_name").(string)
		orgID, _ := binding("org_id").(string)
		for _, r := range repoFilterFixtureRepos {
			if r.name == repoName && r.orgID == orgID {
				return &fixtureRowScanner{rows: [][]any{{r.id}}}, nil
			}
		}
		return &fixtureRowScanner{}, nil
	}
	panic("repoFilterFixtureClient: unexpected query: " + statement)
}

var repoFilterFixtureRepos = []struct{ id, orgID, name string }{
	{repoFilterFixtureRepoOneID, repoFilterFixtureOrgID, "myorg/repo-one"},
	{repoFilterFixtureRepoTwoID, repoFilterFixtureOrgID, "myorg/repo-two"},
	{repoFilterFixtureRepoThreeID, "org-2", "otherorg/repo-three"},
}

type resolveRepoFilterIDsGolden struct {
	Case     string   `json:"case"`
	Resolved []string `json:"resolved"`
}

func loadResolveRepoFilterIDsGolden(t *testing.T, name string) resolveRepoFilterIDsGolden {
	t.Helper()
	path := filepath.Join("testdata", "resolve_repo_filter_ids__"+name+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var golden resolveRepoFilterIDsGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("unmarshal golden %s: %v", path, err)
	}
	return golden
}

// TestResolveRepoFilterIDsMatchesPythonGolden covers ResolveRepoFilterIDs'
// EXPLICIT-ref cases only (org/repo scope, what.repos) -- the cases where
// this Go function's contract matches resolve_repo_filter_ids'
// (api/services/filtering.py:95-110) own materialized-list return shape.
// The Python source's team-scope branch is covered elsewhere, as a
// pushed-down SQL condition: TestTeamRepoScopeConditionQueryShape
// (sqlshape_test.go) proves its structural correctness against a fake
// client, and the team_scope_large_repo_set_integration_test.go suite
// proves its behavioral correctness against a real ClickHouse engine. A
// fake, in-memory client like repoFilterFixtureClient cannot evaluate a
// condition nested inside another caller's own SQL statement, so that
// branch's own correctness is not checked here.
func TestResolveRepoFilterIDsMatchesPythonGolden(t *testing.T) {
	cases := []struct {
		name       string
		scopeLevel string
		scopeIDs   []string
		whatRepos  []string
	}{
		{"org_scope_no_repos", "org", nil, nil},
		{"repo_scope_mixed_uuid_and_slug", "repo", []string{repoFilterFixtureRepoOneID, "myorg/repo-two"}, nil},
		{"repo_scope_unresolvable_slug_skipped", "repo", []string{"myorg/repo-one", "nonexistent/repo"}, nil},
		{"org_scope_with_what_repos", "org", nil, []string{"myorg/repo-one"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			golden := loadResolveRepoFilterIDsGolden(t, tc.name)
			reader, err := NewReader(repoFilterFixtureClient{})
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			got, err := reader.ResolveRepoFilterIDs(context.Background(), tc.scopeLevel, tc.scopeIDs, tc.whatRepos, repoFilterFixtureOrgID)
			if err != nil {
				t.Fatalf("ResolveRepoFilterIDs: %v", err)
			}
			if len(got) != len(golden.Resolved) {
				t.Fatalf("resolved = %v, want %v", got, golden.Resolved)
			}
			for i := range got {
				if got[i] != golden.Resolved[i] {
					t.Fatalf("resolved = %v, want %v", got, golden.Resolved)
				}
			}
		})
	}
}
