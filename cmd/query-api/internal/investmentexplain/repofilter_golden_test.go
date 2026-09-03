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
// (REPOS / USER_METRICS_DAILY) exactly, dispatching on the same
// distinguishing SQL substrings repofilter.go's own queries produce.
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

	case strings.Contains(statement, "FROM user_metrics_daily"):
		teamIDs, _ := binding("team_ids").([]string)
		orgID, _ := binding("org_id").(string)
		wanted := map[string]bool{}
		for _, t := range teamIDs {
			wanted[t] = true
		}
		var seen []string
		seenSet := map[string]bool{}
		for _, row := range repoFilterFixtureUserMetricsDaily {
			if wanted[row.teamID] && row.orgID == orgID && !seenSet[row.repoID] {
				seenSet[row.repoID] = true
				seen = append(seen, row.repoID)
			}
		}
		rows := make([][]any, len(seen))
		for i, id := range seen {
			rows[i] = []any{id}
		}
		return &fixtureRowScanner{rows: rows}, nil
	}
	panic("repoFilterFixtureClient: unexpected query: " + statement)
}

var repoFilterFixtureRepos = []struct{ id, orgID, name string }{
	{repoFilterFixtureRepoOneID, repoFilterFixtureOrgID, "myorg/repo-one"},
	{repoFilterFixtureRepoTwoID, repoFilterFixtureOrgID, "myorg/repo-two"},
	{repoFilterFixtureRepoThreeID, "org-2", "otherorg/repo-three"},
}

var repoFilterFixtureUserMetricsDaily = []struct{ teamID, orgID, repoID string }{
	{"team-a", repoFilterFixtureOrgID, repoFilterFixtureRepoOneID},
	{"team-a", repoFilterFixtureOrgID, repoFilterFixtureRepoTwoID},
	{"team-b", repoFilterFixtureOrgID, repoFilterFixtureRepoOneID},
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
		{"team_scope", "team", []string{"team-a"}, nil},
		{"team_scope_with_what_repos", "team", []string{"team-b"}, []string{"myorg/repo-two"}},
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
