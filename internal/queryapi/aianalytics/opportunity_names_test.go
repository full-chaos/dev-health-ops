package aianalytics

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-8114: aiOpportunities rows carry the repository and team display names
// from the same catalogues aiAttributedPrs reads (CHAOS-7773). The names are
// Go-only fields: the Python detector never had them, so the recorded cases of
// oracle_d_cases.json hold none, and the tests below pin them.
//
// The catalogue of these tests: repository A and B have a full name, repository
// C is in the catalogue with an EMPTY name, and team-b has a name. team-b's
// pattern selects repository B on purpose: an opportunity of repository B that
// carries no team must still have no team name.

const (
	nameRepoA = "11111111-1111-1111-1111-111111111111"
	nameRepoB = "22222222-2222-2222-2222-222222222222"
	nameRepoC = "33333333-3333-3333-3333-333333333333"
)

func namesClientD(t *testing.T, caseName string) *fixtureClientD {
	t.Helper()
	c, ok := loadDCases(t)[caseName]
	if !ok {
		t.Fatalf("no recorded case %q", caseName)
	}
	return &fixtureClientD{
		c:              c,
		catalogueRepos: [][]any{{nameRepoA, "acme/alpha"}, {nameRepoB, "acme/beta"}, {nameRepoC, ""}},
		catalogueTeams: [][]any{{"team-b", "Team B", []string{"acme/beta"}}},
	}
}

func runOpportunities(t *testing.T, client *fixtureClientD) []model.AIOpportunity {
	t.Helper()
	got, err := AiOpportunities(context.Background(), client, "org-1", nil, client.c.Limit)
	if err != nil {
		t.Fatal(err)
	}
	return got.Recommendations
}

func TestAiOpportunities_RowsCarryRepoAndTeamNames(t *testing.T) {
	recs := runOpportunities(t, namesClientD(t, "ai/everything"))
	seen := map[string]int{}
	for _, r := range recs {
		if r.RepoID == nil {
			t.Fatalf("%s has no repository id", r.OpportunityID)
		}
		switch *r.RepoID {
		case nameRepoA:
			seen["A"]++
			if nameOrNil(r.RepoName) != "acme/alpha" {
				t.Errorf("%s: repoName = %s, want acme/alpha", r.Kind, nameOrNil(r.RepoName))
			}
		case nameRepoB:
			seen["B"]++
			if nameOrNil(r.RepoName) != "acme/beta" {
				t.Errorf("%s: repoName = %s, want acme/beta", r.Kind, nameOrNil(r.RepoName))
			}
		case nameRepoC:
			seen["C"]++
			if r.RepoName != nil {
				t.Errorf("%s: repoName = %q for a repository with an empty catalogue name, want nil", r.Kind, *r.RepoName)
			}
		default:
			t.Errorf("unexpected repository %s", *r.RepoID)
		}
		if r.TeamID != nil {
			seen["team"]++
			if *r.TeamID != "team-b" || nameOrNil(r.TeamName) != "Team B" {
				t.Errorf("%s: team = %s / %s, want team-b / Team B", r.Kind, *r.TeamID, nameOrNil(r.TeamName))
			}
			continue
		}
		seen["no team"]++
		if *r.RepoID == nameRepoB {
			seen["no team on B"]++
		}
		// The team name is the name of the row's OWN team id. A row with no team has
		// no team name, even on a repository a team's pattern selects.
		if r.TeamName != nil {
			t.Errorf("%s on %s: teamName = %q for a row with no team, want nil", r.Kind, *r.RepoID, *r.TeamName)
		}
	}
	for _, want := range []string{"A", "B", "C", "team", "no team", "no team on B"} {
		if seen[want] == 0 {
			t.Errorf("the fixture gave no %q row: nothing was measured for it (%v)", want, seen)
		}
	}
}

// An id is never offered as a name: a repository the catalogue does not hold,
// a team the catalogue does not hold and a team with an empty name all give nil.
func TestAiOpportunities_ANameIsNeverTheID(t *testing.T) {
	for name, teams := range map[string][][]any{
		"team not in the catalogue": {{"team-other", "Other", []string{}}},
		"team with an empty name":   {{"team-b", "", []string{}}},
	} {
		t.Run(name, func(t *testing.T) {
			client := namesClientD(t, "ai/everything")
			client.catalogueRepos = [][]any{{nameRepoA, "acme/alpha"}}
			client.catalogueTeams = teams
			teamed, unnamed := 0, 0
			for _, r := range runOpportunities(t, client) {
				if *r.RepoID != nameRepoA {
					unnamed++
					if r.RepoName != nil {
						t.Errorf("%s: repoName = %q for repository %s, which the catalogue does not hold", r.Kind, *r.RepoName, *r.RepoID)
					}
				}
				if r.TeamID != nil {
					teamed++
					if r.TeamName != nil {
						t.Errorf("%s: teamName = %q, want nil; the team id %s stays", r.Kind, *r.TeamName, *r.TeamID)
					}
				}
			}
			if teamed == 0 || unnamed == 0 {
				t.Fatalf("nothing measured: %d row(s) with a team, %d row(s) of an unknown repository", teamed, unnamed)
			}
		})
	}
}

// Team = the id the rollup row carries, whatever provider the team came from:
// the name read does not look at the provider or at the shape of the id.
func TestAiOpportunities_TeamNamesForTeamsOfEveryProvider(t *testing.T) {
	teams := []struct{ provider, id, name, repo string }{
		{"github", "gh:acme/platform", "Platform", "aaaaaaaa-0000-4000-8000-000000000001"},
		{"gitlab", "gl:group/payments", "Payments", "aaaaaaaa-0000-4000-8000-000000000002"},
		{"jira", "jira:PLAT", "Jira Platform", "aaaaaaaa-0000-4000-8000-000000000003"},
		{"linear", "linear:ENG", "Engineering", "aaaaaaaa-0000-4000-8000-000000000004"},
	}
	client := &fixtureClientD{c: oracleDCase{Name: "names/providers", Kind: "ai", Limit: 25}}
	for _, team := range teams {
		// Ten human pull requests, six with a test gap: the test-generation rule fires.
		client.c.Impact = append(client.c.Impact, map[string]any{
			"team_id": team.id, "repo_id": team.repo, "attribution_bucket": "human",
			"prs_total": float64(10), "rework_prs": float64(0), "test_gap_prs": float64(6),
		})
		client.catalogueTeams = append(client.catalogueTeams, []any{team.id, team.name, []string{}})
		client.catalogueRepos = append(client.catalogueRepos, []any{team.repo, "acme/" + team.provider})
	}
	recs := runOpportunities(t, client)
	if len(recs) != len(teams) {
		t.Fatalf("want one opportunity per team, got %d: %+v", len(recs), recs)
	}
	byTeam := map[string]model.AIOpportunity{}
	for _, r := range recs {
		byTeam[nameOrNil(r.TeamID)] = r
	}
	for _, team := range teams {
		r, ok := byTeam[team.id]
		if !ok {
			t.Errorf("%s: no opportunity for team %s", team.provider, team.id)
			continue
		}
		if nameOrNil(r.TeamName) != team.name {
			t.Errorf("%s: teamName = %s, want %s", team.provider, nameOrNil(r.TeamName), team.name)
		}
		if nameOrNil(r.RepoName) != "acme/"+team.provider {
			t.Errorf("%s: repoName = %s, want acme/%s", team.provider, nameOrNil(r.RepoName), team.provider)
		}
	}
}

// The names are read once, for the returned page only, after the limit cut, and
// both reads are bound to the caller's org.
func TestAiOpportunities_NamesAreReadForTheReturnedPageOnly(t *testing.T) {
	for caseName, wantIDs := range map[string][]string{
		// HIGH_REWORK on A, then MECHANICAL_MIGRATIONS on C: the cut leaves B out.
		"ai/limitTwo": {nameRepoA, nameRepoC},
		// Fourteen rows over three repositories: each id once, in first-seen order.
		"ai/everything": {nameRepoA, nameRepoC, nameRepoB},
	} {
		t.Run(caseName, func(t *testing.T) {
			client := namesClientD(t, caseName)
			runOpportunities(t, client)
			if len(client.catalogueStatements) != 2 {
				t.Fatalf("want one teams read and one repository read, got %d:\n%s", len(client.catalogueStatements), strings.Join(client.catalogueStatements, "\n--\n"))
			}
			reads := map[string]int{}
			for i, st := range client.catalogueStatements {
				which := catalogueNameRead(st)
				reads[which]++
				if !strings.HasPrefix(st, "SELECT") || strings.Contains(st, ";") {
					t.Errorf("%s read is not a single SELECT:\n%s", which, st)
				}
				if !strings.Contains(st, "org_id = {org_id:String}") {
					t.Errorf("%s read has no org predicate:\n%s", which, st)
				}
				if v, _ := binding(client.catalogueBindings[i], "org_id"); v != "org-1" {
					t.Errorf("%s read binds org_id=%v, want org-1", which, v)
				}
				if which == "repos" {
					got, _ := binding(client.catalogueBindings[i], "repo_ids")
					if !reflect.DeepEqual(got, wantIDs) {
						t.Errorf("repo_ids = %v, want the page's repositories %v", got, wantIDs)
					}
				}
			}
			if reads["teams"] != 1 || reads["repos"] != 1 {
				t.Errorf("reads = %v, want one teams read and one repository read", reads)
			}
			// The name reads are kept apart from the detector reads the Python calls are compared with.
			for _, st := range client.statements {
				if catalogueNameRead(st) != "" {
					t.Errorf("a name read is recorded among the detector reads:\n%s", st)
				}
			}
		})
	}
}

// No opportunity, no name to read.
func TestAiOpportunities_NoOpportunitiesReadNoNames(t *testing.T) {
	for _, caseName := range []string{"ai/none", "ai/repoUnresolved"} {
		client := namesClientD(t, caseName)
		in := (*model.AIScopeInput)(nil)
		if client.c.Scope != nil {
			in = &model.AIScopeInput{RepoID: client.c.Scope.RepoID, TeamID: client.c.Scope.TeamID}
		}
		got, err := AiOpportunities(context.Background(), client, "org-1", in, client.c.Limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Recommendations) != 0 {
			t.Fatalf("%s: want no opportunity, got %d", caseName, len(got.Recommendations))
		}
		if len(client.catalogueStatements) != 0 {
			t.Errorf("%s: %d name read(s) for an empty answer", caseName, len(client.catalogueStatements))
		}
	}
}

// Missing is not healthy, and a name is a label, not a number: a catalogue read
// that fails leaves the names it would have given null and keeps the rows. The
// two reads fail apart: the team name needs the teams read only.
func TestAiOpportunities_AFailedCatalogueReadKeepsTheRows(t *testing.T) {
	whole := runOpportunities(t, namesClientD(t, "ai/everything"))

	t.Run("repository read fails", func(t *testing.T) {
		client := namesClientD(t, "ai/everything")
		client.catalogueReposErr = errors.New("boom")
		recs := runOpportunities(t, client)
		if len(recs) != len(whole) {
			t.Fatalf("want the %d rows, got %d", len(whole), len(recs))
		}
		teamed := 0
		for _, r := range recs {
			if r.RepoName != nil {
				t.Errorf("%s: repoName = %q after a failed repository read, want nil", r.Kind, *r.RepoName)
			}
			if r.TeamID != nil {
				teamed++
				if nameOrNil(r.TeamName) != "Team B" {
					t.Errorf("%s: teamName = %s, want Team B (the teams read did not fail)", r.Kind, nameOrNil(r.TeamName))
				}
			}
		}
		if teamed == 0 {
			t.Fatal("the fixture gave no row with a team")
		}
	})

	t.Run("teams read fails", func(t *testing.T) {
		client := namesClientD(t, "ai/everything")
		client.catalogueTeamsErr = errors.New("boom")
		recs := runOpportunities(t, client)
		if len(recs) != len(whole) {
			t.Fatalf("want the %d rows, got %d", len(whole), len(recs))
		}
		named := 0
		for _, r := range recs {
			if r.TeamName != nil {
				t.Errorf("%s: teamName = %q after a failed teams read, want nil", r.Kind, *r.TeamName)
			}
			if *r.RepoID == nameRepoA {
				named++
				if nameOrNil(r.RepoName) != "acme/alpha" {
					t.Errorf("%s: repoName = %s, want acme/alpha (the repository read did not fail)", r.Kind, nameOrNil(r.RepoName))
				}
			}
		}
		if named == 0 {
			t.Fatal("the fixture gave no row of repository A")
		}
	})
}

// The names decide nothing: with them or without them the answer holds the same
// opportunities, in the same order, with the same ids, scores and texts.
func TestAiOpportunities_NamesChangeNothingElse(t *testing.T) {
	strip := func(recs []model.AIOpportunity) []map[string]any {
		raw, err := json.Marshal(recs)
		if err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		for _, o := range out {
			delete(o, "repoName")
			delete(o, "teamName")
		}
		return out
	}
	c, ok := loadDCases(t)["ai/everything"]
	if !ok {
		t.Fatal("no recorded case ai/everything")
	}
	bare := runOpportunities(t, &fixtureClientD{c: c})
	for _, r := range bare {
		if r.RepoName != nil || r.TeamName != nil {
			t.Fatalf("an empty catalogue gave a name: %s / %s", nameOrNil(r.RepoName), nameOrNil(r.TeamName))
		}
	}
	named := runOpportunities(t, namesClientD(t, "ai/everything"))
	if len(named) == 0 || !reflect.DeepEqual(strip(named), strip(bare)) {
		t.Fatalf("the names changed the answer:\nwith names    %v\nwithout names %v", strip(named), strip(bare))
	}
}
