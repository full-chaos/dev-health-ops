package aianalytics

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-7773: aiAttributedPrs rows carry the repository and team display names
// from the catalogues the resolver already reads. The names are Go-only fields
// (the Python reference never had them), so the oracle strips exactly these two
// (see goOnlyAttributedPrRowFields) and the tests below pin them instead.

func namesCase(t *testing.T, name string) oracleBCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/oracle_b_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleBCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Cases {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no oracle case %q", name)
	return oracleBCase{}
}

func runNames(t *testing.T, c oracleBCase, scope *model.AIScopeInput) *model.AiAttributedPrsResult {
	t.Helper()
	client := &fixtureClientB{
		fixtureClient: &fixtureClient{c: oracleCase{TeamRows: c.TeamRows, RepoRows: c.RepoRows}, ds: dataset{Daily: c.Daily}},
		c:             c,
	}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	got, err := AttributedPrs(context.Background(), client, "org-1", dr, scope, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func str(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func TestAttributedPrs_RowsCarryRepoAndTeamNames(t *testing.T) {
	got := runNames(t, namesCase(t, "prs/plain"), nil)
	if len(got.Rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(got.Rows))
	}
	for _, r := range got.Rows {
		if str(r.RepoName) != "acme/alpha" {
			t.Errorf("repoName = %s, want acme/alpha (repo %s)", str(r.RepoName), r.RepoID)
		}
		if str(r.TeamID) != "team-a" || str(r.TeamName) != "Team A" {
			t.Errorf("team = %s / %s, want team-a / Team A", str(r.TeamID), str(r.TeamName))
		}
	}
}

func TestAttributedPrs_TeamFilterKeepsTheNamesOfTheKeptRows(t *testing.T) {
	team := "team-b"
	got := runNames(t, namesCase(t, "prs/teamFilter"), &model.AIScopeInput{TeamID: &team})
	if len(got.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(got.Rows))
	}
	r := got.Rows[0]
	if str(r.RepoName) != "Acme/Beta" || str(r.TeamID) != "team-b" || str(r.TeamName) != "B" {
		t.Fatalf("row = repoName %s, team %s / %s", str(r.RepoName), str(r.TeamID), str(r.TeamName))
	}
}

// A repository whose catalogue full name is empty has NO name: the id is never
// offered as a name.
func TestAttributedPrs_EmptyRepoNameIsNilNotTheID(t *testing.T) {
	got := runNames(t, namesCase(t, "prs/teamFilter"), nil)
	seen := false
	for _, r := range got.Rows {
		if r.RepoID == "33333333-3333-3333-3333-333333333333" {
			seen = true
			if r.RepoName != nil {
				t.Fatalf("repoName = %q for a repository with no catalogue name, want nil", *r.RepoName)
			}
		}
	}
	if !seen {
		t.Fatal("fixture has no row for the nameless repository")
	}
}

// A team with an empty name has no name: the team id stays, the name is nil.
func TestAttributedPrs_EmptyTeamNameIsNil(t *testing.T) {
	c := namesCase(t, "prs/plain")
	c.TeamRows = append([]map[string]any(nil), c.TeamRows...)
	first := map[string]any{}
	for k, v := range c.TeamRows[0] {
		first[k] = v
	}
	first["name"] = ""
	c.TeamRows[0] = first
	got := runNames(t, c, nil)
	for _, r := range got.Rows {
		if str(r.TeamID) != "team-a" {
			t.Fatalf("teamId = %s, want team-a", str(r.TeamID))
		}
		if r.TeamName != nil {
			t.Fatalf("teamName = %q, want nil for a team with no name", *r.TeamName)
		}
	}
}

// Missing is not healthy: a failed repository read leaves no name and no team,
// and the rows still come back.
func TestAttributedPrs_RepoCatalogueFailureLeavesNoNames(t *testing.T) {
	c := namesCase(t, "prs/plain")
	c.NameQueryFails = true
	got := runNames(t, c, nil)
	if len(got.Rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(got.Rows))
	}
	for _, r := range got.Rows {
		if r.RepoName != nil || r.TeamID != nil || r.TeamName != nil {
			t.Fatalf("row = %s / %s / %s, want no names and no team", str(r.RepoName), str(r.TeamID), str(r.TeamName))
		}
	}
}

// A failed teams read still returns the repository names.
func TestAttributedPrs_TeamCatalogueFailureKeepsRepoNames(t *testing.T) {
	got := runNames(t, namesCase(t, "prs/teamCatalogueFails"), nil)
	if len(got.Rows) == 0 {
		t.Fatal("no rows")
	}
	named := false
	for _, r := range got.Rows {
		if r.TeamID != nil || r.TeamName != nil {
			t.Fatalf("team = %s / %s, want none when the teams read fails", str(r.TeamID), str(r.TeamName))
		}
		if r.RepoName != nil {
			named = true
		}
	}
	if !named {
		t.Fatal("no row kept its repository name when only the teams read failed")
	}
}

// The oracle strips exactly the two Go-only row fields. A third stripped field
// would let a real divergence from the Python reference pass, so this fails if the
// list grows or changes.
func TestOracleStripsOnlyTheTwoGoOnlyFields(t *testing.T) {
	want := []string{"repoName", "teamName"}
	if len(goOnlyAttributedPrRowFields) != len(want) {
		t.Fatalf("goOnlyAttributedPrRowFields = %v, want exactly %v", goOnlyAttributedPrRowFields, want)
	}
	for i, f := range want {
		if goOnlyAttributedPrRowFields[i] != f {
			t.Fatalf("goOnlyAttributedPrRowFields = %v, want exactly %v", goOnlyAttributedPrRowFields, want)
		}
	}
	// And the Python reference never had them: nothing recorded carries them.
	raw, err := os.ReadFile("testdata/oracle_b_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleBCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Cases {
		rows, _ := c.Expected["rows"].([]any)
		for _, r := range rows {
			m, _ := r.(map[string]any)
			for _, f := range goOnlyAttributedPrRowFields {
				if _, ok := m[f]; ok {
					t.Fatalf("case %s: the recorded Python response has %q; it is not Go-only", c.Name, f)
				}
			}
		}
	}
}

// The strip removes the two fields and nothing else.
func TestStripGoOnlyRowFieldsRemovesOnlyThose(t *testing.T) {
	m := map[string]any{"rows": []any{map[string]any{"repoId": "r", "repoName": "n", "teamName": "t", "teamId": "x", "title": "y"}}}
	stripGoOnlyAttributedPrRowFields(m)
	row := m["rows"].([]any)[0].(map[string]any)
	if _, ok := row["repoName"]; ok {
		t.Fatal("repoName not stripped")
	}
	if _, ok := row["teamName"]; ok {
		t.Fatal("teamName not stripped")
	}
	for _, k := range []string{"repoId", "teamId", "title"} {
		if _, ok := row[k]; !ok {
			t.Fatalf("%s was stripped", k)
		}
	}
}
