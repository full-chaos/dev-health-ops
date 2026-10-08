package aianalytics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-8954: the attribution and governance rows carry the names of their
// repository, team and pull request, and governance rows the rule name. The names
// are Go-only fields (the Python reference never had them); the oracle strips
// goOnlyAttributionRowFields and the tests below pin them.

const (
	repoAlpha = "11111111-1111-1111-1111-111111111111"
	repoBeta  = "22222222-2222-2222-2222-222222222222"
)

// titleRow is one stored pull request of an org.
type titleRow struct {
	org    string
	repoID string
	number uint32
	title  string
}

// titleClient answers the pull request title read from stored rows, honouring the
// org_id binding the statement carries, and passes every other statement on.
type titleClient struct {
	QueryClient
	prs   []titleRow
	fail  bool
	calls int
	text  string
}

func (c *titleClient) Query(ctx context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if !strings.Contains(st, "FROM git_pull_requests") {
		return c.QueryClient.Query(ctx, st, b)
	}
	c.calls++
	c.text = st
	if c.fail {
		return nil, context.DeadlineExceeded
	}
	var org string
	var repos []string
	for _, bd := range b {
		switch bd.Name {
		case "org_id":
			org, _ = bd.Value.(string)
		case "repo_ids":
			repos, _ = bd.Value.([]string)
		}
	}
	var rows [][]any
	for _, r := range c.prs {
		if r.org != org {
			continue
		}
		for _, id := range repos {
			if id == r.repoID {
				rows = append(rows, []any{r.repoID, r.number, r.title})
			}
		}
	}
	return &scriptedRows{rows: rows}, nil
}

func overviewWith(t *testing.T, prs []titleRow, fail bool) (*model.AIAttributionOverviewResult, *titleClient) {
	t.Helper()
	c := namesCase(t, "overview/plain")
	inner := &fixtureClientB{
		fixtureClient: &fixtureClient{c: oracleCase{TeamRows: c.TeamRows, RepoRows: c.RepoRows}, ds: dataset{Daily: c.Daily}},
		c:             c,
	}
	client := &titleClient{QueryClient: inner, prs: prs, fail: fail}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	got, err := AttributionOverview(context.Background(), client, "org-1", dr, nil, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	return got, client
}

func attributionRow(t *testing.T, res *model.AIAttributionOverviewResult, repoID string) model.AIAttributionEvidenceRow {
	t.Helper()
	for _, r := range res.Rows {
		if r.RepoID != nil && *r.RepoID == repoID {
			return r
		}
	}
	t.Fatalf("no row for repository %s", repoID)
	return model.AIAttributionEvidenceRow{}
}

func TestAttributionOverview_RowsCarryRepoTeamAndSubjectTitle(t *testing.T) {
	res, client := overviewWith(t, []titleRow{{"org-1", repoAlpha, 7, "Add retry"}, {"org-1", repoBeta, 6, "Fix flake"}}, false)
	a := attributionRow(t, res, repoAlpha)
	if nameOrNil(a.RepoName) != "acme/alpha" || nameOrNil(a.TeamName) != "Team A" || nameOrNil(a.SubjectTitle) != "Add retry" {
		t.Fatalf("row = %s / %s / %s", nameOrNil(a.RepoName), nameOrNil(a.TeamName), nameOrNil(a.SubjectTitle))
	}
	if !strings.Contains(client.text, "org_id = {org_id:String}") {
		t.Fatalf("pull request title read is not bound to the org:\n%s", client.text)
	}
}

func TestAttributionOverview_NoStoredTitleIsNilNeverTheID(t *testing.T) {
	res, _ := overviewWith(t, []titleRow{{"org-1", repoAlpha, 7, ""}}, false)
	a := attributionRow(t, res, repoAlpha)
	if a.SubjectTitle != nil {
		t.Fatalf("subjectTitle = %q, want nil for a pull request with no stored title", *a.SubjectTitle)
	}
	b := attributionRow(t, res, repoBeta)
	if b.SubjectTitle != nil {
		t.Fatalf("subjectTitle = %q, want nil for a pull request with no stored row", *b.SubjectTitle)
	}
}

func TestAttributionOverview_AnotherOrgsTitleIsNeverServed(t *testing.T) {
	res, _ := overviewWith(t, []titleRow{{"org-2", repoAlpha, 7, "Other org title"}}, false)
	if a := attributionRow(t, res, repoAlpha); a.SubjectTitle != nil {
		t.Fatalf("subjectTitle = %q served from another org", *a.SubjectTitle)
	}
}

func TestAttributionOverview_NonPullRequestSubjectHasNoTitle(t *testing.T) {
	res, _ := overviewWith(t, []titleRow{{"org-1", repoAlpha, 7, "Add retry"}}, false)
	for _, r := range res.Rows {
		if r.SubjectType != "pull_request" && r.SubjectTitle != nil {
			t.Fatalf("subjectType %s carries title %q", r.SubjectType, *r.SubjectTitle)
		}
	}
}

func TestAttributionOverview_TitleReadFailureLeavesRowsServed(t *testing.T) {
	res, client := overviewWith(t, nil, true)
	if client.calls == 0 {
		t.Fatal("the title read never ran")
	}
	a := attributionRow(t, res, repoAlpha)
	if a.SubjectTitle != nil || nameOrNil(a.RepoName) != "acme/alpha" {
		t.Fatalf("row = repo %s, title %s", nameOrNil(a.RepoName), nameOrNil(a.SubjectTitle))
	}
}

func TestPrSubjectKey(t *testing.T) {
	repo := repoAlpha
	cases := []struct {
		typ, id string
		repo    *string
		want    bool
		number  uint32
	}{
		{"pull_request", "7", &repo, true, 7},
		{"pull_request", repoAlpha + "#12", &repo, true, 12},
		{"pull_request", repoAlpha + ":12", &repo, true, 12},
		{"pull_request", repoBeta + "#12", &repo, false, 0},
		{"pull_request", "0", &repo, false, 0},
		{"pull_request", "ABC-1", &repo, false, 0},
		{"pull_request", "7", nil, false, 0},
		{"issue", "7", &repo, false, 0},
	}
	for _, c := range cases {
		k, ok := prSubjectKey(c.typ, c.id, c.repo)
		if ok != c.want || (ok && k.number != c.number) {
			t.Errorf("prSubjectKey(%s, %s) = %v %v, want %v %d", c.typ, c.id, k, ok, c.want, c.number)
		}
	}
}

func TestPolicyRuleNames(t *testing.T) {
	for id, want := range policyRuleNames {
		if got := policyRuleName(id); got == nil || *got != want || *got == id {
			t.Errorf("rule %s -> %v", id, got)
		}
	}
	if policyRuleName("SOMETHING_NEW") != nil {
		t.Fatal("a rule outside the code-owned set must have no name")
	}
	if len(policyRuleNames) != 6 {
		t.Fatalf("policyRuleNames has %d rules, aigovernance defines 6", len(policyRuleNames))
	}
}

func TestOracleStripsOnlyTheAttributionNameFields(t *testing.T) {
	want := []string{"repoName", "teamName", "subjectTitle"}
	if strings.Join(goOnlyAttributionRowFields, ",") != strings.Join(want, ",") {
		t.Fatalf("goOnlyAttributionRowFields = %v, want exactly %v", goOnlyAttributionRowFields, want)
	}
}

// catalogueClient scripts the violations, team, repository and pull request title
// reads, honouring the org_id binding on each.
type catalogueClient struct {
	violations [][]any
	teams      []teamRow
	repos      []repoRow
	prs        []titleRow
	failTeams  bool
}

type teamRow struct{ org, id, name string }
type repoRow struct{ org, id, name string }

func (c *catalogueClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	var org string
	var repoIDs []string
	for _, bd := range b {
		switch bd.Name {
		case "org_id":
			org, _ = bd.Value.(string)
		case "repo_ids":
			repoIDs, _ = bd.Value.([]string)
		}
	}
	if strings.Contains(st, "FROM teams") || strings.Contains(st, "FROM repos") || strings.Contains(st, "FROM git_pull_requests") {
		if !strings.Contains(st, "org_id = {org_id:String}") {
			return nil, context.Canceled
		}
	}
	switch {
	case strings.Contains(st, "FROM ai_governance_coverage_daily"):
		return &scriptedRows{}, nil
	case strings.Contains(st, "FROM ai_policy_events"):
		return &scriptedRows{rows: c.violations}, nil
	case strings.Contains(st, "FROM teams"):
		if c.failTeams {
			return nil, context.DeadlineExceeded
		}
		var rows [][]any
		for _, t := range c.teams {
			if t.org == org {
				rows = append(rows, []any{t.id, t.name, []string{}})
			}
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM repos"):
		var rows [][]any
		for _, r := range c.repos {
			if r.org != org {
				continue
			}
			for _, id := range repoIDs {
				if id == r.id {
					rows = append(rows, []any{r.id, r.name})
				}
			}
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM git_pull_requests"):
		var rows [][]any
		for _, r := range c.prs {
			if r.org != org {
				continue
			}
			for _, id := range repoIDs {
				if id == r.repoID {
					rows = append(rows, []any{r.repoID, r.number, r.title})
				}
			}
		}
		return &scriptedRows{rows: rows}, nil
	}
	return nil, context.Canceled
}

func violation(team, repo any, rule, subjectType, subjectID string) []any {
	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	return []any{team, repo, rule, "high", subjectType, subjectID, ts, "{}"}
}

func governanceWith(t *testing.T, c *catalogueClient) []model.AIGovernanceViolationRow {
	t.Helper()
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	got, err := GovernanceSummary(context.Background(), c, "org-1", dr, nil, 50)
	if err != nil {
		t.Fatal(err)
	}
	return got.RecentViolations
}

func TestGovernanceViolations_RowsCarryNames(t *testing.T) {
	rows := governanceWith(t, &catalogueClient{
		violations: [][]any{
			violation("team-a", repoAlpha, "MISSING_HUMAN_REVIEW", "pull_request", "7"),
			violation("team-a", repoAlpha, "BRAND_NEW_RULE", "commit", "abc"),
			violation(nil, nil, "DISALLOWED_TOOL", "pull_request", "9"),
		},
		teams: []teamRow{{"org-1", "team-a", "Team A"}},
		repos: []repoRow{{"org-1", repoAlpha, "acme/alpha"}},
		prs:   []titleRow{{"org-1", repoAlpha, 7, "Add retry"}},
	})
	if len(rows) != 3 {
		t.Fatalf("rows = %d", len(rows))
	}
	r := rows[0]
	if nameOrNil(r.RepoName) != "acme/alpha" || nameOrNil(r.TeamName) != "Team A" ||
		nameOrNil(r.SubjectTitle) != "Add retry" || nameOrNil(r.RuleName) != "Missing human review" {
		t.Fatalf("row 0 = %s / %s / %s / %s", nameOrNil(r.RepoName), nameOrNil(r.TeamName), nameOrNil(r.SubjectTitle), nameOrNil(r.RuleName))
	}
	if rows[1].RuleName != nil || rows[1].SubjectTitle != nil {
		t.Fatalf("unknown rule / commit subject must have no name: %s / %s", nameOrNil(rows[1].RuleName), nameOrNil(rows[1].SubjectTitle))
	}
	if rows[2].RepoName != nil || rows[2].TeamName != nil || rows[2].SubjectTitle != nil {
		t.Fatalf("a row with no repository and no team must have neither name nor title")
	}
	if nameOrNil(rows[2].RuleName) != "Disallowed AI tool" {
		t.Fatalf("rule name = %s", nameOrNil(rows[2].RuleName))
	}
}

func TestGovernanceViolations_TeamOnlyRowStillNamesItsTeam(t *testing.T) {
	rows := governanceWith(t, &catalogueClient{
		violations: [][]any{violation("team-a", nil, "MISSING_SECURITY_SCAN", "pull_request", "9")},
		teams:      []teamRow{{"org-1", "team-a", "Team A"}},
	})
	if nameOrNil(rows[0].TeamName) != "Team A" {
		t.Fatalf("teamName = %s", nameOrNil(rows[0].TeamName))
	}
}

func TestGovernanceViolations_AnotherOrgsNamesAreNeverServed(t *testing.T) {
	rows := governanceWith(t, &catalogueClient{
		violations: [][]any{violation("team-a", repoAlpha, "MISSING_HUMAN_REVIEW", "pull_request", "7")},
		teams:      []teamRow{{"org-2", "team-a", "Other team"}},
		repos:      []repoRow{{"org-2", repoAlpha, "other/alpha"}},
		prs:        []titleRow{{"org-2", repoAlpha, 7, "Other title"}},
	})
	r := rows[0]
	if r.RepoName != nil || r.TeamName != nil || r.SubjectTitle != nil {
		t.Fatalf("names served across orgs: %s / %s / %s", nameOrNil(r.RepoName), nameOrNil(r.TeamName), nameOrNil(r.SubjectTitle))
	}
}

func TestGovernanceViolations_NamelessCatalogueRowsAreNilNeverTheID(t *testing.T) {
	rows := governanceWith(t, &catalogueClient{
		violations: [][]any{violation("team-a", repoAlpha, "MISSING_HUMAN_REVIEW", "pull_request", "7")},
		teams:      []teamRow{{"org-1", "team-a", ""}},
		repos:      []repoRow{{"org-1", repoAlpha, ""}},
	})
	if rows[0].RepoName != nil || rows[0].TeamName != nil {
		t.Fatalf("empty catalogue names must read as absent: %s / %s", nameOrNil(rows[0].RepoName), nameOrNil(rows[0].TeamName))
	}
}

func TestFlowOpportunities_NameTheirEntity(t *testing.T) {
	c := &catalogueClient{
		teams: []teamRow{{"org-1", "team-a", "Team A"}, {"org-2", "team-z", "Other"}},
		repos: []repoRow{{"org-1", repoAlpha, "acme/alpha"}, {"org-2", repoBeta, "other/beta"}},
	}
	opps := []model.ImproveOpportunity{
		{EntityType: "repo", EntityID: repoAlpha},
		{EntityType: "repo", EntityID: repoBeta},
		{EntityType: "team", EntityID: "team-a"},
		{EntityType: "team", EntityID: "team-z"},
		{EntityType: "team", EntityID: "team-missing"},
	}
	nameFlowOpportunities(context.Background(), c, "org-1", opps)
	got := []string{}
	for _, o := range opps {
		got = append(got, nameOrNil(o.EntityDisplayName))
	}
	if strings.Join(got, "|") != "acme/alpha|<nil>|Team A|<nil>|<nil>" {
		t.Fatalf("entityDisplayName = %v", got)
	}
}

func TestFlowOpportunities_TeamOnlyPageStillNamesTeams(t *testing.T) {
	c := &catalogueClient{teams: []teamRow{{"org-1", "team-a", "Team A"}}}
	opps := []model.ImproveOpportunity{{EntityType: "team", EntityID: "team-a"}}
	nameFlowOpportunities(context.Background(), c, "org-1", opps)
	if nameOrNil(opps[0].EntityDisplayName) != "Team A" {
		t.Fatalf("entityDisplayName = %s", nameOrNil(opps[0].EntityDisplayName))
	}
}

func TestFlowOpportunities_CatalogueFailureLeavesRowsServed(t *testing.T) {
	c := &catalogueClient{failTeams: true, repos: []repoRow{{"org-1", repoAlpha, "acme/alpha"}}}
	opps := []model.ImproveOpportunity{{EntityType: "repo", EntityID: repoAlpha}, {EntityType: "team", EntityID: "team-a"}}
	nameFlowOpportunities(context.Background(), c, "org-1", opps)
	if nameOrNil(opps[0].EntityDisplayName) != "acme/alpha" || opps[1].EntityDisplayName != nil {
		t.Fatalf("names = %s / %s", nameOrNil(opps[0].EntityDisplayName), nameOrNil(opps[1].EntityDisplayName))
	}
}

func TestAttributionOverview_WhitespaceTitleIsNil(t *testing.T) {
	res, _ := overviewWith(t, []titleRow{{"org-1", repoAlpha, 7, " \t"}}, false)
	if a := attributionRow(t, res, repoAlpha); a.SubjectTitle != nil {
		t.Fatalf("subjectTitle = %q, want nil", *a.SubjectTitle)
	}
}

func TestCatalogue_WhitespaceNamesAreNil(t *testing.T) {
	c := repoCatalogue{repoNames: map[string]string{"r": " \t"}, teamNames: map[string]string{"t": "  "}}
	team := "t"
	if n := c.repoName("r"); n != nil {
		t.Errorf("repoName = %q, want nil", *n)
	}
	if n := c.teamName(&team); n != nil {
		t.Errorf("teamName = %q, want nil", *n)
	}
}
