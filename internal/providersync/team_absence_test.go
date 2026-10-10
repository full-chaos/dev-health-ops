package providersync

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// A team that is no longer listed (CHAOS-9102): the gate decides which dropped
// teams are gone, and the provers of GitHub and GitLab give the provider's own
// answer for one team.

// teamAnswers is a TeamAbsenceProver that answers from a table and counts the
// teams it was asked for.
type teamAnswers struct {
	answers map[string]SnapshotAbsence
	asked   []string
}

func (prover *teamAnswers) TeamAbsence(_ context.Context, teamID string) SnapshotAbsence {
	prover.asked = append(prover.asked, teamID)
	return prover.answers[teamID]
}

func droppedRequest(listing TeamListingEvidence, lookups *AbsenceLookups[TeamSnapshotRow], dropped ...string) ownershipCloseRequest {
	return ownershipCloseRequest{
		ref: TeamCatalogReference{OrgID: "org", IntegrationID: "integration-a"}, provider: "github",
		listed: []string{"gh:platform"}, dropped: dropped, teamListing: listing, teamLookups: lookups,
	}
}

func wholeListing(responses int) TeamListingEvidence {
	return TeamListingEvidence{Listed: []string{"gh:platform"}, ProvesEnd: true, Responses: responses}
}

func TestDroppedTeamsAreDecidedByTheListingTheScopeAndTheProviderAnswer(t *testing.T) {
	proven, held, failed := SnapshotAbsenceProven, SnapshotFactStillHeld, SnapshotAbsenceNotProven
	cases := []struct {
		name         string
		listing      TeamListingEvidence
		census       OwnershipScopeCensus
		answers      map[string]SnapshotAbsence
		wantGone     []string
		wantAsked    []string
		wantReasons  []string // reasons of the legs, any dataset
		wantClosable []string
	}{
		{
			name: "a listing of one response proves the absence: no lookup", listing: wholeListing(1), census: staticScopeCensus{},
			wantGone: []string{"gh:ops", "gh:old"}, wantClosable: []string{"gh:platform", "gh:ops", "gh:old"},
		},
		{
			name:    "a cursor walk's end is its written statement: no lookup",
			listing: TeamListingEvidence{Listed: []string{"gh:platform"}, ProvesEnd: true, Cursor: true}, census: staticScopeCensus{},
			wantGone: []string{"gh:ops", "gh:old"}, wantClosable: []string{"gh:platform", "gh:ops", "gh:old"},
		},
		{
			name: "a listing of two responses needs the provider's answer for each team", listing: wholeListing(2), census: staticScopeCensus{},
			answers:  map[string]SnapshotAbsence{"gh:ops": proven, "gh:old": held},
			wantGone: []string{"gh:ops"}, wantAsked: []string{"gh:ops", "gh:old"}, wantClosable: []string{"gh:platform", "gh:ops"},
			wantReasons: []string{TeamAbsenceStillThere},
		},
		{
			name: "an answer that is not a clear no closes nothing", listing: wholeListing(3), census: staticScopeCensus{},
			answers:  map[string]SnapshotAbsence{"gh:ops": failed, "gh:old": SnapshotAbsenceOverBudget}, // the provider's own "over budget" is not an answer
			wantGone: nil, wantAsked: []string{"gh:ops", "gh:old"}, wantClosable: []string{"gh:platform"},
			wantReasons: []string{TeamAbsenceNotProven},
		},
		{
			name:    "a listing with no confirmed end closes no dropped team and asks nothing",
			listing: TeamListingEvidence{Listed: []string{"gh:platform"}, ProvesEnd: false, Responses: 1}, census: staticScopeCensus{},
			wantClosable: []string{"gh:platform"}, wantReasons: []string{OwnershipCloseSkippedTeamListingIncomplete},
		},
		{
			name:    "a listing that returned no team closes no dropped team and asks nothing",
			listing: TeamListingEvidence{ProvesEnd: true, Responses: 1}, census: staticScopeCensus{},
			wantClosable: []string{"gh:platform"}, wantReasons: []string{OwnershipCloseSkippedNoTeamListed},
		},
		{
			name: "another active integration of the provider keeps every dropped team open and asks nothing", listing: wholeListing(2),
			census: staticScopeCensus{siblings: 1}, wantReasons: []string{OwnershipCloseSkippedScopeShared},
		},
		{
			name: "a census that cannot be read keeps every dropped team open and asks nothing", listing: wholeListing(2),
			census: staticScopeCensus{err: context.DeadlineExceeded}, wantReasons: []string{OwnershipCloseSkippedCensusFailed},
		},
		{
			name: "no census keeps every dropped team open and asks nothing", listing: wholeListing(1), census: nil,
			wantReasons: []string{OwnershipCloseSkippedCensusUnavailable},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prover := &teamAnswers{answers: c.answers}
			request := droppedRequest(c.listing, NewTeamAbsenceLookups(context.Background(), prover), "gh:ops", "gh:old")
			decision := decideOwnershipClose(context.Background(), c.census, request)
			var gone []string
			for team := range decision.gone {
				gone = append(gone, team)
			}
			sort.Strings(gone)
			wantGone := append([]string(nil), c.wantGone...)
			sort.Strings(wantGone)
			if !(len(gone) == 0 && len(wantGone) == 0) && !reflect.DeepEqual(gone, wantGone) {
				t.Errorf("gone = %v, want %v", gone, wantGone)
			}
			if !reflect.DeepEqual(prover.asked, c.wantAsked) && !(len(prover.asked) == 0 && len(c.wantAsked) == 0) {
				t.Errorf("the provider was asked for %v, want %v", prover.asked, c.wantAsked)
			}
			gotClosable := append([]string(nil), decision.closable...)
			sort.Strings(gotClosable)
			wantClosable := append([]string(nil), c.wantClosable...)
			sort.Strings(wantClosable)
			if !(len(gotClosable) == 0 && len(wantClosable) == 0) && !reflect.DeepEqual(gotClosable, wantClosable) {
				t.Errorf("closable = %v, want %v", gotClosable, wantClosable)
			}
			var reasons []string
			for _, leg := range decision.legs {
				reasons = append(reasons, leg.Reason)
			}
			for _, want := range c.wantReasons {
				found := false
				for _, got := range reasons {
					found = found || got == want
				}
				if !found {
					t.Errorf("legs %v hold no reason %q", reasons, want)
				}
			}
			if len(c.wantReasons) == 0 && len(reasons) != 0 {
				t.Errorf("legs = %v, want none", reasons)
			}
			for _, team := range decision.read {
				if decision.gone[team] {
					continue
				}
				if team != "gh:platform" {
					t.Errorf("read holds %q: a dropped team that is not gone must not be read for a close", team)
				}
			}
		})
	}
}

func TestDroppedTeamLookupsAreBoundedByOneBudgetForTheRun(t *testing.T) {
	prover := &teamAnswers{answers: map[string]SnapshotAbsence{}}
	var dropped []string
	for index := 0; index < TeamAbsenceLookupBudget+7; index++ {
		team := "gh:t" + strings.Repeat("x", 1) + string(rune('a'+index%26)) + string(rune('a'+index/26))
		prover.answers[team] = SnapshotAbsenceProven
		dropped = append(dropped, team)
	}
	lookups := NewTeamAbsenceLookups(context.Background(), prover)
	decision := decideOwnershipClose(context.Background(), staticScopeCensus{},
		droppedRequest(wholeListing(2), lookups, dropped...))
	if len(prover.asked) != TeamAbsenceLookupBudget || len(decision.gone) != TeamAbsenceLookupBudget {
		t.Fatalf("asked %d, gone %d, want %d of %d dropped teams: the rest is over the budget", len(prover.asked), len(decision.gone),
			TeamAbsenceLookupBudget, len(dropped))
	}
	overBudget := false
	for _, leg := range decision.legs {
		overBudget = overBudget || (leg.Reason == TeamAbsenceOverBudget && leg.Detail == "7 dropped teams kept open")
	}
	if !overBudget {
		t.Errorf("legs %+v: want one %s leg for the 7 teams over the budget", decision.legs, TeamAbsenceOverBudget)
	}
	// A second close of the same run (the membership close) asks no team twice.
	second := decideOwnershipClose(context.Background(), staticScopeCensus{}, droppedRequest(wholeListing(2), lookups, dropped[:5]...))
	if len(prover.asked) != TeamAbsenceLookupBudget || len(second.gone) != 5 {
		t.Errorf("a team asked for in one close was asked again in the next: asked %d, gone %d", len(prover.asked), len(second.gone))
	}
}

func TestAMemberOfATeamProvenGoneIsProvenGoneWithItAndNoOtherMemberIs(t *testing.T) {
	inner := &countingMemberProver{answer: AbsenceStillMember}
	decision := ownershipCloseDecision{gone: map[string]bool{"gh:ops": true}}
	prover := decision.membershipProver(inner)
	if got := prover.Absence(context.Background(), "gh:ops", "gh:hubot"); got != AbsenceProven {
		t.Errorf("a member of a team that is gone = %v, want proven", got)
	}
	if got := prover.Absence(context.Background(), "gh:platform", "gh:mona"); got != AbsenceStillMember {
		t.Errorf("a member of another team = %v, want the inner answer", got)
	}
	if inner.calls != 1 {
		t.Errorf("the inner prover was asked %d times, want 1 (the member of the team that is gone costs no lookup)", inner.calls)
	}
	if none := (ownershipCloseDecision{}).membershipProver(inner); none != MembershipAbsenceProver(inner) {
		t.Error("with no team gone the inner prover stays as it is")
	}
	if got := (teamGoneAbsence{gone: map[string]bool{}}).Absence(context.Background(), "gh:platform", "gh:mona"); got != AbsenceNotAsked {
		t.Errorf("no inner prover = %v, want not asked (the writer's own list rule)", got)
	}
}

type countingMemberProver struct {
	answer MembershipAbsence
	calls  int
}

func (prover *countingMemberProver) Absence(context.Context, string, string) MembershipAbsence {
	prover.calls++
	return prover.answer
}

// The answers of the provider for one team. The GitLab ones are what gitlab.com
// answered on 2026-10-10 to a read of a group that does not exist and of one
// that does (no credential: the group is public); the GitHub 404 is the shape
// GitHub documents for "get a team by name" (the tree holds no recording of a
// GitHub team).
const (
	gitlabGroupNotFoundRecorded = `{"message":"404 Group Not Found"}`
	githubTeamNotFoundDocs      = `{"message":"Not Found","documentation_url":"https://docs.github.com/rest/teams/teams#get-a-team-by-name","status":"404"}`
)

func teamAnswerServer(t *testing.T, provider string, status int, body string) *providerfoundation.HTTPClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, err := providerfoundation.NewHTTPClient(provider, server.URL, http.DefaultClient,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: 1, MaxWait: 1},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestTheGitHubAnswerForOneTeam(t *testing.T) {
	for _, c := range []struct {
		name   string
		status int
		body   string
		want   SnapshotAbsence
	}{
		{"404 of the provider", 404, githubTeamNotFoundDocs, SnapshotAbsenceProven},
		{"404 with a page of a gateway", 404, `<html><body>404 Not Found</body></html>`, SnapshotAbsenceNotProven},
		{"404 with another message", 404, `{"message":"Resource not accessible by integration"}`, SnapshotAbsenceNotProven},
		{"404 with a body that is not an object", 404, `"Not Found"`, SnapshotAbsenceNotProven},
		{"403 with the provider's message", 403, `{"message":"Not Found"}`, SnapshotAbsenceNotProven},
		{"401", 401, `{"message":"Bad credentials"}`, SnapshotAbsenceNotProven},
		{"500", 500, `{"message":"Not Found"}`, SnapshotAbsenceNotProven},
		{"200 for the team", 200, `{"slug":"ops","name":"Ops"}`, SnapshotFactStillHeld},
		{"200 for the team, another case", 200, `{"slug":"OPS"}`, SnapshotFactStillHeld},
		{"200 for another team (a rename)", 200, `{"slug":"operations"}`, SnapshotAbsenceNotProven},
		{"200 with no slug", 200, `{}`, SnapshotAbsenceNotProven},
		{"200 with a body that is not an object", 200, `[]`, SnapshotAbsenceNotProven},
	} {
		t.Run(c.name, func(t *testing.T) {
			prover := githubTeamAbsence{client: teamAnswerServer(t, "github", c.status, c.body), org: "acme"}
			if got := prover.TeamAbsence(context.Background(), "gh:ops"); got != c.want {
				t.Errorf("answer = %v, want %v", got, c.want)
			}
		})
	}
	for _, id := range []string{"", "gh:", "gl:ops", "ops", "linear:ops"} {
		if got := (githubTeamAbsence{client: teamAnswerServer(t, "github", 404, githubTeamNotFoundDocs), org: "acme"}).TeamAbsence(context.Background(), id); got != SnapshotAbsenceNotProven {
			t.Errorf("team id %q = %v, want not proven: it is not a GitHub team id", id, got)
		}
	}
}

func TestTheGitLabAnswerForOneGroup(t *testing.T) {
	const recorded200 = `{"id":9970,"web_url":"https://gitlab.com/groups/gitlab-org","name":"GitLab.org","path":"gitlab-org","full_path":"gitlab-org"}`
	for _, c := range []struct {
		name   string
		team   string
		status int
		body   string
		want   SnapshotAbsence
	}{
		{"404 of the provider, recorded", "gl:org/gone", 404, gitlabGroupNotFoundRecorded, SnapshotAbsenceProven},
		{"404 with a page of a gateway", "gl:org/gone", 404, `<html>404</html>`, SnapshotAbsenceNotProven},
		{"404 of a project, not a group", "gl:org/gone", 404, `{"message":"404 Project Not Found"}`, SnapshotAbsenceNotProven},
		{"403", "gl:org/gone", 403, `{"message":"403 Forbidden"}`, SnapshotAbsenceNotProven},
		{"500", "gl:org/gone", 500, gitlabGroupNotFoundRecorded, SnapshotAbsenceNotProven},
		{"200 for the group, recorded", "gl:gitlab-org", 200, recorded200, SnapshotFactStillHeld},
		{"200 for another group", "gl:org/team-b", 200, recorded200, SnapshotAbsenceNotProven},
		{"200 with no full_path", "gl:org/team-b", 200, `{}`, SnapshotAbsenceNotProven},
		{"not a GitLab team id", "gh:ops", 404, gitlabGroupNotFoundRecorded, SnapshotAbsenceNotProven},
		{"no group path", "gl:", 404, gitlabGroupNotFoundRecorded, SnapshotAbsenceNotProven},
	} {
		t.Run(c.name, func(t *testing.T) {
			prover := gitlabTeamAbsence{client: teamAnswerServer(t, "gitlab", c.status, c.body)}
			if got := prover.TeamAbsence(context.Background(), c.team); got != c.want {
				t.Errorf("answer = %v, want %v", got, c.want)
			}
		})
	}
}

// TestEveryCloseThroughTheGateStatesItsDroppedTeams reads the source: a caller
// of decideOwnershipClose that does not name its dropped teams and the team
// listing they were absent from would go back to closing the listed teams
// only. The callers are the five closes of the three catalogs (the GitHub and
// GitLab ownership and membership closes, and the Linear membership close).
func TestEveryCloseThroughTheGateStatesItsDroppedTeams(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	callers := map[string]int{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if ident, ok := literal.Type.(*ast.Ident); !ok || ident.Name != "ownershipCloseRequest" {
				return true
			}
			keys := map[string]bool{}
			for _, element := range literal.Elts {
				if pair, ok := element.(*ast.KeyValueExpr); ok {
					if key, ok := pair.Key.(*ast.Ident); ok {
						keys[key.Name] = true
					}
				}
			}
			callers[name]++
			if !keys["dropped"] || !keys["teamListing"] {
				t.Errorf("%s: an ownershipCloseRequest that names no dropped teams and no team listing closes the listed teams only (keys: %v)", name, keys)
			}
			return true
		})
	}
	want := map[string]int{
		"github_team_catalog_collector.go": 2, "gitlab_team_catalog_route.go": 2, "linear_team_catalog_collector.go": 1,
	}
	if !reflect.DeepEqual(callers, want) {
		t.Errorf("the closes through the gate are %v, want %v: a new close names its dropped teams and is added here", callers, want)
	}
}

// A dropped team that is proven gone closes its grants even when no listed
// team's own listing proved its end: the proof of the dropped team is the team
// listing, not the listing of another team.
func TestAGoneTeamClosesItsGrantsWhenEveryListedTeamListingIsUnproven(t *testing.T) {
	at := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	from := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	open := []githubTeamRepoOwnershipRow{
		{TeamID: "gh:platform", RepoFullName: "acme/api", Source: githubTeamCatalogSource, ValidFrom: from},
		{TeamID: "gh:ops", RepoFullName: "acme/infra", Source: githubTeamCatalogSource, ValidFrom: from},
	}
	request := ownershipCloseRequest{
		ref: TeamCatalogReference{OrgID: "org", IntegrationID: "integration-a"}, provider: githubTeamCatalogProvider,
		listed: []string{"gh:platform"}, unproven: []string{"gh:platform"}, responses: map[string]int{"gh:platform": 2},
		dropped: []string{"gh:ops"}, teamListing: wholeListing(1), teamLookups: NewTeamAbsenceLookups(context.Background(), nil),
	}
	decision := decideOwnershipClose(context.Background(), staticScopeCensus{}, request)
	if !decision.gone["gh:ops"] {
		t.Fatalf("gone = %v, want the dropped team: the team listing was one response", decision.gone)
	}
	_, plan := githubRepoOwnershipSnapshot(nil, open, at, decision.snapshot(GitHubTeamRepoGrantKind, "the team's repositories", nil))
	if len(plan.Retract) != 1 || open[plan.Retract[0].Open].TeamID != "gh:ops" {
		t.Errorf("retractions = %+v, want the one grant of the gone team (the listed team's unproven listing keeps its grant open)", plan.Retract)
	}
	// With no team gone the same run closes nothing.
	request.teamListing = TeamListingEvidence{Listed: []string{"gh:platform"}, ProvesEnd: true, Responses: 2}
	none := decideOwnershipClose(context.Background(), staticScopeCensus{}, request)
	_, plan = githubRepoOwnershipSnapshot(nil, open, at, none.snapshot(GitHubTeamRepoGrantKind, "the team's repositories", nil))
	if len(plan.Retract) != 0 {
		t.Errorf("retractions = %+v, want none: a listing of two responses and no answer proves nothing", plan.Retract)
	}
}
