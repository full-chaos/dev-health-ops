//go:build integration

package providersync

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// seedImportedTeam stores a teams row for id, as an earlier teams run did.
func seedImportedTeam(f carryFixture, provider, id string) {
	f.t.Helper()
	f.team(provider, id, carryPtr(id), nil, 1, carryOld, nil, nil)
}

func sortedTeamKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func openProjectOwnershipTeams(f carryFixture, provider string) []string {
	f.t.Helper()
	teams := map[string]bool{}
	for _, row := range openGitLabOwnership(f.ctx, f.t, f.conn, f.orgID, provider) {
		teams[row.TeamID] = true
	}
	return sortedTeamKeys(teams)
}

func openMembershipTeamIDs(f carryFixture, provider string) []string {
	f.t.Helper()
	teams := map[string]bool{}
	for fact := range openMembershipFacts(f.ctx, f.t, f.conn, f.orgID, provider) {
		team, _, _ := strings.Cut(fact, "|")
		teams[team] = true
	}
	return sortedTeamKeys(teams)
}

func equalTeamLists(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// CHAOS-9123 (chris 2026-10-14: the three team catalog switches stay
// independent; a link row of a team that was not imported is not written).
// A run with members (or projects) on and teams off writes the links of the
// teams that have a teams row only. Each provider: one team imported before,
// one never imported.

func TestAGitHubMembersOnlyRunWritesNoMembershipOfAnUnimportedTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	seedImportedTeam(f, "github", "gh:platform")
	githubDroppedRun(ctx, t, conn, f.orgID, newDroppedGitHubOrg(), TeamCatalogSelections{Members: true}, carryAt, staticScopeCensus{})
	if got := openMembershipTeamIDs(f, "github"); !equalTeamLists(got, []string{"gh:platform"}) {
		t.Errorf("teams with open memberships = %v, want only the imported gh:platform", got)
	}
}

func TestAGitLabRunWithTeamsOffWritesNoLinkOfAnUnimportedTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	fake := newGitLabSnapshotServer(t)
	fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {"org/team-a/api"}})
	seedImportedTeam(f, "gitlab", "gl:org")
	if _, err := gitlabSnapshotRun(ctx, t, conn, f.orgID, fake, TeamCatalogSelections{Projects: true}, false, carryAt); err != nil {
		t.Fatal(err)
	}
	if got := openProjectOwnershipTeams(f, "gitlab"); !equalTeamLists(got, []string{"gl:org"}) {
		t.Errorf("teams with open project ownership = %v, want only the imported gl:org", got)
	}
}

func TestALinearRunWithTeamsOffWritesNoLinkOfAnUnimportedTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	teams := `{"data":{"teams":{"nodes":[` +
		`{"id":"team-raw-eng","key":"ENG","name":"Eng","members":{"nodes":[{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}],"pageInfo":{"hasNextPage":false,"endCursor":null}}},` +
		`{"id":"team-raw-ops","key":"OPS","name":"Ops","members":{"nodes":[{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],` +
		`"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	rest := []string{
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}
	seedImportedTeam(f, "linear", "linear:ENG")
	collector := LinearTeamCatalogCollector{
		ScopeCensus: staticScopeCensus{},
		Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
		Sink:        LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: carrySeamLease()},
	}
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = f.orgID
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = true
	ref.IntegrationID = "integration-a"
	if _, err := collector.CollectTeamCatalog(ctx, ref, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: append([]string{teams}, rest...)})),
		TeamCatalogSelections{Members: true, Projects: true}, carryAt); err != nil {
		t.Fatal(err)
	}
	if got := openMembershipTeamIDs(f, "linear"); !equalTeamLists(got, []string{"linear:ENG"}) {
		t.Errorf("teams with open memberships = %v, want only the imported linear:ENG", got)
	}
}

// A team the same run imports (teams on) is an imported team for its links.
func TestAGitHubRunWithTeamsOnWritesTheLinksOfTheTeamsItImports(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	githubDroppedRun(ctx, t, conn, f.orgID, newDroppedGitHubOrg(), TeamCatalogSelections{Teams: true, Members: true}, carryAt.Add(time.Minute), staticScopeCensus{})
	if got := openMembershipTeamIDs(f, "github"); !equalTeamLists(got, []string{"gh:ops", "gh:platform"}) {
		t.Errorf("teams with open memberships = %v, want both teams of the run", got)
	}
}
