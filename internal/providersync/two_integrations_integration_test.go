//go:build integration

package providersync

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// Ownership rows carry no integration key, so a run proves its own scope
// only. The tests below run the real collectors against a real store for an
// organization with two integrations of one provider (A and B, each with its
// own complete, non-empty answer): the run of B must not close the open rows
// of A while A is active, and says so; when A is not active, B is the only
// owner and closes what its answer no longer holds; a single integration
// still closes what it lost.

const (
	twoIntegrationsA = "11111111-1111-4111-8111-111111111111"
	twoIntegrationsB = "22222222-2222-4222-8222-222222222222"
)

func TestTwoLinearIntegrationsOfOneOrganization(t *testing.T) {
	const end = `,"pageInfo":{"hasNextPage":false,"endCursor":null}`
	teams := func(raw, key string) string {
		return `{"data":{"teams":{"nodes":[{"id":"` + raw + `","key":"` + key + `","name":"T","members":{"nodes":[]` + end + `}}]` + end + `}}}`
	}
	const cycles = `{"data":{"cycles":{"nodes":[]` + end + `}}}`
	project := func(id, teamKey string) string {
		return `{"data":{"projects":{"nodes":[` + linearProjectNodeJSON(id, teamKey) + `]` + end + `}}}`
	}
	for _, c := range []struct {
		name string
		// census is the organization's Linear integrations when B runs.
		census         activeIntegrationCensus
		second         string
		secondAnswer   []string
		wantOpen       func(org string) []string
		wantIncomplete bool
		wantRetracted  int
	}{
		{name: "two active integrations: the run of B closes no row of A",
			census: activeIntegrationCensus{twoIntegrationsA: true, twoIntegrationsB: true}, second: twoIntegrationsB,
			secondAnswer: []string{teams("raw-b", "OPS"), cycles, project("pb", "OPS")},
			wantOpen: func(org string) []string {
				return []string{"linear:OPS|" + org + ":linear:OPS", "linear:OPS|pb", "linear:QA|" + org + ":linear:QA", "linear:QA|pa"}
			}, wantIncomplete: true},
		{name: "A is not active any more: B is the only owner and closes what its answer does not hold",
			census: activeIntegrationCensus{twoIntegrationsA: false, twoIntegrationsB: true}, second: twoIntegrationsB,
			secondAnswer: []string{teams("raw-b", "OPS"), cycles, project("pb", "OPS")},
			wantOpen: func(org string) []string {
				return []string{"linear:OPS|" + org + ":linear:OPS", "linear:OPS|pb"}
			}, wantRetracted: 2},
		{name: "control, one integration: its next run closes the project it lost",
			census: activeIntegrationCensus{twoIntegrationsA: true}, second: twoIntegrationsA,
			secondAnswer: []string{teams("raw-a", "QA"), cycles, project("pa2", "QA")},
			wantOpen: func(org string) []string {
				return []string{"linear:QA|" + org + ":linear:QA", "linear:QA|pa2"}
			}, wantRetracted: 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			org := "two-linear-" + uuid.NewString()
			ctx, conn := newWorkItemEffectsConn(t)
			lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
			sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
			run := func(integrationID string, census OwnershipScopeCensus, at time.Time, responses []string) TeamCatalogResult {
				t.Helper()
				claim := nativeTestClaim("linear", "work-items")
				claim.OrgID = org
				ref := teamCatalogRefFromClaim(claim)
				ref.IntegrationID = integrationID
				ref.Strict = false
				result, err := linearCollectorForScopeTest(sink, census, func() time.Time { return at }).CollectTeamCatalog(ctx, ref,
					providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
					linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: responses})),
					TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at)
				if err != nil {
					t.Fatalf("collect: %v", err)
				}
				return result
			}
			open := func() []string {
				t.Helper()
				out := []string{}
				for _, row := range openGitLabOwnership(ctx, t, conn, org, "linear") {
					out = append(out, row.TeamID+"|"+row.Project)
				}
				sort.Strings(out)
				return out
			}
			first := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			run(twoIntegrationsA, activeIntegrationCensus{twoIntegrationsA: true}, first, []string{teams("raw-a", "QA"), cycles, project("pa", "QA")})
			setup := []string{"linear:QA|" + org + ":linear:QA", "linear:QA|pa"}
			sort.Strings(setup)
			if got := open(); !reflect.DeepEqual(got, setup) {
				t.Fatalf("setup: after the run of A open = %v, want %v", got, setup)
			}
			counted, linearCounted := snapshotAbandonedCounts(t), linearIncompleteCounts(t)
			result := run(c.second, c.census, first.Add(time.Hour), c.secondAnswer)
			wantCounted, wantLinear := map[string]int64{}, map[string]int64{}
			if c.wantIncomplete {
				wantCounted["linear/linear_project_ownership/"+OwnershipCloseSkippedScopeShared] = 1
				wantCounted["linear/linear_team_key_ownership/"+OwnershipCloseSkippedScopeShared] = 1
				// The Linear counter names the reason once for the run.
				wantLinear[OwnershipCloseSkippedScopeShared] = 1
			}
			linearMoved := map[string]int64{}
			for reason, n := range linearIncompleteCounts(t) {
				if d := n - linearCounted[reason]; d != 0 {
					linearMoved[reason] = d
				}
			}
			if !reflect.DeepEqual(linearMoved, wantLinear) {
				t.Errorf("%s moved %v, want %v", linearOwnershipSnapshotIncompleteName, linearMoved, wantLinear)
			}
			if got := snapshotAbandonedMoved(t, counted); !reflect.DeepEqual(got, wantCounted) {
				t.Errorf("%s moved %v, want %v", snapshotCloseAbandonedName, got, wantCounted)
			}
			want := c.wantOpen(org)
			sort.Strings(want)
			if got := open(); !reflect.DeepEqual(got, want) {
				t.Errorf("open rows after the second run = %v, want %v", got, want)
			}
			if result.OwnershipSnapshotIncomplete != c.wantIncomplete || result.OwnershipRetracted != c.wantRetracted {
				t.Errorf("incomplete=%v retracted=%d, want %v and %d", result.OwnershipSnapshotIncomplete, result.OwnershipRetracted,
					c.wantIncomplete, c.wantRetracted)
			}
		})
	}
}

func TestTwoJiraIntegrationsOfOneOrganization(t *testing.T) {
	// Site A holds the project OPS (native id 10001), site B the project
	// WEB (native id 20001). The legacy links table of the organization
	// holds a link for each key.
	siteA := `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}],"isLast":true,"total":1}`
	siteB := `{"values":[{"id":"20001","key":"WEB","name":"Web Project"}],"isLast":true,"total":1}`
	siteANext := `{"values":[{"id":"10002","key":"OPS2","name":"Ops Two"}],"isLast":true,"total":1}`
	for _, c := range []struct {
		name           string
		census         activeIntegrationCensus
		second         string
		secondAnswer   string
		secondKey      string
		wantOpen       []string
		wantIncomplete bool
		wantRetracted  int
	}{
		{name: "two active integrations: the run of B closes no row of A",
			census: activeIntegrationCensus{twoIntegrationsA: true, twoIntegrationsB: true}, second: twoIntegrationsB,
			secondAnswer: siteB, secondKey: "WEB",
			wantOpen: []string{"jira:ops-team-a|10001", "jira:ops-team-b|20001"}, wantIncomplete: true},
		{name: "A is not active any more: B is the only owner and closes what its answer does not hold",
			census: activeIntegrationCensus{twoIntegrationsA: false, twoIntegrationsB: true}, second: twoIntegrationsB,
			secondAnswer: siteB, secondKey: "WEB",
			wantOpen: []string{"jira:ops-team-b|20001"}, wantRetracted: 1},
		{name: "control, one integration: its next run closes the project it lost",
			census: activeIntegrationCensus{twoIntegrationsA: true}, second: twoIntegrationsA,
			secondAnswer: siteANext, secondKey: "OPS2",
			wantOpen: []string{"jira:ops-team-c|10002"}, wantRetracted: 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
			orgID := uuid.NewString()
			seeded := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
			for key, team := range map[string]string{"OPS": "ops-team-a", "WEB": "ops-team-b", "OPS2": "ops-team-c"} {
				if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
					orgID, key, team, key, team, seeded); err != nil {
					t.Fatal(err)
				}
			}
			run := func(integrationID string, census OwnershipScopeCensus, at time.Time, live, key string) TeamCatalogResult {
				t.Helper()
				result, err := jiraCollectorForScopeTest(JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}, census).CollectTeamCatalog(ctx,
					TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: integrationID},
					providerfoundation.Credential{Provider: "jira"},
					jiraTeamCatalogTestClient(t, fakehttp.Client(&jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
						"/rest/api/3/project/" + key:            {body: `{"projectTypeKey":"business"}`},
						jiraTeamCatalogProjectSearchURI:         {body: live},
						jiraTeamCatalogArchivedProjectSearchURI: {body: `{"values":[],"isLast":true}`},
					}})),
					TeamCatalogSelections{Projects: true}, at)
				if err != nil {
					t.Fatalf("team catalog sync: %v", err)
				}
				return result
			}
			open := func() []string {
				t.Helper()
				out := []string{}
				for _, row := range openGitLabOwnership(ctx, t, conn, orgID, "jira") {
					out = append(out, row.TeamID+"|"+row.Project)
				}
				sort.Strings(out)
				return out
			}
			first := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			run(twoIntegrationsA, activeIntegrationCensus{twoIntegrationsA: true}, first, siteA, "OPS")
			if got, want := open(), []string{"jira:ops-team-a|10001"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("setup: after the run of A open = %v, want %v", got, want)
			}
			counted := snapshotAbandonedCounts(t)
			result := run(c.second, c.census, first.Add(time.Hour), c.secondAnswer, c.secondKey)
			wantCounted := map[string]int64{}
			if c.wantIncomplete {
				wantCounted["jira/jira_legacy_ownership/"+OwnershipCloseSkippedScopeShared] = 1
			}
			if got := snapshotAbandonedMoved(t, counted); !reflect.DeepEqual(got, wantCounted) {
				t.Errorf("%s moved %v, want %v", snapshotCloseAbandonedName, got, wantCounted)
			}
			if got := open(); !reflect.DeepEqual(got, c.wantOpen) {
				t.Errorf("open rows after the second run = %v, want %v", got, c.wantOpen)
			}
			if result.OwnershipSnapshotIncomplete != c.wantIncomplete || result.OwnershipRetracted != c.wantRetracted {
				t.Errorf("incomplete=%v retracted=%d, want %v and %d", result.OwnershipSnapshotIncomplete, result.OwnershipRetracted,
					c.wantIncomplete, c.wantRetracted)
			}
		})
	}
}
