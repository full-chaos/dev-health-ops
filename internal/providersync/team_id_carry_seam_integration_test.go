//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func carrySeamLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

// runLinearCatalogBehindTheCarry runs the real Linear collector, behind the
// carry as the registries build it, on one Linear team ENG ("Provider Eng")
// with one member, alice@example.com.
func runLinearCatalogBehindTheCarry(t *testing.T, f carryFixture) TeamCatalogResult {
	t.Helper()
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[` +
			`{"id":"team-raw-eng","key":"ENG","name":"Provider Eng","members":{"nodes":[{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}` +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	collector := CarryFirstTeamCatalogCollector{Conn: f.conn, Writer: "test", Collector: LinearTeamCatalogCollector{
		ScopeCensus: staticScopeCensus{},
		Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
		Sink:        LinearReferenceCatalogClickHouseEffects{Conn: f.conn, Lease: carrySeamLease()},
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = f.orgID
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = true
	result, err := collector.CollectTeamCatalog(f.ctx, ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}, linearWorkItemsClient(t, fakehttp.Client(doer)),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, carryAt)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	return result
}

// The Linear collector reads the sync policy and the manual memberships by
// the prefixed id in its guards, before its first write. The bare team's
// MANUAL policy is honoured (the provider name does not overwrite the
// admin's team) and the bare team's own manual member is not a conflict.
func TestTheLinearCatalogReadsTheBareTeamsPolicyAndManualMembersAfterTheCarry(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.exec(`INSERT INTO team_sync_policies (org_id, team_id, sync_policy, managed_fields, updated_by, updated_at) VALUES (?, 'ENG', 2, [], 'admin', ?)`, f.orgID, carryOld)
	f.exec(`INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'linear', 'ENG', 'linear:alice@example.com', [], 'manual', 1, 100, 10, ?, NULL, ?)`, f.orgID, carryOld, carryOld)
	f.observation("linear", "ENG", "ENG")

	result := runLinearCatalogBehindTheCarry(t, f)

	if result.TeamsSkippedPolicy != 1 || result.TeamsWritten != 0 {
		t.Errorf("teams skipped by policy = %d, written = %d; want the MANUAL policy honoured (1, 0)", result.TeamsSkippedPolicy, result.TeamsWritten)
	}
	if got := f.str(`SELECT ifNull(name, '') FROM teams FINAL WHERE org_id = ? AND id = 'linear:ENG'`); got != "team ENG" {
		t.Errorf("linear:ENG name = %q, want the admin-owned name kept", got)
	}
	if result.MembershipsSkippedManualConflict != 0 || result.MembershipsStagedForReview != 0 {
		t.Errorf("membership conflicts skipped = %d, staged = %d; the team's own manual member is no conflict",
			result.MembershipsSkippedManualConflict, result.MembershipsStagedForReview)
	}
	if got := f.count(`SELECT count() FROM team_drift_changes FINAL WHERE org_id = ? AND entity_type = 'identity' AND status = 'pending'`); got != 0 {
		t.Errorf("pending identity changes = %d, want 0", got)
	}
	if got := f.str(`SELECT arrayStringConcat(arraySort(groupArray(concat(team_id, '/', toString(source)))), ',') FROM team_memberships FINAL WHERE org_id = ? AND member_id = 'linear:alice@example.com' AND valid_to IS NULL`); got != "linear:ENG/manual,linear:ENG/native" {
		t.Errorf("open memberships of alice = %q, want the manual and the native one, both on linear:ENG", got)
	}
	if got := f.str(carryActiveBareTeams); got != "" {
		t.Errorf("active bare ids = %q", got)
	}
}

// An admin team that a Linear observation names becomes the Linear team:
// the policy guard does not rewrite the observation before the carry, so
// there is one active team, with the admin's manual member, and nothing is
// left to carry.
func TestTheLinearCatalogKeepsOneTeamForAnAdminTeamItNames(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "ENG", nil, nil, 1, carryOld, []string{"admin@example.com"}, nil)
	f.observation("linear", "ENG", "ENG")

	runLinearCatalogBehindTheCarry(t, f)

	active := f.str(`SELECT arrayStringConcat(arraySort(groupArray(concat(id, '/', provider, '/', arrayStringConcat(manual_members, ';')))), ',') FROM teams FINAL WHERE org_id = ? AND is_active = 1`)
	if active != "linear:ENG/linear/admin@example.com" {
		t.Errorf("active teams = %q, want one Linear team with the admin's manual member", active)
	}
	later, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), true)
	if err != nil || later.Found() || later.AdminTeamsNotCarried != 0 {
		t.Errorf("later dry run = %+v, %v; want nothing left", later, err)
	}
}

// The Jira project-as-team leg closes and opens jira_legacy links by the
// prefixed id: run behind the carry, the open link of an Atlassian team
// keeps its first valid_from.
func TestTheJiraProjectAsTeamCatalogKeepsTheFirstSeenOfALink(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("jira", carryAtlassianID, carryPtr(carryAtlassianARI), nil, 1, carryOld, nil, nil)
	f.exec(`INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', ?, '10001', 'OPS', 'jira_legacy', 1, 90, 20, ?, ?)`,
		f.orgID, carryAtlassianID, carryFirst, carryFirst)
	f.exec(`INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, 'OPS', ?, 'OPS', 'ops', ?)`,
		f.orgID, carryAtlassianID, carryOld)
	byURI := map[string]jiraTeamCatalogFixtureResponse{
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"business"}`},
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops"}],"isLast":true,"total":1}`},
	}
	collector := CarryFirstTeamCatalogCollector{Conn: conn, Writer: "test", Collector: JiraTeamCatalogCollector{
		ScopeCensus: staticScopeCensus{},
		Handler:     JiraTeamCatalogRouteHandler{},
		Sink:        JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: carrySeamLease()},
	}}
	if _, err := collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: f.orgID, SyncRunID: "run"},
		providerfoundation.Credential{Provider: "jira"},
		jiraTeamCatalogTestClient(t, fakehttp.Client(&jiraTeamCatalogFixtureDoer{t: t, byURI: byURI})),
		TeamCatalogSelections{Projects: true}, carryAt); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := f.str(`SELECT toString(groupArray(valid_from)) FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = ? AND project_id = '10001' AND valid_to IS NULL`, "jira:"+carryAtlassianID)
	if got != "['2026-06-01 00:00:00.000']" {
		t.Errorf("open jira:<uuid>/10001 valid_from = %s, want the first-seen 2026-06-01", got)
	}
}
