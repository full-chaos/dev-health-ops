//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-9144 (round 122): provider_user_id is evidence for rename detection. Every
// route that copies, reads back or stages a membership row must keep it, or a later
// rename becomes a leave and a join without any error.

const withIDColumns = `org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, provider_user_id`

// The team-ID carry moves the open membership rows to the keyed team id: the new row
// keeps the stored provider_user_id.
func TestTeamIDCarryKeepsTheProviderUserIDOfAMembership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.exec(`INSERT INTO team_memberships (`+withIDColumns+`) VALUES (?, 'linear', 'ENG', 'linear:alice@example.com', NULL, 'alice@example.com', [], 'native', 1, 100, 10, ?, NULL, ?, 'user-1')`,
		f.orgID, carryOld, carryOld)
	if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
		t.Fatalf("carry: %v", err)
	}
	if got := f.str(`SELECT ifNull(provider_user_id, '<NULL>') FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'linear:ENG' AND member_id = 'linear:alice@example.com' AND valid_to IS NULL`); got != "user-1" {
		t.Errorf("carried provider_user_id = %q, want user-1", got)
	}
}

// The effect readback is "exact" only when the stored row holds the provider_user_id
// the effect wrote; a stored row without it is a conflict, so the effect is written again.
func TestMembershipEffectReadbackIsNotExactWhenTheStoredRowHasNoProviderUserID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	claim := Claim{Unit: Unit{OrgID: orgID}}
	at := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	id := "user-1"
	seed := func(provider, team, member, source string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO team_memberships (`+withIDColumns+`) VALUES (?, ?, ?, ?, NULL, NULL, [], ?, 1, 100, 10, ?, NULL, ?, NULL)`,
			orgID, provider, team, member, source, at, at); err != nil {
			t.Fatal(err)
		}
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })

	seed("gitlab", "gl:g", "gl:alice", gitlabTeamCatalogSource)
	gl := gitlabTeamCatalogMembershipRow{OrgID: orgID, Provider: "gitlab", TeamID: "gl:g", MemberID: "gl:alice", IdentityFacets: []string{},
		Source: gitlabTeamCatalogSource, IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: at, UpdatedAt: at, ProviderUserID: &id}
	if got, err := (GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}).inspectMembership(ctx, claim, gl); err != nil || got != EffectConflict {
		t.Errorf("gitlab readback = %v (err %v), want EffectConflict", got, err)
	}
	gl.ProviderUserID = nil
	if got, err := (GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}).inspectMembership(ctx, claim, gl); err != nil || got != EffectExact {
		t.Errorf("gitlab readback of a row with no id = %v (err %v), want EffectExact", got, err)
	}

	seed("jira", "jira:t", "jira:alice", jiraTeamCatalogSource)
	jr := jiraTeamCatalogMembershipRow{OrgID: orgID, Provider: "jira", TeamID: "jira:t", MemberID: "jira:alice", IdentityFacets: []string{},
		Source: jiraTeamCatalogSource, IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: at, UpdatedAt: at, ProviderUserID: &id}
	if got, err := (JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}).inspectMembership(ctx, claim, jr); err != nil || got != EffectConflict {
		t.Errorf("jira readback = %v (err %v), want EffectConflict", got, err)
	}

	seed("linear", "linear:ENG", "linear:alice@example.com", "native")
	lr := linearReferenceMembershipRow{OrgID: orgID, Provider: "linear", TeamID: "linear:ENG", MemberID: "linear:alice@example.com", IdentityFacets: []string{},
		Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: at, UpdatedAt: at, ProviderUserID: &id}
	if got, err := (LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}).inspectMemberships(ctx, claim, []linearReferenceMembershipRow{lr}); err != nil || got != EffectConflict {
		t.Errorf("linear readback = %v (err %v), want EffectConflict", got, err)
	}
}

// A membership an admin's manual pin keeps out is staged for review: the staged
// payload holds provider_user_id, so an approval writes it. The change id is still
// the one Python derives (it never hashes the new key).
func TestDriftReviewStagesTheProviderUserIDOfAConflictingMembership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	validFrom := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO team_memberships (`+withIDColumns+`) VALUES (?, 'github', 'gh:other-team', 'gh:octocat', 'octocat', NULL, ['gh:octocat'], 'manual', 1, 0, 0, ?, NULL, ?, NULL)`,
		orgID, validFrom, validFrom); err != nil {
		t.Fatal(err)
	}
	doer := githubTeamCatalogAdapterDoer(t)
	doer.byPath["/orgs/acme/teams/platform/members"] = `[{"login":"octocat","id":583231}]`
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"}, credential,
		githubTeamCatalogAdapterClient(t, doer), TeamCatalogSelections{Teams: true, Members: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	var changeID, field, oldJSON, payload string
	if err := conn.QueryRow(ctx, `SELECT change_id, ifNull(field, ''), old_value_json, new_value_json FROM team_drift_changes FINAL WHERE org_id = ? AND change_type = 'membership_changed'`, orgID).
		Scan(&changeID, &field, &oldJSON, &payload); err != nil {
		t.Fatalf("no staged membership change: %v", err)
	}
	const idKey = `"provider_user_id":"583231",`
	if !strings.Contains(payload, idKey) {
		t.Fatalf("staged payload = %s, want provider_user_id 583231", payload)
	}
	// The id is the one Python derives: the hash is over the payload WITHOUT the new key.
	if want := changeIDForIdentityMembership(orgID, "gh:platform", "github", "gh:octocat", field, oldJSON, strings.Replace(payload, idKey, "", 1)); changeID != want {
		t.Errorf("change id = %s, want %s (the hash of the payload without provider_user_id)", changeID, want)
	}
}

func TestMembershipPayloadJSONAddsOnlyTheProviderUserIDAndNeverChangesTheHashedShape(t *testing.T) {
	id := "42"
	row := teamDriftMembershipView{Provider: "gitlab", TeamID: "gl:g", MemberID: "gl:a", Source: "gitlab_team_catalog", ValidFrom: time.Unix(0, 0).UTC(), UpdatedAt: time.Unix(0, 0).UTC()}
	if _, has := membershipPayloadJSON("o", row)["provider_user_id"]; has {
		t.Error("a row with no id must not add the key")
	}
	if pyCanonicalJSON(membershipPayloadJSON("o", row)) != pyCanonicalJSON(membershipRowJSON("o", row)) {
		t.Error("a row with no id must match the Python payload")
	}
	row.ProviderUserID = &id
	if _, has := membershipPayloadJSON("o", row)["provider_user_id"]; !has {
		t.Error("a row with an id must stage it")
	}
	if _, has := membershipRowJSON("o", row)["provider_user_id"]; has {
		t.Error("the hashed shape must stay the Python shape")
	}
}

// The retirement of a Jira project-as-team row closes its membership rows with an
// INSERT ... SELECT: the closed row keeps provider_user_id.
func TestJiraProjectAsTeamRetirementKeepsTheProviderUserIDOfAMembership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	f.team("jira", "OPS", "OPS", nil)
	f.ownership("jira", "OPS", "10001", "OPS", "native")
	if err := conn.Exec(ctx, `INSERT INTO team_memberships (`+withIDColumns+`) VALUES (?, 'jira', 'OPS', 'jira:lead-1', NULL, NULL, [], 'native', 1, 100, 10, ?, NULL, ?, 'account-1')`, f.orgID, f.old, f.old); err != nil {
		t.Fatal(err)
	}
	if _, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC), false); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := conn.QueryRow(ctx, `SELECT ifNull(provider_user_id, '<NULL>') FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'OPS' AND member_id = 'jira:lead-1' AND valid_to IS NOT NULL`, f.orgID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "account-1" {
		t.Errorf("retired membership provider_user_id = %q, want account-1", got)
	}
}

// The Jira project-as-team catalog writer stores provider_user_id (the accountId of a lead).
func TestJiraTeamCatalogWriterStoresTheProviderUserID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	at := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	id := "account-1"
	row := jiraTeamCatalogMembershipRow{OrgID: orgID, Provider: "jira", TeamID: "OPS", MemberID: "jira:lead-1", IdentityFacets: []string{},
		Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: at, UpdatedAt: at, ProviderUserID: &id}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	if err := (JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}).writeMemberships(ctx, []jiraTeamCatalogMembershipRow{row}); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := conn.QueryRow(ctx, `SELECT ifNull(provider_user_id, '<NULL>') FROM team_memberships FINAL WHERE org_id = ? AND member_id = 'jira:lead-1'`, orgID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "account-1" {
		t.Errorf("stored provider_user_id = %q, want account-1", got)
	}
}
