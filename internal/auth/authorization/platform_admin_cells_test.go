package authorization

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// platformAdminActions is the admin-guard set (the organization actions of the go-api policy.Admin and
// policy.AdminOrg profile rows), the query-api actions whose legacy path admits a superuser in any organization, and
// the go-api billing and license reads whose legacy handlers admit an admin.billing holder before any membership read.
// The catalog flag must name exactly these.
var platformAdminActions = []string{
	"acr.credential.issue", "analytics.read", "billing.manage", "billing.read", "git.read", "integrations.read", "integrations.write",
	"license.read", "members.invite", "members.manage", "metrics.read", "reports.write", "settings.write",
	"teams.write", "work_items.read",
}

func flaggedActions(t *testing.T) []string {
	t.Helper()
	actions, _, _ := vocab(t)
	var out []string
	for _, a := range actions {
		if a.PlatformAdmin == "allowed" {
			out = append(out, a.Action)
		}
	}
	sort.Strings(out)
	if !reflect.DeepEqual(out, platformAdminActions) {
		t.Fatalf("flagged actions %v, want %v", out, platformAdminActions)
	}
	return out
}

func platformAdminSnapshot(platformRole, memberRole string, org Status) fakeSnapshot {
	s := fakeSnapshot{principal: StatusActive, platformRole: platformRole, orgStatus: map[string]Status{testOrg: org},
		memberships: map[string]Membership{}}
	if memberRole != "" {
		s.memberships[testOrg] = Membership{OrgID: testOrg, Role: memberRole, Status: StatusActive}
	}
	return s
}

// filteredFlaggedAction is the one flagged action that is repository_scope filtered (device approval and consent take
// their repository scopes from it); its cells are in credential_issue_scope_test.go.
const filteredFlaggedAction = "acr.credential.issue"

func wantOwnerShapeAllow(t *testing.T, name string, d Decision, why Reason) {
	t.Helper()
	want := &Obligations{TenantScope: testOrg}
	if !d.Allowed || d.Reason != why || !reflect.DeepEqual(d.Obligations, want) || d.Audit != nil {
		t.Errorf("%s: %+v (obligations %+v), want allow %s with tenant scope only", name, d, d.Obligations, why)
	}
}

// catalogWithOrgsOnlyRole is the embedded catalog plus a platform role that holds admin.orgs and nothing else: the
// rule keys on the action, never on the role name.
func catalogWithOrgsOnlyRole(t *testing.T) *policycatalog.Catalog {
	t.Helper()
	var roles map[string]any
	if err := json.Unmarshal(vocabulary.RolesV1, &roles); err != nil {
		t.Fatal(err)
	}
	roles["roles"] = append(roles["roles"].([]any), map[string]any{"role": "orgs_only", "scope": "platform", "actions": []string{"admin.orgs"}})
	raw, err := json.Marshal(roles)
	if err != nil {
		t.Fatal(err)
	}
	c, err := policycatalog.Load(vocabulary.ActionsV1, raw)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPlatformAdminCellsOverTheFlaggedActions(t *testing.T) {
	e := NewEvaluator()
	injected := newEvaluator(catalogWithOrgsOnlyRole(t))
	for _, a := range flaggedActions(t) {
		if a == filteredFlaggedAction {
			continue // its allow also carries the repository scope: TestAcrCredentialIssue* cells
		}
		req := orgRequest(a)
		wantOwnerShapeAllow(t, a+" owner", e.Evaluate(req, platformAdminSnapshot("", "owner", StatusActive)), ReasonAllowedByRole)
		wantOwnerShapeAllow(t, a+" platform_admin + member",
			e.Evaluate(req, platformAdminSnapshot("platform_admin", "member", StatusActive)), ReasonAllowedByPlatformAdmin)
		wantOwnerShapeAllow(t, a+" platform_admin + no membership row",
			e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusActive)), ReasonAllowedByPlatformAdmin)
		wantOwnerShapeAllow(t, a+" injected admin.orgs-only role + member",
			injected.Evaluate(req, platformAdminSnapshot("orgs_only", "member", StatusActive)), ReasonAllowedByPlatformAdmin)
		for _, member := range []string{"", "member", "owner"} {
			wantOwnerShapeAllow(t, a+" platform_admin + "+member+" in a suspended org",
				e.Evaluate(req, platformAdminSnapshot("platform_admin", member, StatusSuspended)), ReasonAllowedByPlatformAdmin)
			wantOwnerShapeAllow(t, a+" platform_admin + "+member+" in an unknown org",
				e.Evaluate(req, platformAdminSnapshot("platform_admin", member, StatusUnknown)), ReasonAllowedByPlatformAdmin)
			for _, org := range []Status{StatusRemoved, StatusInvited} {
				if d := e.Evaluate(req, platformAdminSnapshot("platform_admin", member, org)); d.Allowed || d.Reason != ReasonOrganizationInactive {
					t.Errorf("%s platform_admin + %q in org %v: %+v, want deny organization_inactive", a, member, org, d)
				}
			}
		}
		// A flagged action the member role holds is allowed to a member by role, with or without platform_owner;
		// every other flagged action is refused to a member without admin.orgs.
		memberHolds := policycatalog.Default().Allows("member", a)
		for _, platform := range []string{"platform_owner", ""} {
			d := e.Evaluate(req, platformAdminSnapshot(platform, "member", StatusActive))
			if memberHolds {
				wantOwnerShapeAllow(t, a+" "+platform+" + member (member role)", d, ReasonAllowedByRole)
			} else if d.Allowed || d.Reason != ReasonRoleLacksAction {
				t.Errorf("%s %q + member: %+v, want deny role_lacks_action", a, platform, d)
			}
		}
		if d := e.Evaluate(req, platformAdminSnapshot("platform_owner", "", StatusActive)); d.Allowed || d.Reason != ReasonNoMembership {
			t.Errorf("%s platform_owner + no membership row: %+v, want deny no_membership", a, d)
		}
	}
}

// An organization action outside the flagged set gives a platform admin nothing the membership does not.
func TestPlatformAdminGetsNoUnflaggedOrganizationAction(t *testing.T) {
	flagged := map[string]bool{}
	for _, a := range flaggedActions(t) {
		flagged[a] = true
	}
	actions, _, _ := vocab(t)
	e := NewEvaluator()
	checked := 0
	for _, a := range actions {
		if a.Scope != "organization" || a.ResourceKind != "organization" || flagged[a.Action] {
			continue
		}
		checked++
		req := orgRequest(a.Action)
		if d := e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusActive)); d.Allowed || d.Reason != ReasonNoMembership {
			t.Errorf("%s platform_admin + no membership row: %+v, want deny no_membership", a.Action, d)
		}
		for _, member := range []string{"viewer", "member"} {
			alone := e.Evaluate(req, platformAdminSnapshot("", member, StatusActive))
			with := e.Evaluate(req, platformAdminSnapshot("platform_admin", member, StatusActive))
			if !reflect.DeepEqual(alone, with) {
				t.Errorf("%s %s: with platform_admin %+v, alone %+v", a.Action, member, with, alone)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no unflagged organization action")
	}
	if d := e.Evaluate(orgRequest("org.delete"), platformAdminSnapshot("platform_admin", "member", StatusActive)); d.Allowed {
		t.Errorf("org.delete platform_admin + member: %+v, want deny", d)
	}
}

// acp D3 (x) word, 2026-11-06, CHAOS-13240: a holder's suspended organization allows the flagged actions.
func TestPlatformAdminSuspendedOrganizationAllowsAHolderOnly(t *testing.T) {
	e := NewEvaluator()
	for _, a := range flaggedActions(t) {
		if a == filteredFlaggedAction {
			continue // its allow also carries the repository scope: TestAcrCredentialIssue* cells
		}
		req := orgRequest(a)
		wantOwnerShapeAllow(t, a+" active org, no membership row",
			e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusActive)), ReasonAllowedByPlatformAdmin)
		wantOwnerShapeAllow(t, a+" suspended org, no membership row",
			e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusSuspended)), ReasonAllowedByPlatformAdmin)
		wantOwnerShapeAllow(t, a+" own suspended org",
			e.Evaluate(req, platformAdminSnapshot("platform_admin", "owner", StatusSuspended)), ReasonAllowedByPlatformAdmin)
		for _, platform := range []string{"", "platform_owner"} {
			for _, member := range []string{"viewer", "member", "admin", "owner"} {
				if d := e.Evaluate(req, platformAdminSnapshot(platform, member, StatusSuspended)); d.Allowed || d.Reason != ReasonOrganizationInactive {
					t.Errorf("%s %q + %s in a suspended org: %+v, want deny organization_inactive", a, platform, member, d)
				}
			}
		}
	}
	role, err := policycatalog.Default().Role("platform_admin")
	if err != nil || len(role.Actions) == 0 {
		t.Fatalf("platform_admin: %+v %v", role, err)
	}
	for _, a := range role.Actions {
		for _, org := range []Status{StatusSuspended, StatusUnknown} {
			if d := e.Evaluate(platformRequest(a), platformAdminSnapshot("platform_admin", "owner", org)); !d.Allowed || d.Reason != ReasonAllowedByRole {
				t.Errorf("platform action %s with the org %v: %+v, want allow", a, org, d)
			}
		}
	}
}

// acp word on PLAN-13214-1, 2026-11-06, after the executed legacy measurement (go-api 40/40 and query-api 8/8 admit
// a superuser whose token names an organization with no row): the platform-admin rule comes before the organization
// status check for an unknown organization too. A member without admin.orgs keeps organization_inactive, and a
// removed organization still denies.
func TestPlatformAdminUnknownOrganizationAllowsAHolderOnly(t *testing.T) {
	e := NewEvaluator()
	for _, a := range flaggedActions(t) {
		if a == filteredFlaggedAction {
			continue // its allow also carries the repository scope: TestAcrCredentialIssue* cells
		}
		req := orgRequest(a)
		for _, member := range []string{"", "owner"} {
			wantOwnerShapeAllow(t, a+" platform_admin + "+member+" in an unknown org",
				e.Evaluate(req, platformAdminSnapshot("platform_admin", member, StatusUnknown)), ReasonAllowedByPlatformAdmin)
		}
		if d := e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusRemoved)); d.Allowed || d.Reason != ReasonOrganizationInactive {
			t.Errorf("%s platform_admin, removed org: %+v, want deny organization_inactive", a, d)
		}
		for _, platform := range []string{"", "platform_owner"} {
			for _, member := range []string{"", "viewer", "member", "admin", "owner"} {
				if d := e.Evaluate(req, platformAdminSnapshot(platform, member, StatusUnknown)); d.Allowed || d.Reason != ReasonOrganizationInactive {
					t.Errorf("%s %q + %q in an unknown org: %+v, want deny organization_inactive", a, platform, member, d)
				}
			}
		}
	}
	actions, _, _ := vocab(t)
	flagged := map[string]bool{}
	for _, a := range flaggedActions(t) {
		flagged[a] = true
	}
	for _, a := range actions {
		if a.Scope != "organization" || a.ResourceKind != "organization" || flagged[a.Action] {
			continue
		}
		if d := e.Evaluate(orgRequest(a.Action), platformAdminSnapshot("platform_admin", "owner", StatusUnknown)); d.Allowed || d.Reason != ReasonOrganizationInactive {
			t.Errorf("unflagged %s platform_admin + owner in an unknown org: %+v, want deny organization_inactive", a.Action, d)
		}
	}
}
