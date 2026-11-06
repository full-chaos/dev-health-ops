package authorization

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// platformAdminActions is the admin-guard set: the organization actions of the go-api policy.Admin and
// policy.AdminOrg profile rows. The catalog flag must name exactly these.
var platformAdminActions = []string{
	"billing.manage", "integrations.read", "integrations.write", "members.invite", "members.manage",
	"members.read", "settings.read", "settings.write", "teams.read", "teams.write",
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
		req := orgRequest(a)
		wantOwnerShapeAllow(t, a+" owner", e.Evaluate(req, platformAdminSnapshot("", "owner", StatusActive)), ReasonAllowedByRole)
		wantOwnerShapeAllow(t, a+" platform_admin + member",
			e.Evaluate(req, platformAdminSnapshot("platform_admin", "member", StatusActive)), ReasonAllowedByPlatformAdmin)
		wantOwnerShapeAllow(t, a+" platform_admin + no membership row",
			e.Evaluate(req, platformAdminSnapshot("platform_admin", "", StatusActive)), ReasonAllowedByPlatformAdmin)
		wantOwnerShapeAllow(t, a+" injected admin.orgs-only role + member",
			injected.Evaluate(req, platformAdminSnapshot("orgs_only", "member", StatusActive)), ReasonAllowedByPlatformAdmin)
		for _, org := range []Status{StatusUnknown, StatusSuspended, StatusRemoved} {
			for _, member := range []string{"", "member", "owner"} {
				if d := e.Evaluate(req, platformAdminSnapshot("platform_admin", member, org)); d.Allowed || d.Reason != ReasonOrganizationInactive {
					t.Errorf("%s platform_admin + %q in org %v: %+v, want deny organization_inactive", a, member, org, d)
				}
			}
		}
		// members.read, settings.read and teams.read are member actions: a member is allowed them by role, with or
		// without platform_owner; every other flagged action is refused to a member without admin.orgs.
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

// The inactive-organization narrowing is kept for an admin.orgs holder (acp/pm condition 1), both ways: an active
// organization with no membership row allows, the holder's own inactive organization denies, and the platform-scope
// actions (the admin organization list, reactivate, status and delete rows) stay allowed with the organization inactive.
func TestPlatformAdminInactiveOrganizationNarrowingIsKept(t *testing.T) {
	e := NewEvaluator()
	for _, a := range flaggedActions(t) {
		wantOwnerShapeAllow(t, a+" active org, no membership row",
			e.Evaluate(orgRequest(a), platformAdminSnapshot("platform_admin", "", StatusActive)), ReasonAllowedByPlatformAdmin)
		if d := e.Evaluate(orgRequest(a), platformAdminSnapshot("platform_admin", "owner", StatusSuspended)); d.Allowed || d.Reason != ReasonOrganizationInactive {
			t.Errorf("%s own inactive org: %+v, want deny organization_inactive", a, d)
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
