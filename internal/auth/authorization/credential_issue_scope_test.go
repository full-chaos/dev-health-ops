package authorization

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// acr.credential.issue is repository_scope filtered (catalog 18): device approval and consent take their
// repository scopes from this decision. One rule gives every caller its shape: the membership default and a live
// wildcard grant are org-wide (repository_scope organization), exact grants are the named allowlist, and the
// platform admin with no membership row is org-wide. Every organization role gets the org scope by default (rule 2).
func TestAcrCredentialIssueAllowCarriesTheScopeOfItsCaller(t *testing.T) {
	e := NewEvaluator()
	req := orgRequest("acr.credential.issue")
	orgWide := &Obligations{TenantScope: testOrg, RepositoryScope: RepositoryScopeOrganization}
	named := &Obligations{TenantScope: testOrg, RepositoryRestricted: true, RepositoryAllowlist: []string{"acme/api", "acme/web"}}
	cells := []struct {
		name string
		snap Snapshot
		want *Obligations
	}{
		{"viewer, membership default", repoSnapshot("viewer"), orgWide},
		{"member, membership default", repoSnapshot("member"), orgWide},
		{"admin, membership default", repoSnapshot("admin"), orgWide},
		{"owner, membership default", repoSnapshot("owner"), orgWide},
		{"member, live wildcard grant", repoSnapshot("member", liveGrant("*")), orgWide},
		{"member, exact grants", repoSnapshot("member", liveGrant("acme/web"), liveGrant("acme/api")), named},
		{"platform admin, no membership row", grantFake{fakeSnapshot: platformAdminSnapshot("platform_admin", "", StatusActive), now: nowNano}, orgWide},
	}
	for _, c := range cells {
		got := e.Evaluate(req, c.snap)
		if !got.Allowed {
			t.Errorf("%s: %+v, want allow", c.name, got)
			continue
		}
		o := *got.Obligations
		o.Redaction = nil
		if !reflect.DeepEqual(&o, c.want) {
			t.Errorf("%s: obligations %+v, want %+v", c.name, got.Obligations, c.want)
		}
		if got.CacheClass != "" || got.ExpiresAtUnixNano != 0 {
			t.Errorf("%s: cache %q expires %d, want none of its own (catalog class none: never cached)", c.name, got.CacheClass, got.ExpiresAtUnixNano)
		}
	}
}

// The platform-admin rule on acr.credential.issue is the organization branch's, with the org-wide scope: allowed in
// an active, suspended or unknown organization (also to a role that holds only admin.orgs), denied in a removed or
// invited one; a platform role without admin.orgs and no membership row is denied no_membership.
func TestAcrCredentialIssuePlatformAdminAllowIsOrgWideByTheOrganizationBranchRule(t *testing.T) {
	e := NewEvaluator()
	injected := newEvaluator(catalogWithOrgsOnlyRole(t))
	req := orgRequest("acr.credential.issue")
	want := &Obligations{TenantScope: testOrg, RepositoryScope: RepositoryScopeOrganization}
	snap := func(role string, org Status) grantFake {
		return grantFake{fakeSnapshot: platformAdminSnapshot(role, "", org), now: nowNano}
	}
	for _, org := range []Status{StatusActive, StatusSuspended, StatusUnknown} {
		for name, d := range map[string]Decision{
			"platform_admin":       e.Evaluate(req, snap("platform_admin", org)),
			"admin.orgs-only role": injected.Evaluate(req, snap("orgs_only", org)),
		} {
			if !d.Allowed || d.Reason != ReasonAllowedByPlatformAdmin || !reflect.DeepEqual(d.Obligations, want) {
				t.Errorf("%s org %v: %+v (obligations %+v), want allowed_by_platform_admin %+v", name, org, d, d.Obligations, want)
			}
		}
	}
	for _, org := range []Status{StatusRemoved, StatusInvited} {
		if d := e.Evaluate(req, snap("platform_admin", org)); d.Allowed {
			t.Errorf("platform_admin org %v: %+v, want deny", org, d)
		}
	}
	if d := e.Evaluate(req, snap("platform_owner", StatusActive)); d.Allowed || d.Reason != ReasonNoMembership {
		t.Errorf("platform_owner, no membership row: %+v, want deny no_membership", d)
	}
}

// Making the action filtered changes neither its cache class (none: a high-risk allow is never cached) nor its
// impersonation (denied): a delegated session is refused it for every role.
func TestAcrCredentialIssueStaysNeverCachedAndImpersonationDenied(t *testing.T) {
	a, err := policycatalog.Default().Action("acr.credential.issue")
	if err != nil {
		t.Fatal(err)
	}
	if a.CacheClass != policycatalog.CacheNone || a.RiskClass != policycatalog.RiskHigh || a.Impersonation != policycatalog.ImpersonationDenied {
		t.Fatalf("acr.credential.issue = %+v, want high risk, cache none, impersonation denied", a)
	}
	e := NewEvaluator()
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		if d := e.Evaluate(orgRequest("acr.credential.issue"), delegated(role, []string{"acr.credential.issue"}, "platform_admin")); d.Allowed {
			t.Errorf("delegated %s: %+v, want deny", role, d)
		}
	}
}

// Impersonation as built: an owner-exempt delegated session (platform_owner actor) acting as a default member gets
// the member's own answer on acr.credential.issue, org-wide, audited; a platform_admin actor stays impersonation-denied.
func TestDelegatedAcrCredentialIssueIsTheTargetsOwnAnswer(t *testing.T) {
	e := NewEvaluator()
	req := orgRequest("acr.credential.issue")
	d := e.Evaluate(req, delegated("member", []string{"acr.credential.issue"}, ownerActorRol))
	if !d.Allowed || d.Obligations == nil || d.Obligations.RepositoryScope != RepositoryScopeOrganization || d.Audit == nil {
		t.Errorf("platform_owner actor -> default member: %+v %+v, want the member's org-wide allow, audited", d, d.Obligations)
	}
	if d := e.Evaluate(req, delegated("member", []string{"acr.credential.issue"}, "platform_admin")); d.Allowed || d.Reason != ReasonImpersonationDenied {
		t.Errorf("platform_admin actor -> default member: %+v, want impersonation_denied", d)
	}
}
