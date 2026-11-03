package authorization

import (
	"fmt"
	"slices"
	"testing"
)

const filteredOrgAction = "analytics.read"

func grantOf(role, key string) Grant {
	return Grant{OrgID: testOrg, Role: role, ResourceKind: "repository", ResourceKey: key}
}

func orgAllowObligations(t *testing.T, role string, grants ...Grant) *Obligations {
	t.Helper()
	d := NewEvaluator().Evaluate(orgRequest(filteredOrgAction), repoSnapshot(role, grants...))
	if !d.Allowed {
		t.Fatalf("%s asking %s: %+v", role, filteredOrgAction, d)
	}
	if d.Obligations == nil {
		return &Obligations{}
	}
	return d.Obligations
}

func TestOrgAllowCarriesTheTenantScope(t *testing.T) {
	for _, action := range []string{"org.read", "analytics.read", "teams.read"} {
		d := NewEvaluator().Evaluate(orgRequest(action), repoSnapshot("member"))
		if !d.Allowed || d.Obligations == nil || d.Obligations.TenantScope != testOrg {
			t.Errorf("%s: want an allow with tenant_scope %s, got %+v %+v", action, testOrg, d, d.Obligations)
		}
	}
	d := NewEvaluator().Evaluate(platformRequest("admin.users"), platformSnapshot("platform_admin"))
	if !d.Allowed || d.Obligations != nil {
		t.Errorf("a platform allow carries no tenant scope: %+v %+v", d, d.Obligations)
	}
}

func TestMemberWithNoGrantRowIsUnrestrictedInsideTheOrganization(t *testing.T) {
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		o := orgAllowObligations(t, role)
		if o == nil || o.TenantScope != testOrg || o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 {
			t.Errorf("%s with no grant row: %+v, want tenant scope only", role, o)
		}
	}
}

func TestMemberWithLiveGrantsGetsTheSortedKeys(t *testing.T) {
	o := orgAllowObligations(t, "member", liveGrant("acme/web"), liveGrant("acme/api"), liveGrant("acme/api"))
	if o == nil || !o.RepositoryRestricted || !slices.Equal(o.RepositoryAllowlist, []string{"acme/api", "acme/web"}) {
		t.Fatalf("obligations %+v, want [acme/api acme/web]", o)
	}
}

func TestAWildcardGrantEmitsNoWildcard(t *testing.T) {
	o := orgAllowObligations(t, "member", liveGrant("*"), liveGrant("acme/api"))
	if o == nil || o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 || o.TenantScope != testOrg {
		t.Fatalf("a live wildcard grant: %+v, want tenant scope only (unrestricted), no list", o)
	}
}

func TestGrantsThatAreNotLiveLeaveNoneNotUnrestricted(t *testing.T) {
	other := liveGrant("acme/other")
	other.OrgID = "org-2"
	for name, rows := range map[string][]Grant{
		"expired":                {expiredGrant("acme/api")},
		"revoked":                {revokedGrant("acme/api")},
		"only another org":       {other},
		"expired at the instant": {func() Grant { g := liveGrant("acme/api"); g.ExpiresAtUnixNano = nowNano; return g }()},
	} {
		o := NewEvaluator().Evaluate(orgRequest(filteredOrgAction), repoSnapshot("member", rows...)).Obligations
		if name == "only another org" {
			if o == nil || o.RepositoryRestricted {
				t.Errorf("%s: rows of another organization are ignored, the pair has no row: %+v", name, o)
			}
			continue
		}
		if o == nil || !o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 {
			t.Errorf("%s: %+v, want an empty restricted list (none)", name, o)
		}
	}
}

func TestAGrantWhoseRoleLacksTheActionIsLeftOut(t *testing.T) {
	d := NewEvaluator().Evaluate(orgRequest("analytics.export"), repoSnapshot("member", grantOf("viewer", "acme/v"), grantOf("member", "acme/m")))
	if !d.Allowed || d.Obligations == nil || !slices.Equal(d.Obligations.RepositoryAllowlist, []string{"acme/m"}) {
		t.Fatalf("analytics.export: %+v %+v, want only the member grant (viewer lacks export)", d, d.Obligations)
	}
}

func TestRepositoryAllowCarriesItsOneKey(t *testing.T) {
	for name, s := range map[string]grantFake{
		"live grant":         repoSnapshot("member", liveGrant("acme/api")),
		"membership default": repoSnapshot("member"),
	} {
		d := NewEvaluator().Evaluate(repoRequest("acme/api"), s)
		if !d.Allowed || d.Obligations == nil || d.Obligations.TenantScope != testOrg || !d.Obligations.RepositoryRestricted ||
			!slices.Equal(d.Obligations.RepositoryAllowlist, []string{"acme/api"}) {
			t.Errorf("%s: %+v %+v, want tenant scope and [acme/api]", name, d, d.Obligations)
		}
	}
}

func TestAWildcardRepositoryKeyIsRefused(t *testing.T) {
	for name, s := range map[string]grantFake{
		"membership default": repoSnapshot("owner"),
		"wildcard grant":     repoSnapshot("member", liveGrant("*")),
	} {
		wantDeny(t, name, NewEvaluator().Evaluate(repoRequest("*"), s), ReasonInvalidRequest)
	}
}

func TestAnAllowlistOverTheBoundIsDeniedNotTruncated(t *testing.T) {
	if MaxRepositoryAllowlist != 256 {
		t.Fatalf("MaxRepositoryAllowlist %d", MaxRepositoryAllowlist)
	}
	build := func(n int) []Grant {
		var rows []Grant
		for i := 0; i < n; i++ {
			rows = append(rows, liveGrant(fmt.Sprintf("o/r%03d", i)))
		}
		return rows
	}
	if o := orgAllowObligations(t, "member", build(MaxRepositoryAllowlist)...); len(o.RepositoryAllowlist) != MaxRepositoryAllowlist {
		t.Errorf("at the bound: %d keys, want %d", len(o.RepositoryAllowlist), MaxRepositoryAllowlist)
	}
	d := NewEvaluator().Evaluate(orgRequest(filteredOrgAction), repoSnapshot("member", build(MaxRepositoryAllowlist+1)...))
	wantDeny(t, "one over the bound", d, ReasonRepositoryAllowlistTooLarge)
	if d.Obligations != nil || d.CacheClass != CacheNone {
		t.Errorf("a deny carries no obligations and is never cached: %+v", d)
	}
	if !slices.Contains(AllReasons(), ReasonRepositoryAllowlistTooLarge) {
		t.Error("AllReasons lacks repository_allowlist_too_large")
	}
}

func TestAFilteredOrgActionNeedsASnapshotThatReportsGrants(t *testing.T) {
	wantDeny(t, "plain snapshot", NewEvaluator().Evaluate(orgRequest(filteredOrgAction), orgSnapshot("member")), ReasonGrantAbsent)
}

// The decision and the obligation never disagree: a key is in the obligation exactly when the
// repository decision of the same principal, action and key allows it (evaluateRepository).
func TestObligationEqualsTheRepositoryDecisionForEveryRoleClass(t *testing.T) {
	e := NewEvaluator()
	keys := []string{"acme/api", "acme/web", "acme/old", "acme/gone", "acme/other-role", "acme/never"}
	expiredRow, revokedRow := expiredGrant("acme/old"), revokedGrant("acme/gone")
	rowSets := map[string][]Grant{
		"no row":        nil,
		"live keys":     {liveGrant("acme/api"), liveGrant("acme/web")},
		"wildcard":      {liveGrant("*")},
		"wildcard+key":  {liveGrant("*"), liveGrant("acme/api")},
		"dead rows":     {expiredRow, revokedRow},
		"live and dead": {liveGrant("acme/api"), expiredRow, revokedRow},
	}
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		for _, action := range []string{"analytics.read", "analytics.export", "metrics.read", "work_items.read", "git.read"} {
			if !e.cat.Allows(role, action) {
				continue
			}
			act, err := e.cat.Action(action)
			if err != nil {
				t.Fatal(err)
			}
			for setName, rows := range rowSets {
				s := repoSnapshot(role, rows...)
				d := e.Evaluate(orgRequest(action), s)
				if !d.Allowed {
					t.Errorf("%s %s %s: %+v", role, action, setName, d)
					continue
				}
				o := d.Obligations
				for _, key := range keys {
					want := e.evaluateRepository(Request{Action: action, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: key}, act, s, 1).Allowed
					got := o != nil && (!o.RepositoryRestricted || slices.Contains(o.RepositoryAllowlist, key))
					if got != want {
						t.Errorf("%s %s %s key %s: obligation says %v, the repository decision says %v (%+v)", role, action, setName, key, got, want, o)
					}
				}
			}
		}
	}
}

func TestNoEmittedAllowlistHoldsAWildcard(t *testing.T) {
	e := NewEvaluator()
	for _, rows := range [][]Grant{nil, {liveGrant("*")}, {liveGrant("*"), liveGrant("acme/api")}, {liveGrant("acme/api")}, {revokedGrant("*")}} {
		for _, req := range []Request{orgRequest(filteredOrgAction), repoRequest("acme/api")} {
			d := e.Evaluate(req, repoSnapshot("member", rows...))
			if d.Obligations != nil && slices.Contains(d.Obligations.RepositoryAllowlist, "*") {
				t.Errorf("%+v over %+v emitted a wildcard: %+v", req, rows, d.Obligations)
			}
		}
	}
}

func TestADeniedDecisionCarriesNoObligations(t *testing.T) {
	e := NewEvaluator()
	cases := map[string]Decision{
		"suspended principal": e.Evaluate(orgRequest(filteredOrgAction), func() grantFake { s := repoSnapshot("member"); s.principal = StatusSuspended; return s }()),
		"role lacks":          e.Evaluate(orgRequest("analytics.export"), repoSnapshot("viewer")),
		"no membership":       e.Evaluate(orgRequest(filteredOrgAction), grantFake{fakeSnapshot: fakeSnapshot{principal: StatusActive, orgStatus: map[string]Status{testOrg: StatusActive}}, now: nowNano}),
		"wildcard key":        e.Evaluate(repoRequest("*"), repoSnapshot("owner")),
	}
	for name, d := range cases {
		if d.Allowed || d.Obligations != nil {
			t.Errorf("%s: %+v %+v", name, d, d.Obligations)
		}
	}
}

func TestDelegatedFilteredAllowKeepsItsAllowlist(t *testing.T) {
	e := NewEvaluator()
	d := e.Evaluate(orgRequest(filteredOrgAction), delegatedRepo([]string{filteredOrgAction}, liveGrant("acme/api")))
	if !d.Allowed || d.Obligations == nil || d.Obligations.MinimumAssurance == "" ||
		!d.Obligations.RepositoryRestricted || !slices.Equal(d.Obligations.RepositoryAllowlist, []string{"acme/api"}) {
		t.Fatalf("%+v %+v, want assurance and the narrowed list", d, d.Obligations)
	}
}
