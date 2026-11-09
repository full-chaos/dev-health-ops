package authorization

import (
	"encoding/json"
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
	d := filteredOrgEvaluator().Evaluate(orgRequest(filteredOrgAction), repoSnapshot(role, grants...))
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
		d := filteredOrgEvaluator().Evaluate(orgRequest(action), repoSnapshot("member"))
		if !d.Allowed || d.Obligations == nil || d.Obligations.TenantScope != testOrg {
			t.Errorf("%s: want an allow with tenant_scope %s, got %+v %+v", action, testOrg, d, d.Obligations)
		}
	}
	d := filteredOrgEvaluator().Evaluate(platformRequest("admin.users"), platformSnapshot("platform_admin"))
	if !d.Allowed || d.Obligations != nil {
		t.Errorf("a platform allow carries no tenant scope: %+v %+v", d, d.Obligations)
	}
}

// obligationWire is the JSON of the obligations a decision core allow holds: the members a consumer reads.
func obligationWire(t *testing.T, o *Obligations) map[string]any {
	t.Helper()
	raw, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// wantOrganizationScope fails unless o states a read of every repository of the organization by
// repository_scope organization, with no allowlist member and the tenant scope.
func wantOrganizationScope(t *testing.T, name string, o *Obligations) {
	t.Helper()
	if o == nil {
		t.Errorf("%s: no obligations, want repository_scope organization", name)
		return
	}
	m := obligationWire(t, o)
	if m["repository_scope"] != "organization" || o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 || o.TenantScope != testOrg {
		t.Errorf("%s: obligations %v, want repository_scope organization, the tenant scope and no allowlist", name, m)
	}
}

func TestMemberWithNoGrantRowReadsEveryRepositoryByRepositoryScope(t *testing.T) {
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		wantOrganizationScope(t, role+" with no grant row", orgAllowObligations(t, role))
	}
}

func TestMemberWithLiveGrantsGetsTheSortedKeys(t *testing.T) {
	o := orgAllowObligations(t, "member", liveGrant("acme/web"), liveGrant("acme/api"), liveGrant("acme/api"))
	if o == nil || !o.RepositoryRestricted || !slices.Equal(o.RepositoryAllowlist, []string{"acme/api", "acme/web"}) {
		t.Fatalf("obligations %+v, want [acme/api acme/web]", o)
	}
}

func TestAWildcardGrantIsRepositoryScopeNotAWildcard(t *testing.T) {
	wantOrganizationScope(t, "a live wildcard grant", orgAllowObligations(t, "member", liveGrant("*"), liveGrant("acme/api")))
}

func TestARestrictedAllowCarriesNoRepositoryScope(t *testing.T) {
	for name, rows := range map[string][]Grant{
		"live keys": {liveGrant("acme/api")},
		"none live": {revokedGrant("acme/api")},
	} {
		o := orgAllowObligations(t, "member", rows...)
		if m := obligationWire(t, o); m["repository_scope"] != nil || !o.RepositoryRestricted {
			t.Errorf("%s: obligations %v, want the allowlist member alone", name, m)
		}
	}
}

func TestAnUnfilteredOrgAllowCarriesNoRepositoryMember(t *testing.T) {
	for _, action := range []string{"org.read", "teams.read", "settings.read"} {
		d := filteredOrgEvaluator().Evaluate(orgRequest(action), repoSnapshot("member", liveGrant("acme/api")))
		if !d.Allowed {
			t.Fatalf("%s: %+v", action, d)
		}
		if m := obligationWire(t, d.Obligations); m["repository_scope"] != nil || m["repository_allowlist"] != nil {
			t.Errorf("%s: obligations %v, want no repository member", action, m)
		}
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
		o := filteredOrgEvaluator().Evaluate(orgRequest(filteredOrgAction), repoSnapshot("member", rows...)).Obligations
		if name == "only another org" {
			wantOrganizationScope(t, name+": rows of another organization are ignored, the pair has no row", o)
			continue
		}
		if o == nil || !o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 {
			t.Errorf("%s: %+v, want an empty restricted list (none)", name, o)
		}
	}
}

func TestAGrantWhoseRoleLacksTheActionIsLeftOut(t *testing.T) {
	d := filteredOrgEvaluator().Evaluate(orgRequest("analytics.export"), repoSnapshot("member", grantOf("viewer", "acme/v"), grantOf("member", "acme/m")))
	if !d.Allowed || d.Obligations == nil || !slices.Equal(d.Obligations.RepositoryAllowlist, []string{"acme/m"}) {
		t.Fatalf("analytics.export: %+v %+v, want only the member grant (viewer lacks export)", d, d.Obligations)
	}
}

// An allow by an exact grant carries its one key; an org-wide allow (membership default, wildcard grant) carries
// repository_scope organization instead (TestRepositoryAllowCarriesTheOrganizationScopeWhenOrgWideInFact).
func TestRepositoryAllowCarriesItsOneKey(t *testing.T) {
	for name, s := range map[string]grantFake{
		"live grant": repoSnapshot("member", liveGrant("acme/api")),
	} {
		d := filteredOrgEvaluator().Evaluate(repoRequest("acme/api"), s)
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
		wantDeny(t, name, filteredOrgEvaluator().Evaluate(repoRequest("*"), s), ReasonInvalidRequest)
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
	d := filteredOrgEvaluator().Evaluate(orgRequest(filteredOrgAction), repoSnapshot("member", build(MaxRepositoryAllowlist+1)...))
	wantDeny(t, "one over the bound", d, ReasonRepositoryAllowlistTooLarge)
	if d.Obligations != nil || d.CacheClass != CacheNone {
		t.Errorf("a deny carries no obligations and is never cached: %+v", d)
	}
	if !slices.Contains(AllReasons(), ReasonRepositoryAllowlistTooLarge) {
		t.Error("AllReasons lacks repository_allowlist_too_large")
	}
}

func TestAFilteredOrgActionNeedsASnapshotThatReportsGrants(t *testing.T) {
	wantDeny(t, "plain snapshot", filteredOrgEvaluator().Evaluate(orgRequest(filteredOrgAction), orgSnapshot("member")), ReasonGrantAbsent)
}

// The decision and the obligation never disagree: a key is in the obligation exactly when the
// repository decision of the same principal, action and key allows it (evaluateRepository).
func TestObligationEqualsTheRepositoryDecisionForEveryRoleClass(t *testing.T) {
	e := filteredOrgEvaluator()
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
					m := obligationWire(t, o)
					if (m["repository_scope"] != nil) == o.RepositoryRestricted {
						t.Errorf("%s %s %s: obligations %v, want exactly one repository member", role, action, setName, m)
					}
					got := o != nil && (m["repository_scope"] == "organization" || slices.Contains(o.RepositoryAllowlist, key))
					if got != want {
						t.Errorf("%s %s %s key %s: obligation says %v, the repository decision says %v (%+v)", role, action, setName, key, got, want, o)
					}
				}
			}
		}
	}
}

func TestNoEmittedAllowlistHoldsAWildcard(t *testing.T) {
	e := filteredOrgEvaluator()
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
	e := filteredOrgEvaluator()
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
	e := filteredOrgEvaluator()
	d := e.Evaluate(orgRequest(filteredOrgAction), delegatedRepo([]string{filteredOrgAction}, liveGrant("acme/api")))
	if !d.Allowed || d.Obligations == nil || d.Obligations.MinimumAssurance == "" ||
		!d.Obligations.RepositoryRestricted || !slices.Equal(d.Obligations.RepositoryAllowlist, []string{"acme/api"}) {
		t.Fatalf("%+v %+v, want assurance and the narrowed list", d, d.Obligations)
	}
}
