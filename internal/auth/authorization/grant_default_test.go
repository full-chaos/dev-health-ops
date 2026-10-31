package authorization

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

func defaultAllowed(t *testing.T, name string, d Decision, now int64) {
	t.Helper()
	if !d.Allowed || d.Reason != ReasonAllowedByMembershipDefault || d.CacheClass != CacheDecisionBound || d.ExpiresAtUnixNano != now+testBound {
		t.Errorf("%s: want membership default allow, decision_bound until now+bound: %+v", name, d)
	}
}

func TestMembershipDefault_NoRowsAllows(t *testing.T) {
	e := plantedEvaluator()
	defaultAllowed(t, "no rows", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member")), nowNano)
	defaultAllowed(t, "no rows, org wide key", e.Evaluate(repoRequest("*"), repoSnapshot("viewer")), nowNano)
}

func TestMembershipDefault_OwnerReadsAsAdmin(t *testing.T) {
	e := newEvaluator(plantedCatalog{base: policycatalog.Default(), holders: map[string]bool{"admin": true}})
	defaultAllowed(t, "owner", e.Evaluate(repoRequest("acme/api"), repoSnapshot("owner")), nowNano)
	defaultAllowed(t, "admin", e.Evaluate(repoRequest("acme/api"), repoSnapshot("admin")), nowNano)
	for _, role := range []string{"viewer", "member"} {
		wantDeny(t, role+" lacks the action", e.Evaluate(repoRequest("acme/api"), repoSnapshot(role)), ReasonRoleLacksAction)
	}
	wantDeny(t, "unknown role", e.Evaluate(repoRequest("acme/api"), repoSnapshot("ghost")), ReasonRoleUnknown)
	wantDeny(t, "platform role as membership role", e.Evaluate(repoRequest("acme/api"), repoSnapshot("platform_admin")), ReasonRoleScopeMismatch)
}

func TestMembershipDefault_AnyRowOfThePairEndsIt(t *testing.T) {
	e := plantedEvaluator()
	row := func(key string, mut func(*Grant)) Grant {
		g := liveGrant(key)
		mut(&g)
		return g
	}
	wantDeny(t, "one revoked row", e.Evaluate(repoRequest("acme/api"),
		repoSnapshot("member", row("acme/api", func(g *Grant) { g.Revoked = true }))), ReasonGrantRevoked)
	wantDeny(t, "one revoked row of another repo", e.Evaluate(repoRequest("acme/api"),
		repoSnapshot("member", row("acme/web", func(g *Grant) { g.Revoked = true }))), ReasonRepositoryNotGranted)
	wantDeny(t, "one expired row", e.Evaluate(repoRequest("acme/api"),
		repoSnapshot("member", row("acme/api", func(g *Grant) { g.ExpiresAtUnixNano = nowNano - 1 }))), ReasonGrantExpired)
	wantDeny(t, "one expired row of another repo", e.Evaluate(repoRequest("acme/api"),
		repoSnapshot("member", row("acme/web", func(g *Grant) { g.ExpiresAtUnixNano = nowNano }))), ReasonRepositoryNotGranted)
	g := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", liveGrant("acme/api")))
	if !g.Allowed || g.Reason != ReasonAllowedByRepositoryGrant {
		t.Errorf("live exact grant on A: %+v", g)
	}
	wantDeny(t, "live exact grant on A, request B", e.Evaluate(repoRequest("acme/web"), repoSnapshot("member", liveGrant("acme/api"))), ReasonRepositoryNotGranted)
	// A row of another organization is not a row of this pair.
	foreign := liveGrant("acme/web")
	foreign.OrgID = orgB
	defaultAllowed(t, "foreign row only", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", foreign)), nowNano)
	// A row of any resource kind counts as a row of the pair.
	odd := liveGrant("acme/web")
	odd.ResourceKind = "organization"
	wantDeny(t, "row of another kind", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", odd)), ReasonRepositoryNotGranted)
}

func TestMembershipDefault_GrantReadErrorDenies(t *testing.T) {
	e := plantedEvaluator()
	for name, rows := range map[string][]Grant{"no rows": nil, "live row": {liveGrant("acme/api")}, "wildcard": {liveGrant("*")}} {
		s := repoSnapshot("member", rows...)
		s.grantErr = errors.New("read failed")
		d := e.Evaluate(repoRequest("acme/api"), s)
		wantDeny(t, "read error with "+name, d, ReasonGrantReadFailed)
		if d.CacheClass != CacheNone {
			t.Errorf("deny cache class %q", d.CacheClass)
		}
	}
}

func TestMembershipDefault_ServiceAccountGetsNone(t *testing.T) {
	e := plantedEvaluator()
	s := repoSnapshot("member")
	s.memberships = map[string]Membership{}
	s.sa = map[string]ServiceAccount{testOrg: {OrgID: testOrg, Status: StatusActive}}
	wantDeny(t, "service account, no rows", e.Evaluate(repoRequest("acme/api"), s), ReasonGrantAbsent)
	// Even with a membership row beside the service-account row.
	s.memberships = map[string]Membership{testOrg: {OrgID: testOrg, Role: "admin", Status: StatusActive}}
	wantDeny(t, "service account with membership, no rows", e.Evaluate(repoRequest("acme/api"), s), ReasonGrantAbsent)
}

func TestMembershipDefault_CrossOrgNoRowsDenies(t *testing.T) {
	e := plantedEvaluator()
	s := repoSnapshot("admin")
	s.orgStatus = map[string]Status{orgA: StatusActive, orgB: StatusActive}
	s.memberships = map[string]Membership{orgA: {OrgID: orgA, Role: "admin", Status: StatusActive}}
	req := Request{Action: repoAction, ResourceKind: "repository", OrganizationID: orgB, ResourceKey: "acme/api"}
	wantDeny(t, "member of X, resource in Y", e.Evaluate(req, s), ReasonNoMembership)
	s.memberships = map[string]Membership{orgB: {OrgID: orgA, Role: "admin", Status: StatusActive}}
	wantDeny(t, "membership row names X for Y", e.Evaluate(req, s), ReasonOrgMismatch)
}

func TestMembershipDefault_ClockAndTheThirtySecondBound(t *testing.T) {
	e := plantedEvaluator()
	s := repoSnapshot("member")
	s.now = 0
	wantDeny(t, "no clock", e.Evaluate(repoRequest("acme/api"), s), ReasonClockUnavailable)
	if d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member")); d.ExpiresAtUnixNano != nowNano+30_000_000_000 {
		t.Errorf("expiry %d, want clock + 30 s", d.ExpiresAtUnixNano-nowNano)
	}
	// Near the largest instant the expiry saturates: it never wraps negative.
	s = repoSnapshot("member")
	s.now = 1<<63 - 1 - 5
	if d := e.Evaluate(repoRequest("acme/api"), s); !d.Allowed || d.ExpiresAtUnixNano != 1<<63-1 {
		t.Errorf("saturating expiry: %+v", d)
	}
	// No snapshot or evaluator input carries a bound: it is a package constant.
	it := reflect.TypeOf((*GrantSnapshot)(nil)).Elem()
	for i := 0; i < it.NumMethod(); i++ {
		if strings.Contains(strings.ToLower(it.Method(i).Name), "bound") {
			t.Errorf("GrantSnapshot method %s carries a bound", it.Method(i).Name)
		}
	}
}

func TestDecisionCacheClass(t *testing.T) {
	e := plantedEvaluator()
	g := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", liveGrant("acme/api")))
	if !g.Allowed || g.CacheClass != CacheResourceRevisionBound || g.ExpiresAtUnixNano != 0 {
		t.Errorf("grant allow: %+v", g)
	}
	d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member"))
	if !d.Allowed || d.CacheClass != CacheDecisionBound {
		t.Errorf("default allow: %+v", d)
	}
	for name, dec := range map[string]Decision{
		"deny no grant": e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", liveGrant("acme/web"))),
		"deny invalid":  e.Evaluate(Request{}, repoSnapshot("member")),
		"deny no clock": e.Evaluate(repoRequest("acme/api"), grantFake{fakeSnapshot: orgSnapshot("member")}),
	} {
		if dec.Allowed || dec.CacheClass != CacheNone || dec.ExpiresAtUnixNano != 0 {
			t.Errorf("%s: %+v", name, dec)
		}
	}
	// An action whose catalog class is none is never cached, by either path.
	none := newEvaluator(plantedCatalog{base: policycatalog.Default(), cacheClass: policycatalog.CacheNone})
	for name, s := range map[string]grantFake{"default": repoSnapshot("member"), "grant": repoSnapshot("member", liveGrant("acme/api"))} {
		if dec := none.Evaluate(repoRequest("acme/api"), s); !dec.Allowed || dec.CacheClass != CacheNone || dec.ExpiresAtUnixNano != 0 {
			t.Errorf("class none, %s: %+v", name, dec)
		}
	}
	for _, c := range []CacheClass{CacheNone, CacheDecisionBound, CacheResourceRevisionBound} {
		if string(c) == "" {
			t.Error("empty cache class constant")
		}
	}
}

func TestHoldsByMembershipDefault(t *testing.T) {
	e := plantedEvaluator()
	row := liveGrant("acme/api")
	for _, tc := range []struct {
		name string
		e    *Evaluator
		role string
		act  string
		rows []Grant
		want bool
	}{
		{"member no rows", e, "member", repoAction, nil, true},
		{"owner reads as admin", newEvaluator(plantedCatalog{base: policycatalog.Default(), holders: map[string]bool{"admin": true}}), "owner", repoAction, nil, true},
		{"any row", e, "member", repoAction, []Grant{row}, false},
		{"unknown action", e, "member", "acr.nope.read", nil, false},
		{"organization action is not repository scoped", e, "member", "org.read", nil, false},
		{"unknown role", e, "ghost", repoAction, nil, false},
		{"nil evaluator", nil, "member", repoAction, nil, false},
	} {
		if got := tc.e.HoldsByMembershipDefault(tc.role, tc.act, tc.rows); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}
