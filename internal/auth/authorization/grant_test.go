package authorization

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	testBound  = int64(30_000_000_000) // the 30 s of TRD section 14, written out independently of the package constant
	repoAction = "acr.context.read"
	orgA       = "org-a"
	orgB       = "org-b"
	nowNano    = int64(1_800_000_000_000_000_000)
)

// plantedCatalog adds one repository-scoped action to the embedded catalog
// until the real catalog carries one (CHAOS-10774). viewer, member and admin
// hold it; owner does not, so a grant naming owner lacks the action.
type plantedCatalog struct {
	base       *policycatalog.Catalog
	holders    map[string]bool // nil: plantedHolders
	cacheClass string          // "": resource_revision_bound
	noFlag     bool            // the action carries no impersonation flag
}

var plantedHolders = map[string]bool{"viewer": true, "member": true, "admin": true}

func (p plantedCatalog) Version() int { return p.base.Version() }
func (p plantedCatalog) Action(id string) (policycatalog.Action, error) {
	if id == repoAction {
		return policycatalog.Action{ID: id, Scope: policycatalog.ScopeOrganization, ResourceKind: "repository",
			RiskClass: policycatalog.RiskLow, CacheClass: p.class(), Impersonation: p.flag()}, nil
	}
	return p.base.Action(id)
}
func (p plantedCatalog) Role(id string) (policycatalog.Role, error) { return p.base.Role(id) }
func (p plantedCatalog) flag() string {
	if p.noFlag {
		return ""
	}
	return policycatalog.ImpersonationAllowed
}
func (p plantedCatalog) class() string {
	if p.cacheClass == "" {
		return policycatalog.CacheResourceRevisionBound
	}
	return p.cacheClass
}
func (p plantedCatalog) Allows(role, action string) bool {
	if action == repoAction {
		if p.holders != nil {
			return p.holders[role]
		}
		return plantedHolders[role]
	}
	return p.base.Allows(role, action)
}

func plantedEvaluator() *Evaluator {
	return newEvaluator(plantedCatalog{base: policycatalog.Default()})
}

// grantFake answers every Grants call with the same rows whatever org is
// asked for, so the evaluator itself must hold the org line.
type grantFake struct {
	fakeSnapshot
	grants   []Grant
	workload *WorkloadLimits
	grantErr error
	sa       map[string]ServiceAccount
	now      int64
}

func (g grantFake) Grants(string) ([]Grant, error) { return g.grants, g.grantErr }
func (g grantFake) Workload() (WorkloadLimits, bool) {
	if g.workload == nil {
		return WorkloadLimits{}, false
	}
	return *g.workload, true
}
func (g grantFake) ServiceAccount(id string) (ServiceAccount, bool) {
	s, ok := g.sa[id]
	return s, ok
}
func (g grantFake) NowUnixNano() int64 { return g.now }

func repoSnapshot(role string, grants ...Grant) grantFake {
	return grantFake{fakeSnapshot: orgSnapshot(role), grants: grants, now: nowNano}
}

func liveGrant(key string) Grant {
	return Grant{OrgID: testOrg, Role: "viewer", ResourceKind: "repository", ResourceKey: key}
}

func repoRequest(key string) Request {
	return Request{Action: repoAction, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: key}
}

func wantDeny(t *testing.T, name string, d Decision, why Reason) {
	t.Helper()
	if d.Allowed || d.Reason != why {
		t.Errorf("%s: got %+v want deny %s", name, d, why)
	}
}

func TestRepoGrant_AllowsLiveGrant(t *testing.T) {
	e := plantedEvaluator()
	for _, tc := range []struct{ name, grantKey, reqKey string }{
		{"exact key", "acme/api", "acme/api"},
		{"org wide grant, one repo", "*", "acme/api"},
		{"org wide grant, org wide request", "*", "*"},
	} {
		d := e.Evaluate(repoRequest(tc.reqKey), repoSnapshot("member", liveGrant(tc.grantKey)))
		if !d.Allowed || d.Reason != ReasonAllowedByRepositoryGrant {
			t.Errorf("%s: %+v", tc.name, d)
		}
	}
	past := liveGrant("acme/api")
	past.ExpiresAtUnixNano = nowNano + 1
	if d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", past)); !d.Allowed {
		t.Errorf("unexpired grant: %+v", d)
	}
}

func TestRepoGrant_AnyRowEndsTheMembershipDefault(t *testing.T) {
	e := plantedEvaluator()
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		wantDeny(t, role+" other repo", e.Evaluate(repoRequest("acme/api"), repoSnapshot(role, liveGrant("acme/web"))), ReasonRepositoryNotGranted)
	}
	wantDeny(t, "repo grant for org wide request", e.Evaluate(repoRequest("*"), repoSnapshot("member", liveGrant("acme/api"))), ReasonRepositoryNotGranted)
}

func TestRepoGrant_GrantNeedsStanding(t *testing.T) {
	e := plantedEvaluator()
	g := liveGrant("acme/api")
	s := repoSnapshot("member", g)
	s.memberships = map[string]Membership{}
	wantDeny(t, "no membership", e.Evaluate(repoRequest("acme/api"), s), ReasonNoMembership)
	for _, st := range []Status{StatusUnknown, StatusInvited, StatusSuspended, StatusRemoved} {
		s := repoSnapshot("member", g)
		s.memberships = map[string]Membership{testOrg: {OrgID: testOrg, Role: "member", Status: st}}
		wantDeny(t, "membership "+string(rune('0'+st)), e.Evaluate(repoRequest("acme/api"), s), ReasonMembershipInactive)
	}
	s = repoSnapshot("member", g)
	s.memberships = map[string]Membership{testOrg: {OrgID: orgB, Role: "member", Status: StatusActive}}
	wantDeny(t, "membership of another org", e.Evaluate(repoRequest("acme/api"), s), ReasonOrgMismatch)
	s = repoSnapshot("member", g)
	s.orgStatus = map[string]Status{testOrg: StatusSuspended}
	wantDeny(t, "org suspended", e.Evaluate(repoRequest("acme/api"), s), ReasonOrganizationInactive)
	s = repoSnapshot("member", g)
	s.principal = StatusSuspended
	wantDeny(t, "principal suspended", e.Evaluate(repoRequest("acme/api"), s), ReasonPrincipalInactive)
}

func TestRepoGrant_CrossOrgAlwaysDenies(t *testing.T) {
	e := plantedEvaluator()
	foreign := Grant{OrgID: orgB, Role: "admin", ResourceKind: "repository", ResourceKey: "*"}
	// Member of org a only; the snapshot hands back a grant row of org b.
	s := repoSnapshot("member", foreign)
	s.orgStatus = map[string]Status{orgA: StatusActive, orgB: StatusActive}
	s.memberships = map[string]Membership{orgA: {OrgID: orgA, Role: "member", Status: StatusActive}}
	req := Request{Action: repoAction, ResourceKind: "repository", OrganizationID: orgB, ResourceKey: "acme/api"}
	wantDeny(t, "resource in org b, member of org a only", e.Evaluate(req, s), ReasonNoMembership)
	// Member of both orgs: the org b row is org b's pair, never org a's.
	s.memberships[orgB] = Membership{OrgID: orgB, Role: "member", Status: StatusActive}
	req.OrganizationID = orgB
	if d := e.Evaluate(req, s); !d.Allowed || d.Reason != ReasonAllowedByRepositoryGrant {
		t.Errorf("org b row in org b: %+v", d)
	}
	req.OrganizationID = orgA
	own := liveGrant("acme/web")
	own.OrgID = orgA
	s.grants = []Grant{foreign, own}
	wantDeny(t, "foreign wildcard beside own row", e.Evaluate(req, s), ReasonRepositoryNotGranted)
	// Membership that names another org is refused whatever the key.
	m := repoSnapshot("member", liveGrant("*"))
	m.memberships = map[string]Membership{testOrg: {OrgID: orgB, Role: "member", Status: StatusActive}}
	wantDeny(t, "membership row of another org", e.Evaluate(repoRequest("acme/api"), m), ReasonOrgMismatch)
	// Org ids are the only line: case and space variants never reach a lookup.
	for _, id := range []string{"Org-A", "org-a ", "ORG-A", "org-a\x00"} {
		bad := req
		bad.OrganizationID = id
		wantDeny(t, "org id variant "+id, e.Evaluate(bad, s), ReasonInvalidRequest)
	}
}

func TestRepoGrant_ExpiryAndRevocation(t *testing.T) {
	e := plantedEvaluator()
	at := func(exp int64, revoked bool) Grant {
		g := liveGrant("acme/api")
		g.ExpiresAtUnixNano = exp
		g.Revoked = revoked
		return g
	}
	wantDeny(t, "expired", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(nowNano-1, false))), ReasonGrantExpired)
	wantDeny(t, "expires exactly now", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(nowNano, false))), ReasonGrantExpired)
	wantDeny(t, "negative expiry", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(-5, false))), ReasonGrantExpired)
	wantDeny(t, "revoked", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(0, true))), ReasonGrantRevoked)
	wantDeny(t, "revoked and unexpired", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(nowNano+1, true))), ReasonGrantRevoked)
	wantDeny(t, "revoked and expired", e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(nowNano-1, true))), ReasonGrantRevoked)
	if d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(0, true), at(nowNano+1, false))); !d.Allowed {
		t.Errorf("a live row beside a revoked row: %+v", d)
	}
	if d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", at(nowNano-1, false), at(0, false))); !d.Allowed {
		t.Errorf("a live row beside an expired row: %+v", d)
	}
}

func TestRepoGrant_ClockUnavailableDenies(t *testing.T) {
	e := plantedEvaluator()
	for _, now := range []int64{0, -1, -nowNano} {
		s := repoSnapshot("member", liveGrant("*"))
		s.now = now
		wantDeny(t, "clock", e.Evaluate(repoRequest("acme/api"), s), ReasonClockUnavailable)
	}
}

func TestRepoGrant_GrantRoleAndShapeAreChecked(t *testing.T) {
	e := plantedEvaluator()
	mk := func(mut func(*Grant)) Grant {
		g := liveGrant("acme/api")
		mut(&g)
		return g
	}
	cases := map[string]Grant{
		"unknown role":          mk(func(g *Grant) { g.Role = "ghost" }),
		"role lacks action":     mk(func(g *Grant) { g.Role = "owner" }),
		"empty role":            mk(func(g *Grant) { g.Role = "" }),
		"platform role":         mk(func(g *Grant) { g.Role = "platform_admin" }),
		"organization kind":     mk(func(g *Grant) { g.ResourceKind = "organization" }),
		"empty kind":            mk(func(g *Grant) { g.ResourceKind = "" }),
		"uppercase key":         mk(func(g *Grant) { g.ResourceKey = "Acme/Api" }),
		"key with trailing nul": mk(func(g *Grant) { g.ResourceKey = "acme/api\x00" }),
		"key prefix":            mk(func(g *Grant) { g.ResourceKey = "acme/ap" }),
		"owner wildcard":        mk(func(g *Grant) { g.ResourceKey = "acme/*" }),
		"nested key":            mk(func(g *Grant) { g.ResourceKey = "acme/team/api" }),
		"empty key":             mk(func(g *Grant) { g.ResourceKey = "" }),
	}
	for name, g := range cases {
		if d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", g)); d.Allowed {
			t.Errorf("%s: allowed %+v", name, d)
		}
	}
	if d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", mk(func(g *Grant) { g.ResourceKey = "*" }))); !d.Allowed {
		t.Errorf("wildcard control: %+v", d)
	}
}

func TestRepoGrant_RequestKeyShape(t *testing.T) {
	e := plantedEvaluator()
	s := repoSnapshot("member", liveGrant("*"))
	long := string(bytes.Repeat([]byte("a"), 101))
	for _, key := range []string{"", "ACME/api", "acme/API", "a/b/c", "acme/api ", " acme/api", "acme/api\n", "acme/api\x00", "acme", "/api", "acme/", "**", "*/*", "acme/*", long + "/api", "acme/" + long, "-a/b", ".a/b"} {
		out, ok := NormalizeRequest(repoRequest(key))
		if ok || out != (Request{}) {
			t.Errorf("accepted key %q", key)
		}
		wantDeny(t, "key "+key, e.Evaluate(repoRequest(key), s), ReasonInvalidRequest)
	}
	for _, key := range []string{"*", "a/b", "acme/api", "a.b-c_d/e", "0/0"} {
		if out, ok := NormalizeRequest(repoRequest(key)); !ok || out != repoRequest(key) {
			t.Errorf("refused or changed key %q: %+v", key, out)
		}
	}
	noOrg := repoRequest("acme/api")
	noOrg.OrganizationID = ""
	if _, ok := NormalizeRequest(noOrg); ok {
		t.Error("repository request without an organization accepted")
	}
	for _, r := range []Request{
		{Action: "org.read", ResourceKind: "organization", OrganizationID: testOrg, ResourceKey: "*"},
		{Action: "org.read", ResourceKind: "organization", OrganizationID: testOrg, ResourceKey: "acme/api"},
		{Action: "admin.users", ResourceKind: "platform", ResourceKey: "*"},
	} {
		if _, ok := NormalizeRequest(r); ok {
			t.Errorf("non-repository request carries a key: %+v", r)
		}
		wantDeny(t, "key on "+r.ResourceKind, e.Evaluate(r, repoSnapshot("owner", liveGrant("*"))), ReasonInvalidRequest)
	}
}

func TestRepoGrant_ServiceAccountReplacesMembership(t *testing.T) {
	e := plantedEvaluator()
	g := liveGrant("acme/api")
	sa := func(org string, st Status) grantFake {
		s := repoSnapshot("member", g)
		s.memberships = map[string]Membership{}
		s.sa = map[string]ServiceAccount{testOrg: {OrgID: org, Status: st}}
		return s
	}
	if d := e.Evaluate(repoRequest("acme/api"), sa(testOrg, StatusActive)); !d.Allowed || d.Reason != ReasonAllowedByRepositoryGrant {
		t.Errorf("active service account: %+v", d)
	}
	wantDeny(t, "sa of another org", e.Evaluate(repoRequest("acme/api"), sa(orgB, StatusActive)), ReasonOrgMismatch)
	for _, st := range []Status{StatusUnknown, StatusInvited, StatusSuspended, StatusRemoved} {
		wantDeny(t, "sa status", e.Evaluate(repoRequest("acme/api"), sa(testOrg, st)), ReasonServiceAccountInactive)
	}
	// A disabled service account never falls back to a membership row.
	s := sa(testOrg, StatusSuspended)
	s.memberships = map[string]Membership{testOrg: {OrgID: testOrg, Role: "member", Status: StatusActive}}
	wantDeny(t, "sa disabled, membership active", e.Evaluate(repoRequest("acme/api"), s), ReasonServiceAccountInactive)
	// An active service account without a grant is still denied.
	s = sa(testOrg, StatusActive)
	s.grants = nil
	wantDeny(t, "sa with no rows gets no default", e.Evaluate(repoRequest("acme/api"), s), ReasonGrantAbsent)
}

func TestRepoGrant_WorkloadLimits(t *testing.T) {
	e := plantedEvaluator()
	lim := func(aud string, auds, acts []string) grantFake {
		s := repoSnapshot("member", liveGrant("acme/api"))
		s.workload = &WorkloadLimits{Audience: aud, Audiences: auds, AllowedActions: acts}
		return s
	}
	ok := lim("svc-a", []string{"svc-a", "svc-b"}, []string{repoAction})
	if d := e.Evaluate(repoRequest("acme/api"), ok); !d.Allowed {
		t.Fatalf("control: %+v", d)
	}
	wantDeny(t, "wrong audience", e.Evaluate(repoRequest("acme/api"), lim("svc-z", []string{"svc-a"}, []string{repoAction})), ReasonWorkloadAudienceDenied)
	wantDeny(t, "empty audience", e.Evaluate(repoRequest("acme/api"), lim("", []string{"svc-a"}, []string{repoAction})), ReasonWorkloadAudienceDenied)
	wantDeny(t, "empty registered list", e.Evaluate(repoRequest("acme/api"), lim("svc-a", nil, []string{repoAction})), ReasonWorkloadAudienceDenied)
	wantDeny(t, "audience case", e.Evaluate(repoRequest("acme/api"), lim("SVC-A", []string{"svc-a"}, []string{repoAction})), ReasonWorkloadAudienceDenied)
	wantDeny(t, "action not listed", e.Evaluate(repoRequest("acme/api"), lim("svc-a", []string{"svc-a"}, []string{"git.read"})), ReasonWorkloadActionDenied)
	wantDeny(t, "no actions", e.Evaluate(repoRequest("acme/api"), lim("svc-a", []string{"svc-a"}, nil)), ReasonWorkloadActionDenied)
	wantDeny(t, "wildcard action", e.Evaluate(repoRequest("acme/api"), lim("svc-a", []string{"svc-a"}, []string{"*"})), ReasonWorkloadActionDenied)
	// The limits only narrow: a workload without a grant is denied.
	s := lim("svc-a", []string{"svc-a"}, []string{repoAction})
	s.grants = []Grant{liveGrant("acme/web")}
	wantDeny(t, "workload without a grant for the repository", e.Evaluate(repoRequest("acme/api"), s), ReasonRepositoryNotGranted)
	// The limits narrow organization actions too.
	o := repoSnapshot("owner")
	o.workload = &WorkloadLimits{Audience: "svc-a", Audiences: []string{"svc-a"}, AllowedActions: []string{"org.read"}}
	if d := e.Evaluate(orgRequest("org.read"), o); !d.Allowed {
		t.Errorf("listed org action: %+v", d)
	}
	wantDeny(t, "unlisted org action", e.Evaluate(orgRequest("org.write"), o), ReasonWorkloadActionDenied)
	o.workload.Audience = "svc-z"
	wantDeny(t, "wrong audience on org action", e.Evaluate(orgRequest("org.read"), o), ReasonWorkloadAudienceDenied)
}

func TestRequestCarriesNoAudience(t *testing.T) {
	rt := reflect.TypeOf(Request{})
	for i := 0; i < rt.NumField(); i++ {
		if n := strings.ToLower(rt.Field(i).Name); strings.Contains(n, "aud") || strings.Contains(n, "workload") || strings.Contains(n, "role") {
			t.Errorf("Request field %s carries authority", rt.Field(i).Name)
		}
	}
}

func TestRepoGrant_SnapshotWithoutGrantsDenies(t *testing.T) {
	wantDeny(t, "plain snapshot", plantedEvaluator().Evaluate(repoRequest("acme/api"), orgSnapshot("admin")), ReasonGrantAbsent)
}

func TestRepoGrant_RealCatalogRepositoryActions(t *testing.T) {
	cat := policycatalog.Default()
	var repoActs []policycatalog.Action
	for _, a := range cat.Actions() {
		if a.ResourceKind == "repository" {
			repoActs = append(repoActs, a)
		}
	}
	if len(repoActs) == 0 {
		t.Fatal("the embedded catalog holds no repository-scoped action (CHAOS-10774 put acr.context.read there)")
	}
	e := NewEvaluator()
	for _, a := range repoActs {
		req := Request{Action: a.ID, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: "acme/api"}
		for _, role := range cat.Roles() {
			if role.Scope != policycatalog.ScopeOrganization {
				continue
			}
			g := Grant{OrgID: testOrg, Role: role.ID, ResourceKind: "repository", ResourceKey: "acme/api"}
			d := e.Evaluate(req, repoSnapshot("viewer", g))
			if want := cat.Allows(role.ID, a.ID); d.Allowed != want {
				t.Errorf("%s via grant role %s: allowed=%v want %v (%s)", a.ID, role.ID, d.Allowed, want, d.Reason)
			}
			other := Grant{OrgID: testOrg, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/web"}
			wantDeny(t, a.ID+" org role "+role.ID+" beside another repo's row", e.Evaluate(req, repoSnapshot(role.ID, other)), ReasonRepositoryNotGranted)
			wantsDefault := cat.Allows(role.ID, a.ID) || (role.ID == "owner" && cat.Allows("admin", a.ID))
			if d := e.Evaluate(req, repoSnapshot(role.ID)); d.Allowed != wantsDefault {
				t.Errorf("%s default for membership role %s: allowed=%v want %v (%s)", a.ID, role.ID, d.Allowed, wantsDefault, d.Reason)
			}
		}
	}
}

func TestRepoGrant_ReasonsAreClosed(t *testing.T) {
	allowed := map[Reason]bool{}
	for _, r := range AllReasons() {
		allowed[r] = true
	}
	for _, r := range []Reason{
		ReasonAllowedByRepositoryGrant, ReasonGrantAbsent, ReasonRepositoryNotGranted, ReasonGrantExpired,
		ReasonGrantRevoked, ReasonWorkloadAudienceDenied, ReasonWorkloadActionDenied,
		ReasonClockUnavailable, ReasonServiceAccountInactive,
		ReasonAllowedByMembershipDefault, ReasonGrantReadFailed,
	} {
		if !allowed[r] {
			t.Errorf("reason %q missing from AllReasons", r)
		}
	}
	e := plantedEvaluator()
	wl := &WorkloadLimits{Audience: "a", Audiences: []string{"a"}, AllowedActions: []string{repoAction}}
	for _, s := range []grantFake{
		repoSnapshot("member"), repoSnapshot("member", liveGrant("*")), repoSnapshot("member", Grant{}),
		{fakeSnapshot: orgSnapshot("member"), grants: []Grant{liveGrant("*")}},
		{fakeSnapshot: orgSnapshot("member"), grants: []Grant{liveGrant("*")}, workload: wl, now: nowNano},
		{fakeSnapshot: orgSnapshot("member"), now: nowNano},
		{fakeSnapshot: orgSnapshot("member"), now: nowNano, grantErr: errors.New("read")},
	} {
		for _, key := range []string{"acme/api", "*", "", "A/b"} {
			if r := e.Evaluate(repoRequest(key), s).Reason; !allowed[r] {
				t.Errorf("reason %q outside the closed set", r)
			}
		}
	}
	if len(AllReasons()) != 26 {
		t.Errorf("reason count %d", len(AllReasons()))
	}
}

func expiredGrant(key string) Grant {
	g := liveGrant(key)
	g.ExpiresAtUnixNano = nowNano - 1
	return g
}

func revokedGrant(key string) Grant {
	g := liveGrant(key)
	g.Revoked = true
	return g
}

func withWorkload(s grantFake, aud string, auds, acts []string) grantFake {
	s.workload = &WorkloadLimits{Audience: aud, Audiences: auds, AllowedActions: acts}
	return s
}

func noClock(s grantFake) grantFake {
	s.now = 0
	return s
}

func readFails(s grantFake) grantFake {
	s.grantErr = errors.New("read failed")
	return s
}

func saWithNoRows() grantFake {
	s := repoSnapshot("member")
	s.memberships = map[string]Membership{}
	s.sa = map[string]ServiceAccount{testOrg: {OrgID: testOrg, Status: StatusActive}}
	return s
}

func inactiveSA() grantFake {
	s := saWithNoRows()
	s.sa = map[string]ServiceAccount{testOrg: {OrgID: testOrg, Status: StatusSuspended}}
	return s
}
