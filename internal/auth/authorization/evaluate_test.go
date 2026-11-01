package authorization

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const testOrg = "org-1"

type fakeSnapshot struct {
	principal    Status
	platformRole string
	orgStatus    map[string]Status
	memberships  map[string]Membership
}

func (f fakeSnapshot) PrincipalStatus() Status { return f.principal }
func (f fakeSnapshot) PlatformRole() string    { return f.platformRole }
func (f fakeSnapshot) OrganizationStatus(id string) Status {
	return f.orgStatus[id]
}
func (f fakeSnapshot) Membership(id string) (Membership, bool) {
	m, ok := f.memberships[id]
	return m, ok
}

func orgSnapshot(role string) fakeSnapshot {
	return fakeSnapshot{
		principal:   StatusActive,
		orgStatus:   map[string]Status{testOrg: StatusActive},
		memberships: map[string]Membership{testOrg: {OrgID: testOrg, Role: role, Status: StatusActive}},
	}
}

func platformSnapshot(role string) fakeSnapshot {
	return fakeSnapshot{principal: StatusActive, platformRole: role}
}

func orgRequest(action string) Request {
	return Request{Action: action, ResourceKind: "organization", OrganizationID: testOrg}
}

func platformRequest(action string) Request {
	return Request{Action: action, ResourceKind: "platform"}
}

type rawAction struct {
	Action       string `json:"action"`
	Scope        string `json:"scope"`
	ResourceKind string `json:"resource_kind"`
}

type rawRole struct {
	Role    string   `json:"role"`
	Scope   string   `json:"scope"`
	Actions []string `json:"actions"`
}

// vocab reads the embedded vocabulary files directly, never through the
// policycatalog code that the evaluator under test also uses.
func vocab(t *testing.T) ([]rawAction, []rawRole, int) {
	t.Helper()
	var af struct {
		CatalogVersion int         `json:"catalog_version"`
		Actions        []rawAction `json:"actions"`
	}
	var rf struct {
		Roles []rawRole `json:"roles"`
	}
	if err := json.Unmarshal(vocabulary.ActionsV1, &af); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(vocabulary.RolesV1, &rf); err != nil {
		t.Fatal(err)
	}
	if len(af.Actions) == 0 || len(rf.Roles) == 0 {
		t.Fatal("empty vocabulary")
	}
	return af.Actions, rf.Roles, af.CatalogVersion
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestEvaluate_EveryCatalogActionByEveryRole(t *testing.T) {
	actions, roles, _ := vocab(t)
	e := NewEvaluator()
	cells, allowed := 0, 0
	for _, r := range roles {
		for _, a := range actions {
			var req Request
			var snap fakeSnapshot
			switch a.ResourceKind {
			case "organization":
				req = orgRequest(a.Action)
			case "platform":
				req = platformRequest(a.Action)
			case "repository":
				req = Request{Action: a.Action, ResourceKind: "repository", OrganizationID: testOrg}
			default:
				t.Fatalf("action %s has resource kind %q", a.Action, a.ResourceKind)
			}
			switch r.Scope {
			case "organization":
				snap = orgSnapshot(r.Role)
			case "platform":
				snap = platformSnapshot(r.Role)
			default:
				t.Fatalf("role %s has scope %q", r.Role, r.Scope)
			}
			want := r.Scope == a.Scope && contains(r.Actions, a.Action)
			got := e.Evaluate(req, snap)
			cells++
			if got.Allowed != want {
				t.Errorf("role %s action %s: allowed=%v want %v (%s)", r.Role, a.Action, got.Allowed, want, got.Reason)
			}
			if got.Allowed {
				allowed++
				if got.Reason != ReasonAllowedByRole {
					t.Errorf("role %s action %s: allow reason %s", r.Role, a.Action, got.Reason)
				}
			}
		}
	}
	if cells != len(actions)*len(roles) {
		t.Fatalf("cells %d", cells)
	}
	total := 0
	for _, r := range roles {
		total += len(r.Actions)
	}
	if allowed != total {
		t.Fatalf("allowed cells %d, role grants %d", allowed, total)
	}
}

func TestEvaluate_PlatformRoleNeverSatisfiesOrgAction(t *testing.T) {
	actions, roles, _ := vocab(t)
	e := NewEvaluator()
	for _, r := range roles {
		if r.Scope != "platform" {
			continue
		}
		for _, a := range actions {
			if a.Scope != "organization" || a.ResourceKind != "organization" {
				continue
			}
			s := orgSnapshot("")
			s.memberships = map[string]Membership{}
			s.platformRole = r.Role
			if got := e.Evaluate(orgRequest(a.Action), s); got.Allowed || got.Reason != ReasonNoMembership {
				t.Errorf("%s on %s without membership: %+v", r.Role, a.Action, got)
			}
			s = orgSnapshot(r.Role)
			s.platformRole = r.Role
			if got := e.Evaluate(orgRequest(a.Action), s); got.Allowed || got.Reason != ReasonRoleScopeMismatch {
				t.Errorf("%s as membership role on %s: %+v", r.Role, a.Action, got)
			}
		}
	}
}

func TestEvaluate_OrgRoleNeverSatisfiesPlatformAction(t *testing.T) {
	actions, roles, _ := vocab(t)
	e := NewEvaluator()
	for _, r := range roles {
		if r.Scope != "organization" {
			continue
		}
		for _, a := range actions {
			if a.Scope != "platform" {
				continue
			}
			s := orgSnapshot(r.Role)
			s.platformRole = r.Role
			if got := e.Evaluate(platformRequest(a.Action), s); got.Allowed || got.Reason != ReasonRoleScopeMismatch {
				t.Errorf("%s on %s: %+v", r.Role, a.Action, got)
			}
			s = orgSnapshot(r.Role)
			if got := e.Evaluate(platformRequest(a.Action), s); got.Allowed || got.Reason != ReasonPlatformRoleAbsent {
				t.Errorf("%s without platform role on %s: %+v", r.Role, a.Action, got)
			}
		}
	}
}

func TestEvaluate_UnknownActionDenies(t *testing.T) {
	actions, _, _ := vocab(t)
	known := map[string]bool{}
	for _, a := range actions {
		known[a.Action] = true
	}
	e := NewEvaluator()
	for _, a := range []string{"", "admin", "*", "org.*", "ORG.READ", "org.read ", "org.read\n", "org.fly", "billing.fly", "acr.context.write", "superuser.all"} {
		if known[a] {
			t.Fatalf("%q is in the catalog; pick another unknown", a)
		}
		if got := e.Evaluate(orgRequest(a), orgSnapshot("owner")); got.Allowed {
			t.Errorf("action %q allowed", a)
		}
	}
	if got := e.Evaluate(orgRequest("org.fly"), orgSnapshot("owner")); got.Reason != ReasonUnknownAction {
		t.Errorf("reason %s", got.Reason)
	}
	if got := e.Evaluate(platformRequest("admin.fly"), platformSnapshot("platform_admin")); got.Allowed || got.Reason != ReasonUnknownAction {
		t.Errorf("platform unknown: %+v", got)
	}
}

func TestEvaluate_UnknownRoleDenies(t *testing.T) {
	e := NewEvaluator()
	for _, r := range []string{"", "superuser", "root", "Owner", "owner ", "platform_admin", "guest"} {
		if got := e.Evaluate(orgRequest("org.read"), orgSnapshot(r)); got.Allowed {
			t.Errorf("membership role %q allowed", r)
		}
	}
	if got := e.Evaluate(orgRequest("org.read"), orgSnapshot("guest")); got.Reason != ReasonRoleUnknown {
		t.Errorf("reason %s", got.Reason)
	}
	for _, r := range []string{"superuser", "root", "admin", "PLATFORM_ADMIN"} {
		if got := e.Evaluate(platformRequest("admin.users"), platformSnapshot(r)); got.Allowed {
			t.Errorf("platform role %q allowed", r)
		}
	}
	if got := e.Evaluate(platformRequest("admin.users"), platformSnapshot("superuser")); got.Reason != ReasonRoleUnknown {
		t.Errorf("platform reason %s", got.Reason)
	}
}

func TestEvaluate_EveryNonActiveStatusDenies(t *testing.T) {
	e := NewEvaluator()
	for _, v := range []Status{StatusUnknown, StatusInvited, StatusSuspended, StatusRemoved, 7, 255} {
		s := orgSnapshot("owner")
		s.principal = v
		if got := e.Evaluate(orgRequest("org.read"), s); got.Allowed || got.Reason != ReasonPrincipalInactive {
			t.Errorf("principal %d: %+v", v, got)
		}
		p := platformSnapshot("platform_admin")
		p.principal = v
		if got := e.Evaluate(platformRequest("admin.users"), p); got.Allowed || got.Reason != ReasonPrincipalInactive {
			t.Errorf("platform principal %d: %+v", v, got)
		}
		s = orgSnapshot("owner")
		s.orgStatus[testOrg] = v
		if got := e.Evaluate(orgRequest("org.read"), s); got.Allowed || got.Reason != ReasonOrganizationInactive {
			t.Errorf("org %d: %+v", v, got)
		}
		s = orgSnapshot("owner")
		s.memberships[testOrg] = Membership{OrgID: testOrg, Role: "owner", Status: v}
		if got := e.Evaluate(orgRequest("org.read"), s); got.Allowed || got.Reason != ReasonMembershipInactive {
			t.Errorf("membership %d: %+v", v, got)
		}
	}
}

func TestParseStatus(t *testing.T) {
	want := map[string]Status{
		"active": StatusActive, "invited": StatusInvited, "suspended": StatusSuspended, "removed": StatusRemoved,
	}
	for in, w := range want {
		if got := ParseStatus(in); got != w {
			t.Errorf("%q -> %d want %d", in, got, w)
		}
	}
	for _, in := range []string{"", "disabled", "ACTIVE", "Active", "active ", " active", "active\n", "x", "removed\x00"} {
		if got := ParseStatus(in); got != StatusUnknown {
			t.Errorf("%q -> %d want unknown", in, got)
		}
	}
	if StatusUnknown != 0 {
		t.Fatal("the zero Status must be unknown")
	}
}

func TestStatusNamesMatchStoredStatuses(t *testing.T) {
	pairs := []struct {
		name string
		want Status
	}{
		{string(membership.StatusInvited), StatusInvited},
		{string(membership.StatusActive), StatusActive},
		{string(membership.StatusSuspended), StatusSuspended},
		{string(membership.StatusRemoved), StatusRemoved},
		{string(org.StatusActive), StatusActive},
		{string(org.StatusSuspended), StatusSuspended},
	}
	for _, p := range pairs {
		if got := ParseStatus(p.name); got != p.want {
			t.Errorf("stored status %q parses to %d want %d", p.name, got, p.want)
		}
	}
}

func TestEvaluate_CrossOrgMembershipDenies(t *testing.T) {
	e := NewEvaluator()
	s := orgSnapshot("owner")
	s.orgStatus["org-2"] = StatusActive
	r2 := Request{Action: "org.read", ResourceKind: "organization", OrganizationID: "org-2"}
	if got := e.Evaluate(r2, s); got.Allowed || got.Reason != ReasonNoMembership {
		t.Errorf("no membership in org-2: %+v", got)
	}
	s.memberships["org-2"] = Membership{OrgID: testOrg, Role: "owner", Status: StatusActive}
	if got := e.Evaluate(r2, s); got.Allowed || got.Reason != ReasonOrgMismatch {
		t.Errorf("membership row for another org: %+v", got)
	}
}

func TestEvaluate_ResourceKindMismatchDenies(t *testing.T) {
	e := NewEvaluator()
	if got := e.Evaluate(Request{Action: "org.read", ResourceKind: "platform"}, orgSnapshot("owner")); got.Allowed || got.Reason != ReasonResourceMismatch {
		t.Errorf("org action on platform kind: %+v", got)
	}
	if got := e.Evaluate(Request{Action: "admin.users", ResourceKind: "organization", OrganizationID: testOrg}, platformSnapshot("platform_admin")); got.Allowed || got.Reason != ReasonResourceMismatch {
		t.Errorf("platform action on organization kind: %+v", got)
	}
	for _, k := range []string{"repository", "", "Organization", "organization ", "*"} {
		if got := e.Evaluate(Request{Action: "org.read", ResourceKind: k, OrganizationID: testOrg}, orgSnapshot("owner")); got.Allowed || got.Reason != ReasonInvalidRequest {
			t.Errorf("resource kind %q: %+v", k, got)
		}
	}
}

func TestEvaluate_ZeroValuesDeny(t *testing.T) {
	if (Decision{}).Allowed {
		t.Fatal("zero Decision allows")
	}
	e := NewEvaluator()
	if got := e.Evaluate(Request{}, fakeSnapshot{}); got.Allowed {
		t.Fatal("zero request allowed")
	}
	var nilEval *Evaluator
	if got := nilEval.Evaluate(orgRequest("org.read"), orgSnapshot("owner")); got.Allowed {
		t.Fatal("nil evaluator allowed")
	}
	if got := (&Evaluator{}).Evaluate(orgRequest("org.read"), orgSnapshot("owner")); got.Allowed {
		t.Fatal("zero evaluator allowed")
	}
	if got := e.Evaluate(orgRequest("org.read"), nil); got.Allowed {
		t.Fatal("nil snapshot allowed")
	}
	if got := newEvaluator(nil).Evaluate(orgRequest("org.read"), orgSnapshot("owner")); got.Allowed {
		t.Fatal("nil catalog allowed")
	}
	if got := e.Evaluate(orgRequest("org.read"), fakeSnapshot{}); got.Allowed || got.Reason != ReasonPrincipalInactive {
		t.Fatalf("zero snapshot: %+v", got)
	}
}

func TestNormalizeRequest_RefusesNonCanonical(t *testing.T) {
	long := string(bytes.Repeat([]byte("a"), 200))
	bad := []Request{
		{Action: "ORG.READ", ResourceKind: "organization", OrganizationID: testOrg},
		{Action: " org.read", ResourceKind: "organization", OrganizationID: testOrg},
		{Action: "org.read\n", ResourceKind: "organization", OrganizationID: testOrg},
		{Action: "org.read\x00", ResourceKind: "organization", OrganizationID: testOrg},
		{Action: long + ".read", ResourceKind: "organization", OrganizationID: testOrg},
		{Action: "", ResourceKind: "organization", OrganizationID: testOrg},
		{Action: "org", ResourceKind: "organization", OrganizationID: testOrg},
		{Action: "org.read", ResourceKind: "organization", OrganizationID: ""},
		{Action: "org.read", ResourceKind: "organization", OrganizationID: "Org-1"},
		{Action: "org.read", ResourceKind: "organization", OrganizationID: " org-1"},
		{Action: "org.read", ResourceKind: "organization", OrganizationID: "org-1\n"},
		{Action: "org.read", ResourceKind: "organization", OrganizationID: "org\x001"},
		{Action: "org.read", ResourceKind: "organization", OrganizationID: long},
		{Action: "admin.users", ResourceKind: "platform", OrganizationID: testOrg},
		{Action: "admin.users", ResourceKind: "Platform"},
		{Action: "admin.users", ResourceKind: ""},
	}
	e := NewEvaluator()
	for _, r := range bad {
		out, ok := NormalizeRequest(r)
		if ok || out != (Request{}) {
			t.Errorf("accepted %+v", r)
		}
		s := orgSnapshot("owner")
		s.orgStatus[r.OrganizationID] = StatusActive
		s.memberships[r.OrganizationID] = Membership{OrgID: r.OrganizationID, Role: "owner", Status: StatusActive}
		s.platformRole = "platform_admin"
		if got := e.Evaluate(r, s); got.Allowed || got.Reason != ReasonInvalidRequest {
			t.Errorf("%+v: %+v", r, got)
		}
	}
	for _, r := range []Request{orgRequest("org.read"), platformRequest("admin.users")} {
		out, ok := NormalizeRequest(r)
		if !ok || out != r {
			t.Errorf("canonical request %+v refused or changed: %+v", r, out)
		}
	}
}

func TestEvaluate_PolicyRevisionFollowsCatalogVersion(t *testing.T) {
	_, _, ver := vocab(t)
	cur := NewEvaluator().Evaluate(orgRequest("org.read"), orgSnapshot("owner"))
	if cur.PolicyRevision != ver || ver < 1 {
		t.Fatalf("revision %d catalog %d", cur.PolicyRevision, ver)
	}
	next := ver + 1
	bump := func(b []byte) []byte {
		old := []byte(`"catalog_version": ` + strconv.Itoa(ver))
		if !bytes.Contains(b, old) {
			t.Fatalf("catalog_version member not found")
		}
		return bytes.Replace(b, old, []byte(`"catalog_version": `+strconv.Itoa(next)), 1)
	}
	c, err := policycatalog.Load(bump(vocabulary.ActionsV1), bump(vocabulary.RolesV1))
	if err != nil {
		t.Fatal(err)
	}
	for _, dec := range []Decision{
		newEvaluator(c).Evaluate(orgRequest("org.read"), orgSnapshot("owner")),
		newEvaluator(c).Evaluate(orgRequest("org.fly"), orgSnapshot("owner")),
		newEvaluator(c).Evaluate(orgRequest("org.delete"), orgSnapshot("viewer")),
	} {
		if dec.PolicyRevision != next {
			t.Errorf("revision %d want %d (%+v)", dec.PolicyRevision, next, dec)
		}
	}
}

func TestReasonSetIsClosed(t *testing.T) {
	want := []Reason{
		"allowed_by_role", "invalid_request", "unknown_action", "resource_mismatch",
		"principal_inactive", "organization_inactive", "no_membership", "org_mismatch",
		"membership_inactive", "platform_role_absent", "role_unknown", "role_scope_mismatch",
		"role_lacks_action", "allowed_by_repository_grant", "grant_absent", "repository_not_granted",
		"grant_expired", "grant_revoked", "workload_audience_denied",
		"workload_action_denied", "clock_unavailable", "service_account_inactive",
		"allowed_by_membership_default", "grant_read_failed", "impersonation_denied", "delegation_invalid",
	}
	got := AllReasons()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("reasons %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reasons %v want %v", got, want)
		}
	}
	allowed := map[Reason]bool{}
	for _, r := range got {
		allowed[r] = true
	}
	actions, _, _ := vocab(t)
	e := NewEvaluator()
	for _, a := range actions {
		for _, role := range []string{"viewer", "owner", "guest", "platform_admin", ""} {
			for _, req := range []Request{orgRequest(a.Action), platformRequest(a.Action), orgRequest("org.fly"), {}} {
				for _, s := range []fakeSnapshot{orgSnapshot(role), platformSnapshot(role), {}} {
					if r := e.Evaluate(req, s).Reason; !allowed[r] {
						t.Fatalf("reason %q outside the closed set", r)
					}
				}
			}
		}
	}
}

func TestPackageImportsNothingWithIO(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatal("no files")
	}
	banned := map[string]bool{"net/http": true, "database/sql": true, "os": true, "net": true, "io/ioutil": true, "time": true}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range af.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			if banned[p] || strings.Contains(p, "internal/auth/membership") || strings.Contains(p, "internal/auth/org") || strings.Contains(p, "authschema") {
				t.Errorf("%s imports %s", f, p)
			}
		}
	}
}

func TestExportedAPIAcceptsNoCatalog(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatal("no files")
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(token.NewFileSet(), f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range af.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() || fn.Type.Params == nil {
				continue
			}
			for _, p := range fn.Type.Params.List {
				ast.Inspect(p.Type, func(n ast.Node) bool {
					if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "Catalog" {
						t.Errorf("%s: exported func %s takes a catalog", f, fn.Name.Name)
					}
					return true
				})
			}
		}
	}
}

func TestEvaluate_EveryDenyPathCarriesTheCatalogVersion(t *testing.T) {
	_, _, ver := vocab(t)
	if ver < 1 {
		t.Fatal("catalog version")
	}
	inactive := func(mut func(*fakeSnapshot)) Snapshot {
		s := orgSnapshot("owner")
		mut(&s)
		return s
	}
	cases := []struct {
		name   string
		req    Request
		snap   Snapshot
		reason Reason
		eval   *Evaluator // nil: the embedded catalog
	}{
		{"nil snapshot", orgRequest("org.read"), nil, ReasonInvalidRequest, nil},
		{"invalid request", orgRequest("ORG.READ"), orgSnapshot("owner"), ReasonInvalidRequest, nil},
		{"unknown action", orgRequest("org.fly"), orgSnapshot("owner"), ReasonUnknownAction, nil},
		{"resource mismatch", Request{Action: "org.read", ResourceKind: "platform"}, orgSnapshot("owner"), ReasonResourceMismatch, nil},
		{"principal inactive", orgRequest("org.read"), inactive(func(s *fakeSnapshot) { s.principal = StatusSuspended }), ReasonPrincipalInactive, nil},
		{"organization inactive", orgRequest("org.read"), inactive(func(s *fakeSnapshot) { s.orgStatus[testOrg] = StatusSuspended }), ReasonOrganizationInactive, nil},
		{"membership inactive", orgRequest("org.read"), inactive(func(s *fakeSnapshot) {
			s.memberships[testOrg] = Membership{OrgID: testOrg, Role: "owner", Status: StatusRemoved}
		}), ReasonMembershipInactive, nil},
		{"no membership", orgRequest("org.read"), inactive(func(s *fakeSnapshot) { s.memberships = map[string]Membership{} }), ReasonNoMembership, nil},
		{"org mismatch", orgRequest("org.read"), inactive(func(s *fakeSnapshot) {
			s.memberships[testOrg] = Membership{OrgID: "org-2", Role: "owner", Status: StatusActive}
		}), ReasonOrgMismatch, nil},
		{"platform role absent", platformRequest("admin.users"), orgSnapshot("owner"), ReasonPlatformRoleAbsent, nil},
		{"role unknown", orgRequest("org.read"), orgSnapshot("guest"), ReasonRoleUnknown, nil},
		{"role scope mismatch", orgRequest("org.read"), orgSnapshot("platform_admin"), ReasonRoleScopeMismatch, nil},
		{"role lacks action", orgRequest("org.delete"), orgSnapshot("viewer"), ReasonRoleLacksAction, nil},
		// Repository reasons: planted catalog (the real one has no role holding a repository action yet).
		{"grant absent", repoRequest("acme/api"), saWithNoRows(), ReasonGrantAbsent, plantedEvaluator()},
		{"repository not granted", repoRequest("acme/api"), repoSnapshot("member", liveGrant("acme/web")), ReasonRepositoryNotGranted, plantedEvaluator()},
		{"grant expired", repoRequest("acme/api"), repoSnapshot("member", expiredGrant("acme/api")), ReasonGrantExpired, plantedEvaluator()},
		{"grant revoked", repoRequest("acme/api"), repoSnapshot("member", revokedGrant("acme/api")), ReasonGrantRevoked, plantedEvaluator()},
		{"workload audience", repoRequest("acme/api"), withWorkload(repoSnapshot("member", liveGrant("acme/api")), "x", []string{"y"}, []string{repoAction}), ReasonWorkloadAudienceDenied, plantedEvaluator()},
		{"workload action", repoRequest("acme/api"), withWorkload(repoSnapshot("member", liveGrant("acme/api")), "y", []string{"y"}, []string{"org.read"}), ReasonWorkloadActionDenied, plantedEvaluator()},
		{"clock unavailable", repoRequest("acme/api"), noClock(repoSnapshot("member", liveGrant("acme/api"))), ReasonClockUnavailable, plantedEvaluator()},
		{"service account inactive", repoRequest("acme/api"), inactiveSA(), ReasonServiceAccountInactive, plantedEvaluator()},
		{"grant read failed", repoRequest("acme/api"), readFails(repoSnapshot("member")), ReasonGrantReadFailed, plantedEvaluator()},
		{"impersonation denied", orgRequest("org.delete"), delegated("owner", []string{"org.delete"}, ""), ReasonImpersonationDenied, nil},
		{"delegation invalid", orgRequest("org.read"), delegated("owner", nil, "").withChain(nil), ReasonDelegationInvalid, nil},
	}
	seen := map[Reason]bool{}
	for _, c := range cases {
		e := c.eval
		if e == nil {
			e = NewEvaluator()
		}
		got := e.Evaluate(c.req, c.snap)
		if got.Allowed || got.Reason != c.reason {
			t.Errorf("%s: %+v want deny %s", c.name, got, c.reason)
		}
		if got.PolicyRevision != ver {
			t.Errorf("%s: revision %d want %d", c.name, got.PolicyRevision, ver)
		}
		seen[c.reason] = true
	}
	for _, r := range AllReasons() {
		allow := r == ReasonAllowedByRole || r == ReasonAllowedByRepositoryGrant || r == ReasonAllowedByMembershipDefault
		if !allow && !seen[r] {
			t.Errorf("deny reason %s has no revision case", r)
		}
	}
	var nilEval *Evaluator
	for name, got := range map[string]Decision{
		"nil evaluator": nilEval.Evaluate(orgRequest("org.read"), orgSnapshot("owner")),
		"nil catalog":   newEvaluator(nil).Evaluate(orgRequest("org.read"), orgSnapshot("owner")),
		"nil both":      newEvaluator(nil).Evaluate(orgRequest("org.read"), nil),
	} {
		if got.Allowed || got.PolicyRevision != 0 {
			t.Errorf("%s: %+v", name, got)
		}
	}
}

// 10774 put a repository-kind action in the catalog before any role held it;
// 5.3 serves repository requests. Each one follows the grant and membership
// default rules, so a role's outcome is exactly whether the vocabulary files
// say it holds the action (read here from the JSON, not through Allows).
func TestEvaluate_RepositoryKindActionFollowsGrantAndDefaultRules(t *testing.T) {
	actions, roles, _ := vocab(t)
	holds := func(role, action string) bool {
		for _, r := range roles {
			if r.Role == role {
				for _, a := range r.Actions {
					if a == action {
						return true
					}
				}
			}
		}
		return false
	}
	e := NewEvaluator()
	seen := 0
	for _, a := range actions {
		if a.ResourceKind != "repository" {
			continue
		}
		seen++
		req := Request{Action: a.Action, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: "acme/api"}
		for _, r := range roles {
			if r.Scope == "platform" {
				if got := e.Evaluate(req, platformSnapshot(r.Role)); got.Allowed || got.Reason != ReasonGrantAbsent {
					t.Errorf("platform role %s on %s: %+v", r.Role, a.Action, got)
				}
				continue
			}
			wantDefault := holds(r.Role, a.Action) || (r.Role == "owner" && holds("admin", a.Action))
			got := e.Evaluate(req, repoSnapshot(r.Role))
			if got.Allowed != wantDefault || (got.Allowed && got.Reason != ReasonAllowedByMembershipDefault) {
				t.Errorf("membership role %s, no rows, %s: %+v want allowed=%v", r.Role, a.Action, got, wantDefault)
			}
			g := Grant{OrgID: testOrg, Role: r.Role, ResourceKind: "repository", ResourceKey: "acme/api"}
			got = e.Evaluate(req, repoSnapshot("viewer", g))
			if got.Allowed != holds(r.Role, a.Action) || (got.Allowed && got.Reason != ReasonAllowedByRepositoryGrant) {
				t.Errorf("grant role %s, %s: %+v want allowed=%v", r.Role, a.Action, got, holds(r.Role, a.Action))
			}
			other := Grant{OrgID: testOrg, Role: r.Role, ResourceKind: "repository", ResourceKey: "acme/web"}
			if got := e.Evaluate(req, repoSnapshot(r.Role, other)); got.Allowed {
				t.Errorf("membership role %s beside another repository's row, %s: %+v", r.Role, a.Action, got)
			}
		}
	}
	if seen == 0 {
		t.Error("the catalog holds no repository-kind action")
	}
}
