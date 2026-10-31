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
			if a.Scope != "organization" {
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
	for _, a := range []string{"", "admin", "*", "org.*", "ORG.READ", "org.read ", "org.read\n", "org.fly", "billing.fly", "acr.context.read", "superuser.all"} {
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
	pairs := map[string]Status{
		string(membership.StatusInvited):   StatusInvited,
		string(membership.StatusActive):    StatusActive,
		string(membership.StatusSuspended): StatusSuspended,
		string(membership.StatusRemoved):   StatusRemoved,
		string(org.StatusActive):           StatusActive,
		string(org.StatusSuspended):        StatusSuspended,
	}
	for name, st := range pairs {
		if got := ParseStatus(name); got != st {
			t.Errorf("stored status %q parses to %d want %d", name, got, st)
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
		"role_lacks_action",
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
