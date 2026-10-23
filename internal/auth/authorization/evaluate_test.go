package authorization

import (
	"bytes"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
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

func orgRequest(action string) Request {
	return Request{Action: action, ResourceType: "organization", OrganizationID: testOrg}
}

var orgActions = []string{
	"analytics.export", "analytics.read", "git.read", "git.sync", "integrations.read",
	"integrations.write", "members.invite", "members.manage", "members.read",
	"metrics.compute", "metrics.read", "org.delete", "org.read", "org.write",
	"settings.read", "settings.write", "teams.read", "teams.write",
	"work_items.read", "work_items.sync",
}

var platformActions = []string{"admin.orgs", "admin.users"}

// Hand-listed from roles.v1.json; never derived from the catalog code.
var wantRoleActions = map[string][]string{
	"viewer": {"analytics.read", "git.read", "members.read", "metrics.read", "org.read", "settings.read", "teams.read", "work_items.read"},
	"member": {"analytics.export", "analytics.read", "git.read", "members.read", "metrics.read", "org.read", "settings.read", "teams.read", "work_items.read"},
	"admin": {"analytics.export", "analytics.read", "git.read", "git.sync", "integrations.read", "integrations.write", "members.invite", "members.manage",
		"members.read", "metrics.compute", "metrics.read", "org.read", "org.write", "settings.read", "settings.write", "teams.read", "teams.write", "work_items.read", "work_items.sync"},
	"owner": {"analytics.export", "analytics.read", "git.read", "git.sync", "integrations.read", "integrations.write", "members.invite", "members.manage",
		"members.read", "metrics.compute", "metrics.read", "org.delete", "org.read", "org.write", "settings.read", "settings.write", "teams.read", "teams.write", "work_items.read", "work_items.sync"},
}

func has(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func newEval() *Evaluator { return NewEvaluator(policycatalog.Default()) }

func TestEvaluate_EveryCatalogActionByEveryRole(t *testing.T) {
	e := newEval()
	if len(orgActions)+len(platformActions) != len(policycatalog.Default().Actions()) {
		t.Fatalf("test action list drifted from the catalog")
	}
	for role, held := range wantRoleActions {
		for _, a := range orgActions {
			got := e.Evaluate(orgRequest(a), orgSnapshot(role))
			if want := has(held, a); got.Allowed != want {
				t.Errorf("role %s action %s: allowed=%v want %v (%s)", role, a, got.Allowed, want, got.Reason)
			}
		}
		for _, a := range platformActions {
			got := e.Evaluate(Request{Action: a, ResourceType: "platform"}, orgSnapshot(role))
			if got.Allowed {
				t.Errorf("org role %s must not hold platform action %s", role, a)
			}
		}
	}
	for _, a := range platformActions {
		s := fakeSnapshot{principal: StatusActive, platformRole: "platform_admin"}
		if got := e.Evaluate(Request{Action: a, ResourceType: "platform"}, s); !got.Allowed || got.Reason != ReasonAllowedByRole {
			t.Errorf("platform_admin %s: %+v", a, got)
		}
	}
}

func TestEvaluate_PlatformRoleNeverGrantsOrgAction(t *testing.T) {
	e := newEval()
	s := fakeSnapshot{principal: StatusActive, platformRole: "platform_admin", orgStatus: map[string]Status{testOrg: StatusActive}}
	for _, a := range orgActions {
		if got := e.Evaluate(orgRequest(a), s); got.Allowed {
			t.Errorf("platform_admin allowed org action %s", a)
		}
	}
}

func TestEvaluate_OrgRoleNeverGrantsPlatformAction(t *testing.T) {
	e := newEval()
	s := orgSnapshot("owner")
	s.platformRole = "owner"
	for _, a := range platformActions {
		got := e.Evaluate(Request{Action: a, ResourceType: "platform"}, s)
		if got.Allowed || got.Reason != ReasonRoleScopeMismatch {
			t.Errorf("%s: %+v", a, got)
		}
	}
}

func TestEvaluate_UnknownActionDenies(t *testing.T) {
	e := newEval()
	for _, a := range []string{"", "admin", "*", "org.*", "ORG.READ", "org.read ", "org.read\n", "org.fly", "billing.read", "acr.context.read"} {
		got := e.Evaluate(orgRequest(a), orgSnapshot("owner"))
		if got.Allowed {
			t.Errorf("action %q allowed", a)
		}
	}
	if got := e.Evaluate(orgRequest("org.fly"), orgSnapshot("owner")); got.Reason != ReasonUnknownAction {
		t.Errorf("reason %s", got.Reason)
	}
}

func TestEvaluate_UnknownRoleDenies(t *testing.T) {
	e := newEval()
	for _, r := range []string{"", "superuser", "root", "Owner", "owner ", "platform_admin", "guest"} {
		if got := e.Evaluate(orgRequest("org.read"), orgSnapshot(r)); got.Allowed {
			t.Errorf("membership role %q allowed", r)
		}
	}
	if got := e.Evaluate(orgRequest("org.read"), orgSnapshot("guest")); got.Reason != ReasonRoleUnknown {
		t.Errorf("reason %s", got.Reason)
	}
	s := fakeSnapshot{principal: StatusActive, platformRole: "superuser"}
	if got := e.Evaluate(Request{Action: "admin.users", ResourceType: "platform"}, s); got.Allowed || got.Reason != ReasonRoleUnknown {
		t.Errorf("platform: %+v", got)
	}
}

func TestEvaluate_UnknownStatusDenies(t *testing.T) {
	e := newEval()
	for _, v := range []Status{0, 7, 255} {
		if v == StatusActive {
			continue
		}
		s := orgSnapshot("owner")
		s.principal = v
		if got := e.Evaluate(orgRequest("org.read"), s); got.Allowed || got.Reason != ReasonPrincipalInactive {
			t.Errorf("principal %d: %+v", v, got)
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

func TestEvaluate_StatusFromDatabaseValue(t *testing.T) {
	if StatusFromString("active") != StatusActive {
		t.Fatal("active")
	}
	for _, v := range []string{"", "invited", "suspended", "removed", "disabled", "ACTIVE", "active ", "x"} {
		if StatusFromString(v) != StatusNotActive {
			t.Errorf("%q must map to NotActive", v)
		}
	}
}

func TestEvaluate_CrossOrgMembershipDenies(t *testing.T) {
	e := newEval()
	s := orgSnapshot("owner")
	s.orgStatus["org-2"] = StatusActive
	if got := e.Evaluate(Request{Action: "org.read", ResourceType: "organization", OrganizationID: "org-2"}, s); got.Allowed || got.Reason != ReasonNoMembership {
		t.Errorf("no membership in org-2: %+v", got)
	}
	s.memberships["org-2"] = Membership{OrgID: testOrg, Role: "owner", Status: StatusActive}
	if got := e.Evaluate(Request{Action: "org.read", ResourceType: "organization", OrganizationID: "org-2"}, s); got.Allowed || got.Reason != ReasonOrgMismatch {
		t.Errorf("membership row for another org: %+v", got)
	}
}

func TestEvaluate_InactivePrincipalOrOrgDenies(t *testing.T) {
	e := newEval()
	s := orgSnapshot("owner")
	s.principal = StatusNotActive
	if got := e.Evaluate(orgRequest("org.read"), s); got.Allowed {
		t.Error("inactive principal allowed")
	}
	s = fakeSnapshot{principal: StatusNotActive, platformRole: "platform_admin"}
	if got := e.Evaluate(Request{Action: "admin.users", ResourceType: "platform"}, s); got.Allowed {
		t.Error("inactive platform principal allowed")
	}
	s = orgSnapshot("owner")
	s.orgStatus[testOrg] = StatusNotActive
	if got := e.Evaluate(orgRequest("org.read"), s); got.Allowed {
		t.Error("inactive org allowed")
	}
}

func TestEvaluate_ResourceMismatchDenies(t *testing.T) {
	e := newEval()
	if got := e.Evaluate(Request{Action: "org.read", ResourceType: "platform"}, orgSnapshot("owner")); got.Allowed {
		t.Error("org action on platform resource")
	}
	if got := e.Evaluate(Request{Action: "admin.users", ResourceType: "organization", OrganizationID: testOrg}, fakeSnapshot{principal: StatusActive, platformRole: "platform_admin"}); got.Allowed {
		t.Error("platform action on organization resource")
	}
	if got := e.Evaluate(Request{Action: "org.read", ResourceType: "repository", OrganizationID: testOrg}, orgSnapshot("owner")); got.Allowed {
		t.Error("unknown resource type")
	}
}

func TestEvaluate_ZeroValueDenies(t *testing.T) {
	if got := (Decision{}); got.Allowed {
		t.Fatal("zero Decision allows")
	}
	e := newEval()
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
	if got := NewEvaluator(nil).Evaluate(orgRequest("org.read"), orgSnapshot("owner")); got.Allowed {
		t.Fatal("nil catalog allowed")
	}
}

func TestEvaluate_NonCanonicalRequestDenies(t *testing.T) {
	e := newEval()
	for _, org := range []string{"", "Org-1", " org-1", "org-1\n", "org\x001", string(bytes.Repeat([]byte("a"), 200))} {
		s := orgSnapshot("owner")
		s.orgStatus[org] = StatusActive
		s.memberships[org] = Membership{OrgID: org, Role: "owner", Status: StatusActive}
		got := e.Evaluate(Request{Action: "org.read", ResourceType: "organization", OrganizationID: org}, s)
		if got.Allowed || got.Reason != ReasonInvalidRequest {
			t.Errorf("org %q: %+v", org, got)
		}
	}
	if got := e.Evaluate(Request{Action: "admin.users", ResourceType: "platform", OrganizationID: testOrg}, fakeSnapshot{principal: StatusActive, platformRole: "platform_admin"}); got.Allowed {
		t.Error("platform request with an organization id allowed")
	}
}

func TestEvaluate_PolicyRevisionFollowsCatalogVersion(t *testing.T) {
	cur := newEval().Evaluate(orgRequest("org.read"), orgSnapshot("owner"))
	if cur.PolicyRevision != policycatalog.Default().Version() || cur.PolicyRevision < 1 {
		t.Fatalf("revision %d", cur.PolicyRevision)
	}
	next := policycatalog.Default().Version() + 1
	bump := func(b []byte) []byte {
		old := []byte(`"catalog_version": ` + strconv.Itoa(policycatalog.Default().Version()))
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
		NewEvaluator(c).Evaluate(orgRequest("org.read"), orgSnapshot("owner")),
		NewEvaluator(c).Evaluate(orgRequest("org.fly"), orgSnapshot("owner")),
		NewEvaluator(c).Evaluate(orgRequest("org.delete"), orgSnapshot("viewer")),
	} {
		if dec.PolicyRevision != next {
			t.Errorf("revision %d want %d (%+v)", dec.PolicyRevision, next, dec)
		}
	}
}

func TestEvaluate_ReasonIsClosed(t *testing.T) {
	e := newEval()
	allowed := map[Reason]bool{}
	for _, r := range AllReasons() {
		allowed[r] = true
	}
	for _, a := range append(append([]string{}, orgActions...), "org.fly", "") {
		for _, role := range []string{"viewer", "owner", "guest"} {
			if r := e.Evaluate(orgRequest(a), orgSnapshot(role)).Reason; !allowed[r] {
				t.Errorf("reason %q outside the closed set", r)
			}
		}
	}
}

func TestPackageImportsNoNetHTTP(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatal("no files")
	}
	banned := map[string]bool{"net/http": true, "database/sql": true, "os": true, "net": true}
	for _, f := range files {
		af, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range af.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			if banned[p] {
				t.Errorf("%s imports %s", f, p)
			}
		}
	}
}
