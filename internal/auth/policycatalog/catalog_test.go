package policycatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	actionsSHA = "054bcf5d50465548a367670b5a2659434bab78fd5cababb567dd558bb8b09ea2"
	rolesSHA   = "e49d2c4e17db8e38baacb26dc7c36b917cfc46582d51c2abf44b8b670d702e1b"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func TestCanonicalBytesArePinned(t *testing.T) {
	if got := sum(vocabulary.ActionsV1); got != actionsSHA {
		t.Errorf("actions.v1.json sha256 = %s; a change needs an intentional catalog_version decision and a new pin", got)
	}
	if got := sum(vocabulary.RolesV1); got != rolesSHA {
		t.Errorf("roles.v1.json sha256 = %s; a change needs an intentional catalog_version decision and a new pin", got)
	}
}

func TestUnknownActionAndRoleAreRefused(t *testing.T) {
	c := Default()
	for _, id := range []string{"", "nope.read", "analytics", "*", "analytics.*", "ANALYTICS.READ", "analytics.read ", "is_admin", "superuser", "all"} {
		if _, err := c.Action(id); !errors.Is(err, ErrUnknownAction) {
			t.Errorf("Action(%q) err = %v, want ErrUnknownAction", id, err)
		}
	}
	for _, id := range []string{"", "root", "superuser", "Owner", "owner ", "*"} {
		if _, err := c.Role(id); !errors.Is(err, ErrUnknownRole) {
			t.Errorf("Role(%q) err = %v, want ErrUnknownRole", id, err)
		}
	}
	if c.Allows("owner", "nope.read") || c.Allows("nope", "org.read") || c.Allows("", "") {
		t.Error("an unknown action or role must deny")
	}
	if !c.Allows("viewer", "org.read") || c.Allows("viewer", "org.delete") {
		t.Error("viewer must hold org.read and not org.delete")
	}
}

func TestNoRoleOrActionMeansAllActions(t *testing.T) {
	c := Default()
	all := c.Actions()
	for _, r := range c.Roles() {
		if len(r.Actions) >= len(all) {
			t.Errorf("role %s lists %d of %d actions: no role may hold every action", r.ID, len(r.Actions), len(all))
		}
		for _, a := range r.Actions {
			if strings.Contains(a, "*") {
				t.Errorf("role %s holds wildcard %q", r.ID, a)
			}
		}
	}
	for _, a := range all {
		switch a.ID {
		case "superuser", "is_admin", "all", "*", "admin.all":
			t.Errorf("action %q stands for all actions", a.ID)
		}
		if !strings.Contains(a.ID, ".") {
			t.Errorf("action %q is a single segment", a.ID)
		}
	}
	if _, err := c.Role("superuser"); !errors.Is(err, ErrUnknownRole) {
		t.Error("superuser is not a role")
	}
}

func TestScopesDoNotMix(t *testing.T) {
	c := Default()
	scope := map[string]string{}
	for _, a := range c.Actions() {
		scope[a.ID] = a.Scope
	}
	for _, r := range c.Roles() {
		for _, a := range r.Actions {
			if scope[a] != r.Scope {
				t.Errorf("role %s (%s) holds %s action %s", r.ID, r.Scope, scope[a], a)
			}
		}
	}
	if c.Allows("owner", "admin.users") || c.Allows("platform_admin", "org.delete") {
		t.Error("platform and organization authority must stay separate")
	}
}

// platform_owner is a marker role (CHAOS-12294): it holds NO action, so an
// owner-only principal passes no authorization check. To act, an owner also
// holds a platform_admin row.
func TestPlatformOwnerIsAMarkerHoldingNoAction(t *testing.T) {
	c := Default()
	owner, err := c.Role("platform_owner")
	if err != nil {
		t.Fatalf("platform_owner is not in the catalog: %v", err)
	}
	if owner.Scope != ScopePlatform {
		t.Errorf("platform_owner scope = %s, want platform", owner.Scope)
	}
	if len(owner.Actions) != 0 {
		t.Errorf("platform_owner actions = %v, want none", owner.Actions)
	}
	for _, a := range c.Actions() {
		if c.Allows("platform_owner", a.ID) {
			t.Errorf("platform_owner allows %s", a.ID)
		}
	}
	if admin, _ := c.Role("platform_admin"); len(admin.Actions) == 0 {
		t.Error("platform_admin lost its actions")
	}
}

func TestLoadRefusesMalformedCatalogs(t *testing.T) {
	const okA = `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[{"action":"a.b","scope":"organization","resource_kind":"organization","risk_class":"low","cache_class":"decision_bound","impersonation":"denied"},{"action":"a.c","scope":"organization","resource_kind":"organization","risk_class":"low","cache_class":"decision_bound","impersonation":"denied"}]}`
	const okR = `{"schema_version":"roles.v1","catalog_version":1,"description":"d","roles":[{"role":"r","scope":"organization","actions":["a.b"]}]}`
	if _, err := Load([]byte(okA), []byte(okR)); err != nil {
		t.Fatalf("well-formed catalog refused: %v", err)
	}
	bad := map[string][2]string{
		"role lists unknown action": {okA, strings.Replace(okR, `["a.b"]`, `["a.z"]`, 1)},
		"single segment action":     {strings.Replace(okA, `"a.b"`, `"ab"`, 1), strings.Replace(okR, `["a.b"]`, `["ab"]`, 1)},
		"duplicate action":          {strings.Replace(okA, `"a.c"`, `"a.b"`, 1), okR},
		"duplicate role":            {okA, strings.Replace(okR, `}]}`, `},{"role":"r","scope":"organization","actions":[]}]}`, 1)},
		"duplicate role action":     {okA, strings.Replace(okR, `["a.b"]`, `["a.b","a.b"]`, 1)},
		"wildcard action":           {strings.Replace(okA, `"a.b"`, `"a.*"`, 1), strings.Replace(okR, `["a.b"]`, `["a.*"]`, 1)},
		"scope mismatch":            {okA, strings.Replace(okR, `"scope":"organization"`, `"scope":"platform"`, 1)},
		"unknown scope":             {strings.Replace(okA, `"scope":"organization"`, `"scope":"galaxy"`, 1), okR},
		"unknown member":            {strings.Replace(okA, `"description"`, `"extra":1,"description"`, 1), okR},
		"wrong schema version":      {strings.Replace(okA, `actions.v1`, `actions.v2`, 1), okR},
		"zero catalog version":      {strings.Replace(okA, `"catalog_version":1`, `"catalog_version":0`, 1), okR},
		"version mismatch":          {okA, strings.Replace(okR, `"catalog_version":1`, `"catalog_version":2`, 1)},
		"duplicate member":          {strings.Replace(okA, `"catalog_version":1`, `"catalog_version":1,"catalog_version":2`, 1), okR},
		"duplicate nested member":   {okA, strings.Replace(okR, `"scope":"organization","actions"`, `"scope":"organization","scope":"platform","actions"`, 1)},
		"reserved role superuser":   {okA, strings.Replace(okR, `"role":"r"`, `"role":"superuser"`, 1)},
		"reserved role root":        {okA, strings.Replace(okR, `"role":"r"`, `"role":"root"`, 1)},
		"missing risk class":        {strings.Replace(okA, `"risk_class":"low",`, ``, 1), okR},
		"missing cache class":       {strings.Replace(okA, `,"cache_class":"decision_bound"`, ``, 1), okR},
		"unknown risk class":        {strings.Replace(okA, `"risk_class":"low"`, `"risk_class":"medium"`, 1), okR},
		"unknown cache class":       {strings.Replace(okA, `"cache_class":"decision_bound"`, `"cache_class":"forever"`, 1), okR},
		"high risk cached":          {strings.Replace(okA, `"risk_class":"low"`, `"risk_class":"high"`, 1), okR},
		"not json":                  {"{", okR},
		"trailing data":             {okA + "{}", okR},
	}
	for name, in := range bad {
		if _, err := Load([]byte(in[0]), []byte(in[1])); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
}

func TestEveryEmbeddedActionHasARiskAndCacheClassAndHighRiskIsNeverCached(t *testing.T) {
	for _, a := range Default().Actions() {
		if a.RiskClass == "" || a.CacheClass == "" {
			t.Errorf("%s: risk %q cache %q", a.ID, a.RiskClass, a.CacheClass)
		}
		if a.RiskClass == RiskHigh && a.CacheClass != CacheNone {
			t.Errorf("%s is high risk but cache_class %s", a.ID, a.CacheClass)
		}
		if strings.HasSuffix(a.ID, ".read") != (a.RiskClass == RiskLow) {
			t.Errorf("%s: risk_class %s does not follow the read/non-read split", a.ID, a.RiskClass)
		}
	}
}

func TestLoadRefusesARoleHoldingEveryAction(t *testing.T) {
	a := `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[{"action":"a.b","scope":"organization","resource_kind":"organization","risk_class":"low","cache_class":"decision_bound","impersonation":"denied"},{"action":"a.c","scope":"organization","resource_kind":"organization","risk_class":"low","cache_class":"decision_bound","impersonation":"denied"}]}`
	r := `{"schema_version":"roles.v1","catalog_version":1,"description":"d","roles":[{"role":"r","scope":"organization","actions":["a.b","a.c"]}]}`
	if _, err := Load([]byte(a), []byte(r)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func TestRoleSchemaRefusesAReservedRole(t *testing.T) {
	doc := map[string]any{"schema_version": "roles.v1", "catalog_version": 1, "description": "d",
		"roles": []any{map[string]any{"role": "superuser", "scope": "organization", "actions": []any{"a.b"}}}}
	if err := contracts.Validate(repoRoot(t), "role-catalog.v1", doc); err == nil {
		t.Fatal("the role schema accepted superuser")
	}
}

func TestEmbeddedFilesValidateAgainstTheirSchemas(t *testing.T) {
	root := repoRoot(t)
	for surface, raw := range map[string][]byte{"action-catalog.v1": vocabulary.ActionsV1, "role-catalog.v1": vocabulary.RolesV1} {
		var doc any
		if err := decodeStrict(raw, &doc); err != nil {
			t.Fatal(err)
		}
		if err := contracts.Validate(root, surface, doc); err != nil {
			t.Errorf("%s: %v", surface, err)
		}
	}
}

// catalogOnly is the closed set of catalog actions that have no permission in
// internal/api/session/permissions.go. Catalog-only = not served in /auth/me
// until a web-reviewed change adds it there; every other catalog action must
// still equal a session permission.
var catalogOnly = map[string]bool{"admin.impersonate": true, "billing.read": true, "billing.manage": true, "license.read": true, "members.update": true, "authz.decide": true, "acr.context.read": true}

type sessionEntry struct {
	role string
	perm []string
}

// sessionPermissions parses roleHierarchy and standardPermissions from
// internal/api/session/permissions.go.
func sessionPermissions(t *testing.T) ([]sessionEntry, []string) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(repoRoot(t), "internal/api/session/permissions.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var hier []sessionEntry
	var std []string
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
			return true
		}
		lit, ok := vs.Values[0].(*ast.CompositeLit)
		if !ok {
			return true
		}
		switch vs.Names[0].Name {
		case "standardPermissions":
			for _, e := range lit.Elts {
				std = append(std, unq(t, e))
			}
		case "roleHierarchy":
			for _, e := range lit.Elts {
				el := e.(*ast.CompositeLit)
				en := sessionEntry{role: unq(t, el.Elts[0])}
				for _, p := range el.Elts[1].(*ast.CompositeLit).Elts {
					en.perm = append(en.perm, unq(t, p))
				}
				hier = append(hier, en)
			}
		}
		return false
	})
	if len(hier) != 4 || len(std) != 22 {
		t.Fatalf("parsed %d roles and %d permissions from permissions.go", len(hier), len(std))
	}
	return hier, std
}

// Parity: the role -> action sets equal the hierarchy in
// internal/api/session/permissions.go (roleHierarchy; each role holds its own
// entries and every lower role's), with ':' spelled '.'; catalogOnly actions
// are left out of the comparison.
func TestRolesMatchTheSessionHierarchy(t *testing.T) {
	hier, std := sessionPermissions(t)
	c := Default()
	held := map[string]bool{}
	for _, e := range hier {
		for _, p := range e.perm {
			held[strings.Replace(p, ":", ".", 1)] = true
		}
		want := keys(held)
		r, err := c.Role(e.role)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, a := range r.Actions {
			if !catalogOnly[a] {
				got = append(got, a)
			}
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("role %s = %v, want %v", e.role, got, want)
		}
	}
	// Every legacy permission is an action; admin:* are platform actions only.
	for _, p := range std {
		a, err := c.Action(strings.Replace(p, ":", ".", 1))
		if err != nil {
			t.Errorf("permission %s has no action: %v", p, err)
			continue
		}
		if (strings.HasPrefix(p, "admin:")) != (a.Scope == "platform") {
			t.Errorf("action %s has scope %s", a.ID, a.Scope)
		}
	}
	if len(c.Actions()) != len(std)+len(catalogOnly) {
		t.Errorf("catalog has %d actions, permissions.go %d plus %d catalog-only", len(c.Actions()), len(std), len(catalogOnly))
	}
	if _, err := os.Stat(filepath.Join(repoRoot(t), "internal/api/session/permissions.go")); err != nil {
		t.Fatal(err)
	}
}

func unq(t *testing.T, e ast.Expr) string {
	t.Helper()
	s, err := strconv.Unquote(e.(*ast.BasicLit).Value)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// The catalog actions with no session permission are exactly catalogOnly. Load
// already refuses a role that lists an action of the other scope.
func TestCatalogOnlyActionsAreExactlyTheClosedSet(t *testing.T) {
	_, std := sessionPermissions(t)
	session := map[string]bool{}
	for _, p := range std {
		session[strings.Replace(p, ":", ".", 1)] = true
	}
	extra := map[string]bool{}
	for _, a := range Default().Actions() {
		if !session[a.ID] {
			extra[a.ID] = true
		}
	}
	if strings.Join(keys(extra), ",") != strings.Join(keys(catalogOnly), ",") {
		t.Errorf("catalog actions with no session permission = %v, want exactly %v", keys(extra), keys(catalogOnly))
	}
}

// The billing and license actions carry these classes and are held by exactly these roles.
func TestCatalogHoldsTheBillingAndLicenseActions(t *testing.T) {
	classes := map[string][2]string{
		"billing.read":   {RiskLow, CacheDecisionBound},
		"billing.manage": {RiskHigh, CacheNone},
		"license.read":   {RiskLow, CacheDecisionBound},
	}
	holders := map[string][]string{
		"billing.read":   {"admin", "member", "owner", "viewer"},
		"billing.manage": {"admin", "owner"},
		"license.read":   {"admin", "member", "owner", "viewer"},
	}
	c := Default()
	for id, want := range classes {
		a, err := c.Action(id)
		if err != nil {
			t.Errorf("action %s: %v", id, err)
			continue
		}
		if a.RiskClass != want[0] || a.CacheClass != want[1] {
			t.Errorf("action %s = %s/%s, want %s/%s", id, a.RiskClass, a.CacheClass, want[0], want[1])
		}
		var got []string
		for _, r := range c.Roles() {
			if c.Allows(r.ID, id) {
				got = append(got, r.ID)
			}
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(holders[id], ",") {
			t.Errorf("action %s is held by %v, want %v", id, got, holders[id])
		}
	}
}

// acr.context.read is the first repository-kind action: organization scope, a repository as its resource.
func TestEmbeddedCatalogHoldsAcrContextReadAsARepositoryAction(t *testing.T) {
	a, err := Default().Action("acr.context.read")
	if err != nil {
		t.Fatal(err)
	}
	if a.Scope != ScopeOrganization || a.ResourceKind != "repository" || a.RiskClass != RiskLow || a.CacheClass != CacheResourceRevisionBound {
		t.Errorf("acr.context.read = %+v, want organization scope, repository kind, low risk, resource_revision_bound", a)
	}
}

// The resource-kind set widens to repository; the scope sets do not.
func TestRepositoryIsAResourceKindAndNeverAScope(t *testing.T) {
	const okR = `{"schema_version":"roles.v1","catalog_version":1,"description":"d","roles":[{"role":"r","scope":"organization","actions":["a.b"]}]}`
	act := func(scope, kind string) string {
		row := func(id string) string {
			return `{"action":"` + id + `","scope":"` + scope + `","resource_kind":"` + kind + `","risk_class":"low","cache_class":"decision_bound","impersonation":"denied"}`
		}
		return `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[` + row("a.b") + `,` + row("a.c") + `]}`
	}
	if _, err := Load([]byte(act("organization", "repository")), []byte(okR)); err != nil {
		t.Errorf("repository resource_kind refused: %v", err)
	}
	for name, a := range map[string]string{
		"repository scope":      act("repository", "organization"),
		"unknown resource kind": act("organization", "galaxy"),
	} {
		if _, err := Load([]byte(a), []byte(okR)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	allRepo := strings.Replace(okR, `"scope":"organization"`, `"scope":"repository"`, 1)
	if _, err := Load([]byte(act("repository", "repository")), []byte(allRepo)); err == nil {
		t.Error("a repository scope was accepted for an action and its role")
	}
	roleScope := strings.Replace(okR, `"scope":"organization"`, `"scope":"repository"`, 1)
	if _, err := Load([]byte(act("organization", "organization")), []byte(roleScope)); err == nil {
		t.Error("a repository role scope was accepted")
	}
}

func TestImpersonationIsDeniedOnTheG51List(t *testing.T) {
	want := map[string]bool{
		"admin.users": true, "admin.orgs": true, "admin.impersonate": true, "members.invite": true, "members.manage": true,
		"org.write": true, "org.delete": true, "billing.manage": true, "settings.write": true, "integrations.write": true,
		"members.update": true, "authz.decide": true,
	}
	actions := Default().Actions()
	if len(actions) != 29 {
		t.Fatalf("%d actions, want 29", len(actions))
	}
	for _, a := range actions {
		denied := a.Impersonation == ImpersonationDenied
		if denied != want[a.ID] {
			t.Errorf("%s impersonation = %s", a.ID, a.Impersonation)
		}
	}
}

func TestLoadRefusesAnActionWithNoImpersonationValue(t *testing.T) {
	const roles = `{"schema_version":"roles.v1","catalog_version":1,"description":"d","roles":[{"role":"r","scope":"organization","actions":["a.b"]}]}`
	for name, member := range map[string]string{"missing": ``, "unknown": `,"impersonation":"maybe"`, "empty": `,"impersonation":""`} {
		actions := `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[{"action":"a.b","scope":"organization","resource_kind":"organization","risk_class":"low","cache_class":"none"` + member + `}]}`
		if _, err := Load([]byte(actions), []byte(roles)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Load = %v, want ErrInvalid", name, err)
		}
	}
}

func TestOnlyThePlatformAdminRoleHoldsImpersonate(t *testing.T) {
	got := Default().RolesHolding("admin.impersonate", ScopePlatform)
	if len(got) != 1 || got[0] != "platform_admin" {
		t.Fatalf("roles holding admin.impersonate = %v", got)
	}
	if got := Default().RolesHolding("admin.impersonate", ScopeOrganization); len(got) != 0 {
		t.Fatalf("organization roles holding admin.impersonate = %v", got)
	}
}

func TestDelegatedActionsAreTheAllowedActionsOfAnOrganizationRole(t *testing.T) {
	got, err := Default().DelegatedActions("owner")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("owner permits no delegated action")
	}
	for _, id := range got {
		if a, err := Default().Action(id); err != nil || a.Impersonation != ImpersonationAllowed {
			t.Errorf("owner delegated action %s: %+v, %v", id, a, err)
		}
	}
	for _, role := range []string{"platform_admin", "custom", ""} {
		if _, err := Default().DelegatedActions(role); !errors.Is(err, ErrUnknownRole) {
			t.Errorf("DelegatedActions(%q) = %v, want ErrUnknownRole", role, err)
		}
	}
}

func TestImpersonationValuesOfTheRepositoryReadAndTheTwoDeniedActions(t *testing.T) {
	want := map[string]string{
		"acr.context.read": ImpersonationAllowed,
		"authz.decide":     ImpersonationDenied,
		"members.update":   ImpersonationDenied,
	}
	for id, v := range want {
		a, err := Default().Action(id)
		if err != nil || a.Impersonation != v {
			t.Errorf("%s impersonation = %q, %v; want %s", id, a.Impersonation, err, v)
		}
	}
}

func TestShippedRolesDelegateNoRepositoryReadToday(t *testing.T) {
	for _, r := range Default().Roles() {
		if r.Scope != ScopeOrganization {
			continue
		}
		got, err := Default().DelegatedActions(r.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range got {
			if id == "acr.context.read" {
				t.Errorf("role %s delegates acr.context.read: a role now holds it; the cell that it DOES delegate belongs with that change", r.ID)
			}
		}
	}
}

func TestDelegatedActionsReturnTheRepositoryReadForARoleThatHoldsIt(t *testing.T) {
	const actions = `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[{"action":"acr.context.read","scope":"organization","resource_kind":"repository","risk_class":"low","cache_class":"resource_revision_bound","impersonation":"allowed"},{"action":"x.write","scope":"organization","resource_kind":"organization","risk_class":"high","cache_class":"none","impersonation":"denied"},{"action":"x.other","scope":"organization","resource_kind":"organization","risk_class":"high","cache_class":"none","impersonation":"denied"}]}`
	const roles = `{"schema_version":"roles.v1","catalog_version":1,"description":"d","roles":[{"role":"r","scope":"organization","actions":["acr.context.read","x.write"]}]}`
	c, err := Load([]byte(actions), []byte(roles))
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.DelegatedActions("r")
	if err != nil || len(got) != 1 || got[0] != "acr.context.read" {
		t.Fatalf("DelegatedActions = %v, %v; want [acr.context.read]", got, err)
	}
}
