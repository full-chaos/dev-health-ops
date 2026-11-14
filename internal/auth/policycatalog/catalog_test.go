package policycatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	actionsSHA = "ae502cc9dcfbd8f87ee4e8aaa08bbc553c93799c4eaa4f8fc08cb6dab1efcc34"
	rolesSHA   = "80373e40a776353acf619f9baa7d7b7d82d46eedf2f88cb8414e0dd5298b07be"
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
// the frozen table of sessionPermissions (the former internal/api/session/permissions.go). Catalog-only = not in that table
// until a web-reviewed change adds it there; every other catalog action must
// still equal a session permission.
var catalogOnly = map[string]bool{"admin.impersonate": true, "billing.read": true, "billing.manage": true, "license.read": true, "members.update": true, "authz.decide": true, "acr.context.read": true, "reports.write": true, "acr.evidence.read": true, "acr.credential.issue": true, "acr.credential.revoke": true, "sso.write": true,
	"admin.billing": true, "admin.flags": true, "admin.audit": true, "admin.telemetry": true, "admin.stats": true,
	"credentials.project": true,
}

type sessionEntry struct {
	role string
	perm []string
}

// sessionPermissions is the legacy permission table the catalog was validated against: roleHierarchy (each role's own entries; a
// role holds its own and every lower role's) and the 22 standard permission names. It is frozen here from
// internal/api/session/permissions.go as it stood before CHAOS-13585 deleted that file (git show cfeabc7f4cd1:internal/api/session/permissions.go);
// the catalog parity below stays pinned without the production map.
func sessionPermissions(t *testing.T) ([]sessionEntry, []string) {
	t.Helper()
	std := []string{
		"analytics:read", "analytics:export", "metrics:read", "metrics:compute", "work_items:read", "work_items:sync",
		"git:read", "git:sync", "teams:read", "teams:write", "settings:read", "settings:write", "integrations:read",
		"integrations:write", "members:read", "members:invite", "members:manage", "org:read", "org:write", "org:delete",
		"admin:users", "admin:orgs",
	}
	hier := []sessionEntry{
		{"viewer", []string{"analytics:read", "metrics:read", "work_items:read", "git:read", "teams:read", "settings:read", "members:read", "org:read"}},
		{"member", []string{"analytics:export"}},
		{"admin", []string{"metrics:compute", "work_items:sync", "git:sync", "teams:write", "settings:write", "integrations:read",
			"integrations:write", "members:invite", "members:manage", "org:write"}},
		{"owner", []string{"org:delete"}},
	}
	if len(hier) != 4 || len(std) != 22 {
		t.Fatalf("frozen table has %d roles and %d permissions", len(hier), len(std))
	}
	return hier, std
}

// Parity: the role -> action sets equal the hierarchy in
// internal/api/session/permissions.go (roleHierarchy of the frozen table; each role holds its own
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
		t.Errorf("catalog has %d actions, frozen table %d plus %d catalog-only", len(c.Actions()), len(std), len(catalogOnly))
	}
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

// reports.write covers the saved-report writes query-api serves as the GraphQL mutations createSavedReport, updateSavedReport,
// deleteSavedReport, cloneSavedReport and triggerReport. Parity with today's guard: the only check on those writes is
// requireMutationOrg (internal/queryapi/graph/reports_mutations.go:38), which reads the organization and no role, so
// every organization role that can read can write; the platform roles hold no organization action.
func TestReportsWriteIsHeldByEveryOrganizationRoleAsTodaysWritersAre(t *testing.T) {
	c := Default()
	a, err := c.Action("reports.write")
	if err != nil {
		t.Fatalf("action reports.write: %v", err)
	}
	if a.Scope != ScopeOrganization || a.ResourceKind != "organization" || a.RiskClass != RiskHigh || a.CacheClass != CacheNone || a.Impersonation != ImpersonationDenied {
		t.Errorf("reports.write = %+v, want organization scope and kind, high risk, cache none, impersonation denied", a)
	}
	var got []string
	for _, r := range c.Roles() {
		if c.Allows(r.ID, "reports.write") {
			got = append(got, r.ID)
		}
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "admin,member,owner,viewer" {
		t.Errorf("reports.write is held by %v, want admin,member,owner,viewer", got)
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

// acr.context.read reads a context at a revision, like git.read: the organization roles that hold git.read hold it, and
// no platform role does. The real catalog allows those roles by default.
func TestAcrContextReadIsHeldByExactlyTheRolesThatHoldGitRead(t *testing.T) {
	c := Default()
	var holders []string
	for _, r := range c.Roles() {
		if got, want := c.Allows(r.ID, "acr.context.read"), c.Allows(r.ID, "git.read"); got != want {
			t.Errorf("role %s: acr.context.read %v, git.read %v", r.ID, got, want)
		}
		if c.Allows(r.ID, "acr.context.read") {
			holders = append(holders, r.ID)
		}
	}
	sort.Strings(holders)
	if got := strings.Join(holders, ","); got != "admin,member,owner,viewer" {
		t.Errorf("acr.context.read is held by %q, want admin,member,owner,viewer", got)
	}
	if c.Allows("platform_admin", "acr.context.read") {
		t.Error("platform_admin holds acr.context.read")
	}
	for _, role := range []string{"platform_admin", "platform_owner"} {
		for _, action := range []string{"acr.context.read", "git.read"} {
			if c.Allows(role, action) {
				t.Errorf("%s holds %s", role, action)
			}
		}
	}
}

func TestImpersonationIsDeniedOnTheG51List(t *testing.T) {
	want := map[string]bool{
		"admin.users": true, "admin.orgs": true, "admin.impersonate": true,
		"admin.billing": true, "admin.flags": true, "admin.audit": true, "admin.telemetry": true, "admin.stats": true, "members.invite": true, "members.manage": true,
		"org.write": true, "org.delete": true, "billing.manage": true, "settings.write": true, "integrations.write": true, "reports.write": true,
		"members.update": true, "authz.decide": true, "acr.credential.issue": true, "acr.credential.revoke": true, "sso.write": true, "credentials.project": true,
	}
	actions := Default().Actions()
	if len(actions) != 40 {
		t.Fatalf("%d actions, want 40", len(actions))
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
		"acr.context.read":     ImpersonationAllowed,
		"acr.evidence.read":    ImpersonationAllowed,
		"acr.credential.issue": ImpersonationDenied,
		"authz.decide":         ImpersonationDenied,
		"members.update":       ImpersonationDenied,
	}
	for id, v := range want {
		a, err := Default().Action(id)
		if err != nil || a.Impersonation != v {
			t.Errorf("%s impersonation = %q, %v; want %s", id, a.Impersonation, err, v)
		}
	}
}

func TestShippedRolesDelegateTheRepositoryReadExactlyWhenTheyHoldIt(t *testing.T) {
	c := Default()
	delegating := 0
	for _, r := range c.Roles() {
		if r.Scope != ScopeOrganization {
			continue
		}
		got, err := c.DelegatedActions(r.ID)
		if err != nil {
			t.Fatal(err)
		}
		has := false
		for _, id := range got {
			has = has || id == "acr.context.read"
		}
		if has != c.Allows(r.ID, "acr.context.read") {
			t.Errorf("role %s: delegates acr.context.read %v, holds it %v", r.ID, has, c.Allows(r.ID, "acr.context.read"))
		}
		if has {
			delegating++
		}
	}
	if delegating != 4 {
		t.Errorf("%d organization roles delegate acr.context.read, want 4", delegating)
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

// The admin platform actions are exactly the eight the platform_admin
// role lists, every one high risk, never cached and denied to a delegated
// session, and no organization role holds one.
func TestPlatformActionsAreHeldOnlyByPlatformAdmin(t *testing.T) {
	c := Default()
	want := []string{"admin.audit", "admin.billing", "admin.flags", "admin.impersonate", "admin.orgs", "admin.stats", "admin.telemetry", "admin.users"}
	var platform []string
	for _, a := range c.Actions() {
		if a.Scope != ScopePlatform || !strings.HasPrefix(a.ID, "admin.") {
			continue
		}
		platform = append(platform, a.ID)
		if a.RiskClass != RiskHigh || a.CacheClass != CacheNone || a.ResourceKind != "platform" || a.Impersonation != ImpersonationDenied {
			t.Errorf("platform action %s: risk %s cache %s kind %s impersonation %s", a.ID, a.RiskClass, a.CacheClass, a.ResourceKind, a.Impersonation)
		}
		for _, r := range c.Roles() {
			if r.ID != "platform_admin" && c.Allows(r.ID, a.ID) {
				t.Errorf("role %s holds platform action %s", r.ID, a.ID)
			}
		}
	}
	sort.Strings(platform)
	if strings.Join(platform, ",") != strings.Join(want, ",") {
		t.Fatalf("platform actions %v, want %v", platform, want)
	}
	for _, a := range want {
		if !c.Allows("platform_admin", a) {
			t.Errorf("platform_admin does not hold %s", a)
		}
	}
}

// acr.evidence.read reads evidence at a revision like acr.context.read; acr.credential.issue mints a credential and is never cached.
func TestEmbeddedCatalogHoldsTheAcrEvidenceReadAndCredentialIssueRows(t *testing.T) {
	want := map[string]Action{
		"acr.evidence.read":    {Scope: ScopeOrganization, ResourceKind: "repository", RiskClass: RiskLow, CacheClass: CacheResourceRevisionBound, Impersonation: ImpersonationAllowed},
		"acr.credential.issue": {Scope: ScopeOrganization, ResourceKind: "organization", RiskClass: RiskHigh, CacheClass: CacheNone, Impersonation: ImpersonationDenied, RepositoryScope: RepositoryScopeFiltered},
	}
	for id, w := range want {
		a, err := Default().Action(id)
		if err != nil {
			t.Errorf("%s: %v", id, err)
			continue
		}
		if a.Scope != w.Scope || a.ResourceKind != w.ResourceKind || a.RiskClass != w.RiskClass || a.CacheClass != w.CacheClass || a.Impersonation != w.Impersonation ||
			(w.RepositoryScope != "" && a.RepositoryScope != w.RepositoryScope) {
			t.Errorf("%s = %+v, want %+v", id, a, w)
		}
	}
}

// D6849 (legacy parity): every organization role reads ACR evidence and issues an ACR credential, as
// any org session may today (rule 2 for the evidence read). No platform role holds either action.
func TestEveryOrganizationRoleHoldsAcrEvidenceReadAndCredentialIssue(t *testing.T) {
	c := Default()
	org := map[string]bool{"viewer": true, "member": true, "admin": true, "owner": true}
	for _, r := range c.Roles() {
		for _, id := range []string{"acr.evidence.read", "acr.credential.issue"} {
			if got, want := c.Allows(r.ID, id), org[r.ID]; got != want {
				t.Errorf("role %s holds %s = %v, want %v", r.ID, id, got, want)
			}
		}
	}
	for id := range org {
		if _, err := c.Role(id); err != nil {
			t.Errorf("organization role %s is missing: %v", id, err)
		}
	}
}

// Product rule 1 and the executed legacy cells: a platform admin with no membership row approves a device and
// gives consent in the organization of its session, so acr.credential.issue carries the platform-admin flag.
func TestAcrCredentialIssueAllowsAPlatformAdminWithNoMembership(t *testing.T) {
	a, err := Default().Action("acr.credential.issue")
	if err != nil {
		t.Fatal(err)
	}
	if !a.PlatformAdmin {
		t.Fatalf("acr.credential.issue PlatformAdmin = false, want true")
	}
}

// Legacy refuses the ACR repository reads to a platform admin with no membership row in its token's
// organization, so no repository action carries the flag.
func TestNoRepositoryActionCarriesThePlatformAdminFlag(t *testing.T) {
	for _, a := range Default().Actions() {
		if a.ResourceKind == "repository" && a.PlatformAdmin {
			t.Errorf("repository action %s carries platform_admin allowed", a.ID)
		}
	}
}

func TestLoadReadsPlatformAdminOnlyOnAnOrganizationAction(t *testing.T) {
	const roles = `{"schema_version":"roles.v1","catalog_version":1,"description":"d","roles":[{"role":"r","scope":"organization","actions":["a.b"]}]}`
	load := func(scope, kind, member string) (*Catalog, error) {
		actions := `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[` +
			`{"action":"a.b","scope":"organization","resource_kind":"organization","risk_class":"low","cache_class":"none","impersonation":"denied"},` +
			`{"action":"a.c","scope":"` + scope + `","resource_kind":"` + kind + `","risk_class":"low","cache_class":"none","impersonation":"denied"` + member + `}]}`
		return Load([]byte(actions), []byte(roles))
	}
	for member, want := range map[string]bool{``: false, `,"platform_admin":"denied"`: false, `,"platform_admin":"allowed"`: true} {
		c, err := load("organization", "organization", member)
		if err != nil {
			t.Fatalf("%q: %v", member, err)
		}
		if a, _ := c.Action("a.c"); a.PlatformAdmin != want {
			t.Errorf("%q: PlatformAdmin %v, want %v", member, a.PlatformAdmin, want)
		}
	}
	for name, c := range map[string][3]string{
		"unknown value":       {"organization", "organization", `,"platform_admin":"maybe"`},
		"platform action":     {"platform", "platform", `,"platform_admin":"allowed"`},
		"repository resource": {"organization", "repository", `,"platform_admin":"allowed"`},
	} {
		if _, err := load(c[0], c[1], c[2]); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Load = %v, want ErrInvalid", name, err)
		}
	}
}

func TestThePlatformAdminActionsAreTheAdminGuardAndTheLegacyQueryAPIAndBillingActions(t *testing.T) {
	var got []string
	for _, a := range Default().Actions() {
		if a.PlatformAdmin {
			got = append(got, a.ID)
		}
	}
	sort.Strings(got)
	want := []string{"acr.credential.issue", "acr.credential.revoke", "analytics.read", "billing.manage", "billing.read", "git.read", "integrations.read",
		"integrations.write", "license.read", "members.invite", "members.manage", "metrics.read", "org.read", "reports.write",
		"settings.read", "settings.write", "teams.write", "work_items.read"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("platform_admin actions %v, want %v", got, want)
	}
}

// Catalog 20 adds sso.write for create, update, delete, activate and deactivate of an SSO provider. It is organization
// scoped, high risk and never cached, denied to a delegated session, held by admin and owner alone, and it carries no
// platform_admin flag: the legacy provider writes judged the token role alone, so a platform admin with no membership
// row got 403. org.write is unchanged by it, byte for byte, so no other org.write route moves.
func TestSSOWriteIsOwnerAndAdminOnlyAndOrgWriteIsUnchanged(t *testing.T) {
	c := Default()
	sso, err := c.Action("sso.write")
	if err != nil {
		t.Fatal(err)
	}
	if sso.Scope != ScopeOrganization || sso.ResourceKind != ScopeOrganization || sso.RiskClass != "high" || sso.CacheClass != "none" ||
		sso.Impersonation != ImpersonationDenied || sso.PlatformAdmin {
		t.Errorf("sso.write = %+v, want organization, organization, high, none, impersonation denied, no platform_admin", sso)
	}
	org, err := c.Action("org.write")
	if err != nil {
		t.Fatal(err)
	}
	if org.Scope != ScopeOrganization || org.ResourceKind != ScopeOrganization || org.RiskClass != "high" || org.CacheClass != "none" ||
		org.Impersonation != ImpersonationDenied || org.PlatformAdmin {
		t.Errorf("org.write = %+v, want organization, organization, high, none, impersonation denied, no platform_admin (unchanged)", org)
	}
	if got := strings.Join(c.RolesHolding("sso.write", ScopeOrganization), ","); got != "admin,owner" {
		t.Errorf("roles holding sso.write = %s, want admin,owner", got)
	}
	if got := strings.Join(c.RolesHolding("org.write", ScopeOrganization), ","); got != "admin,owner" {
		t.Errorf("roles holding org.write = %s, want admin,owner (unchanged)", got)
	}
	for _, role := range []string{"viewer", "member", "platform_admin", "platform_owner"} {
		if c.Allows(role, "sso.write") {
			t.Errorf("%s holds sso.write", role)
		}
	}
}
