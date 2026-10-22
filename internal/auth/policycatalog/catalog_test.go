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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	actionsSHA = "e53563bacda11b90037d54a26b847304742766b6dfd99d8575349f53cb2dc894"
	rolesSHA   = "8bf44f475ab6bc4f307b56101b92aa6e978d0fa42d1c56385fb42c5c67fa95d6"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
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

func TestLoadRefusesMalformedCatalogs(t *testing.T) {
	const okA = `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[{"action":"a.b","scope":"organization","resource_kind":"organization"},{"action":"a.c","scope":"organization","resource_kind":"organization"}]}`
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
		"not json":                  {"{", okR},
		"trailing data":             {okA + "{}", okR},
	}
	for name, in := range bad {
		if _, err := Load([]byte(in[0]), []byte(in[1])); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
}

func TestLoadRefusesARoleHoldingEveryAction(t *testing.T) {
	a := `{"schema_version":"actions.v1","catalog_version":1,"description":"d","actions":[{"action":"a.b","scope":"organization","resource_kind":"organization"},{"action":"a.c","scope":"organization","resource_kind":"organization"}]}`
	r := `{"schema_version":"roles.v1","catalog_version":1,"description":"d","roles":[{"role":"r","scope":"organization","actions":["a.b","a.c"]}]}`
	if _, err := Load([]byte(a), []byte(r)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
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

// Parity: the role -> action sets equal the hierarchy in
// internal/api/session/permissions.go (roleHierarchy; each role holds its own
// entries and every lower role's), with ':' spelled '.'.
func TestRolesMatchTheSessionHierarchy(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(repoRoot(t), "internal/api/session/permissions.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	type entry struct {
		role string
		perm []string
	}
	var hier []entry
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
				en := entry{role: unq(t, el.Elts[0])}
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
		got := append([]string(nil), r.Actions...)
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
	if len(c.Actions()) != len(std) {
		t.Errorf("catalog has %d actions, permissions.go %d", len(c.Actions()), len(std))
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
