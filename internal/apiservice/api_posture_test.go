package apiservice

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// Readiness holds the manifest the migrate step applied: the same two switches.
func TestAPIPostureFollowsPlatformRolesOwned(t *testing.T) {
	sessions := AuthProxyConfig{Upstream: &url.URL{Scheme: "http", Host: "auth-service:8095"}, On: map[AuthAggregate]bool{AuthSessions: true}}
	for _, c := range []struct {
		proxy AuthProxyConfig
		owned string
		want  postgres.RolePosture
	}{
		{AuthProxyConfig{}, "", postgres.APIPostureFor(false, false)},
		{AuthProxyConfig{}, "true", postgres.APIPostureFor(false, true)},
		{sessions, "true", postgres.APIPostureFor(true, true)},
		{sessions, "false", postgres.APIPostureFor(true, false)},
		{sessions, "", postgres.APIPostureFor(true, false)},
	} {
		got, err := apiPostureFor(c.proxy, envLookup(map[string]string{"AUTH_PLATFORM_ROLES_OWNED": c.owned}))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("owned=%q sessions=%v: the readiness manifest is not the migrate step's", c.owned, c.proxy.Enabled(AuthSessions))
		}
	}
}

// A malformed switch is a configuration error: readiness cannot name a manifest.
func TestAPIPostureRefusesABadPlatformRolesOwned(t *testing.T) {
	_, err := apiPostureFor(AuthProxyConfig{}, envLookup(map[string]string{"AUTH_PLATFORM_ROLES_OWNED": "maybe"}))
	if err == nil || !strings.Contains(err.Error(), "AUTH_PLATFORM_ROLES_OWNED") {
		t.Errorf("a bad AUTH_PLATFORM_ROLES_OWNED was accepted: %v", err)
	}
}

// The readiness check names its manifest only through apiPostureFor: a direct
// postgres.APIPosture or APIPostureFor call elsewhere would read one switch and
// drift from the migrate step.
func TestReadinessNamesTheAPIManifestOnlyThroughAPIPostureFor(t *testing.T) {
	calls := postgresPostureCalls(t, ".")
	if want := map[string]int{"api_posture.go": 1}; !reflect.DeepEqual(calls, want) {
		t.Errorf("postgres.APIPosture/APIPostureFor calls per file = %v, want %v", calls, want)
	}
	deps, err := os.ReadFile("deps.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(deps), "apiPostureFor(authProxy, envsecrets.ProcessLookup)") {
		t.Error("deps.go does not build the readiness manifest with apiPostureFor over the process environment")
	}
}

func postgresPostureCalls(t *testing.T, dir string) map[string]int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]int{}
	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "postgres" && (sel.Sel.Name == "APIPosture" || sel.Sel.Name == "APIPostureFor") {
				calls[filepath.Base(path)]++
			}
			return true
		})
	}
	return calls
}
