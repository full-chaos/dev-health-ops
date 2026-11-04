package rivermigrate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// The api role's manifest follows both of go-api's switches, AUTH_PROXY_SESSIONS
// and AUTH_PLATFORM_ROLES_OWNED, read as go-api reads them.
func TestRivermigrateReadsPlatformRolesOwned(t *testing.T) {
	for _, c := range []struct{ sessions, owned string }{
		{"", ""}, {"true", ""}, {"", "true"}, {"true", "true"}, {"true", "false"}, {"", " TRUE "},
	} {
		values := map[string]string{}
		if c.sessions != "" {
			values["AUTH_PROXY_SESSIONS"] = c.sessions
		}
		if c.owned != "" {
			values["AUTH_PLATFORM_ROLES_OWNED"] = c.owned
		}
		posture, sessions, err := apiPostureFromLookup(envLookup(values))
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		owned := strings.EqualFold(strings.TrimSpace(c.owned), "true")
		if sessions != (c.sessions == "true") {
			t.Errorf("%+v: sessions = %v", c, sessions)
		}
		if want := postgresstore.APIPostureFor(c.sessions == "true", owned); !reflect.DeepEqual(posture, want) {
			t.Errorf("%+v: posture does not follow the switches", c)
		}
		for _, table := range posture.RequiredTables {
			if table.TableName == "users" && table.AllowInsert == owned {
				t.Errorf("%+v: users table-wide insert = %v", c, table.AllowInsert)
			}
		}
	}
}

// A malformed switch is a configuration error, never read as off.
func TestRivermigrateRefusesABadPlatformRolesOwned(t *testing.T) {
	for _, value := range []string{"yes", "1", "on"} {
		_, _, err := apiPostureFromLookup(envLookup(map[string]string{"AUTH_PLATFORM_ROLES_OWNED": value}))
		if err == nil || !strings.Contains(err.Error(), "AUTH_PLATFORM_ROLES_OWNED") {
			t.Errorf("AUTH_PLATFORM_ROLES_OWNED=%q was accepted: %v", value, err)
		}
	}
}

// The migrate step names the api manifest only through apiPostureFromLookup,
// so the grant side reads the same two switches as go-api's readiness.
func TestMigrateNamesTheAPIManifestOnlyThroughAPIPostureFromLookup(t *testing.T) {
	matches, err := filepath.Glob("*.go")
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
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "postgresstore" && (sel.Sel.Name == "APIPosture" || sel.Sel.Name == "APIPostureFor") {
				calls[path]++
			}
			return true
		})
	}
	if want := map[string]int{"api_posture.go": 1}; !reflect.DeepEqual(calls, want) {
		t.Errorf("postgresstore.APIPosture/APIPostureFor calls per file = %v, want %v", calls, want)
	}
	source, err := os.ReadFile("rivermigrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "apiPostureFromLookup(lookup)") {
		t.Error("rivermigrate.go does not build the api grants with apiPostureFromLookup")
	}
}
