package telemetry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// The instance-wide report is served to a non-impersonating caller that holds
// admin.telemetry, and to no other.
func TestPlatformAction_ReportNeedsAdminTelemetry(t *testing.T) {
	var others []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminTelemetry {
			others = append(others, a)
		}
	}
	rest, only, empty := rule.PlatformOf(others...), rule.PlatformOf(rule.ActionAdminTelemetry), rule.PlatformOf()
	h := handlers{logger: slog.New(slog.DiscardHandler)}
	served := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, c := range []struct {
		name string
		user *policy.User
		want int
	}{
		{"the flag only, as before", &policy.User{IsSuperuser: true}, 200},
		{"only admin.telemetry", &policy.User{Platform: &only}, 200},
		{"the flag beside every other action", &policy.User{IsSuperuser: true, Platform: &rest}, 403},
		{"an empty set beside the flag", &policy.User{IsSuperuser: true, Platform: &empty}, 403},
		{"no power", &policy.User{}, 403},
	} {
		r := httptest.NewRequest(http.MethodPost, "/report", nil)
		r = r.WithContext(policy.WithUser(r.Context(), c.user))
		w := httptest.NewRecorder()
		h.requirePlatformRole(served).ServeHTTP(w, r)
		if w.Code != c.want {
			t.Errorf("%s: status %d, want %d", c.name, w.Code, c.want)
		}
	}
}

// No function of the package decides by the superuser flag.
func TestPlatformAction_TelemetryNeverReadsTheSuperuserFlag(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		tree, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(tree, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "IsSuperuser" {
				t.Errorf("%s: reads IsSuperuser at %s", f, fset.Position(sel.Pos()))
			}
			return true
		})
	}
}
