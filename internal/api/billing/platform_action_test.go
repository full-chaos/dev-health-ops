package billing

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

func allBut(action string) *rule.Platform {
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != action {
			rest = append(rest, a)
		}
	}
	p := rule.PlatformOf(rest...)
	return &p
}

// refusedFor serves h on a request carrying user and reports whether the
// handler answered 403 before touching any dependency (the handler here has no
// pool, so a caller that passed would fail some other way).
func refusedFor(t *testing.T, name string, user *policy.User, serve func(http.ResponseWriter, *http.Request), path map[string]string, query string) (refused bool) {
	t.Helper()
	defer func() {
		if recover() != nil {
			refused = false
		}
	}()
	r := httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
	for k, v := range path {
		r.SetPathValue(k, v)
	}
	r = r.WithContext(policy.WithUser(r.Context(), user))
	w := httptest.NewRecorder()
	serve(w, r)
	return w.Code == http.StatusForbidden
}

// The superuser-only billing handlers refuse a caller by the action: a caller
// that holds every action but admin.billing is refused, the same as one with
// no power at all, and a caller that holds only admin.billing passes the check.
func TestPlatformAction_BillingHandlersNeedAdminBilling(t *testing.T) {
	h := handlers{logger: slog.New(slog.DiscardHandler)}
	id := uuid.New().String()
	handlersUnderTest := []struct {
		name  string
		serve func(http.ResponseWriter, *http.Request)
		path  map[string]string
		query string
	}{
		{"listAudit", h.listAudit, nil, "org_id=" + id},
		{"getAudit", h.getAudit, map[string]string{"audit_id": id}, ""},
		{"reconcile", h.reconcile, nil, ""},
		{"listRefunds", h.listRefunds, nil, ""},
		{"getRefund", h.getRefund, map[string]string{"refund_id": id}, ""},
		{"syncPlan", h.syncPlan, map[string]string{"plan_id": id}, ""},
	}
	only := rule.PlatformOf(rule.ActionAdminBilling)
	for _, c := range handlersUnderTest {
		for _, k := range []struct {
			name string
			user *policy.User
			want bool
		}{
			{"every other action", &policy.User{IsSuperuser: true, Platform: allBut(rule.ActionAdminBilling)}, true},
			{"no power", &policy.User{}, true},
			{"only admin.billing", &policy.User{Platform: &only}, false},
			{"the flag only, as before", &policy.User{IsSuperuser: true}, false},
		} {
			if got := refusedFor(t, c.name, k.user, c.serve, c.path, c.query); got != k.want {
				t.Errorf("%s, %s: refused = %v, want %v", c.name, k.name, got, k.want)
			}
		}
	}
}

// invoiceOrg reads another org's invoices only for a caller holding admin.billing.
func TestPlatformAction_InvoiceOrgNeedsAdminBilling(t *testing.T) {
	other := uuid.New()
	own := uuid.New()
	only := rule.PlatformOf(rule.ActionAdminBilling)
	for _, c := range []struct {
		name string
		user *policy.User
		want uuid.UUID
	}{
		{"every other action", &policy.User{OrgID: own.String(), IsSuperuser: true, Platform: allBut(rule.ActionAdminBilling)}, own},
		{"only admin.billing", &policy.User{OrgID: own.String(), Platform: &only}, other},
		{"the flag only, as before", &policy.User{OrgID: own.String(), IsSuperuser: true}, other},
	} {
		got, refusal, err := invoiceOrg(nil, handlers{logger: slog.New(slog.DiscardHandler)}, nil, c.user, &other)
		if err != nil || refusal != nil || got == nil || *got != c.want {
			t.Errorf("%s: org %v, refusal %v, err %v; want %s", c.name, got, refusal, err, c.want)
		}
	}
}

// No function of the package decides by the superuser flag: every platform
// decision here names an action (a flag read would make a narrower set a
// superuser again).
func TestPlatformAction_BillingNeverReadsTheSuperuserFlag(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := parser.ParseFile(fset, f, raw, 0)
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

// The pull route is the one platform-wrapped route of the package: it is
// wrapped by admin.billing, and no other route of the table is wrapped by a
// platform wrap.
func TestPlatformAction_BillingPullRouteIsWrappedByAdminBilling(t *testing.T) {
	raw, err := os.ReadFile("routes.go")
	if err != nil {
		t.Fatal(err)
	}
	wrapped := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, "WrapPlatform(") {
			continue
		}
		wrapped++
		if !strings.Contains(line, "/plans/pull-stripe") || !strings.Contains(line, "WrapPlatform(policy.ActionAdminBilling,") {
			t.Errorf("a platform wrap that is not the pull route under admin.billing: %s", strings.TrimSpace(line))
		}
	}
	if wrapped != 1 {
		t.Errorf("%d platform-wrapped routes, want exactly the pull route", wrapped)
	}
}
