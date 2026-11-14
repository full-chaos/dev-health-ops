package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// noActionPlatformRoute is a WrapPlatform row whose profile row names no action: the guard asks only the wrap's own
// platform question.
const noActionPlatformRoute = "GET /api/v1/admin/platform-question-probe/{probe_id}"

// platformQuestionHandler mounts the rows whose admin-cell answer is the wrap's platform question: the /admin/users
// rows (WrapAdminOr, no profile action) and a WrapPlatform row with no profile action.
func platformQuestionHandler(t *testing.T, enf *Enforcer) http.Handler {
	t.Helper()
	guard := NewGuard(routed(t, legacyStore(false, "member"), &fakeValidator{result: userClaims(nil)}), quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	for pattern, level := range platformAdminGapRoutes {
		if level == Admin {
			mux.Handle(pattern, guard.WrapAdminOr(rule.ActionAdminUsers, echo))
		}
	}
	mux.Handle(noActionPlatformRoute, guard.WrapPlatform(rule.ActionAdminUsers, echo))
	return mux
}

func platformQuestionRoutes(t *testing.T) []string {
	t.Helper()
	rows := productionGoAPIRows(t)
	if _, ok := rows[noActionPlatformRoute]; ok {
		t.Fatalf("%s has a profile row: the probe must be a row with no action", noActionPlatformRoute)
	}
	routes := []string{noActionPlatformRoute}
	for pattern, level := range platformAdminGapRoutes {
		if level == Admin {
			routes = append(routes, pattern)
		}
	}
	return routes
}

// Admin cell on, no decision point client: the platform question has no client to ask. The answer is the no decision
// point 503 with its reason code and no Retry-After, because a retry cannot work until a deploy adds the client.
func TestPlatformQuestionWithNoDecisionPointCarriesTheReasonCode(t *testing.T) {
	unconfigured, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: productionGoAPIRows(t), Switch: onFor("admin"), Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	for _, pattern := range platformQuestionRoutes(t) {
		t.Run(pattern, func(t *testing.T) {
			got := serveCell(platformQuestionHandler(t, unconfigured), pattern, cellBearer())
			if got.status != http.StatusServiceUnavailable || got.body != enforceNoDecisionPointBody || got.header.Get("Retry-After") != "" {
				t.Fatalf("no decision point: %d %q Retry-After %q, want 503 %q and no Retry-After",
					got.status, got.body, got.header.Get("Retry-After"), enforceNoDecisionPointBody)
			}
		})
	}
}

// A handler's own platform question (HoldsPlatform, the telemetry report) with no decision point client gives the
// same answer as the guard's.
func TestHandlerPlatformQuestionWithNoDecisionPointCarriesTheReasonCode(t *testing.T) {
	unconfigured, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: productionGoAPIRows(t), Switch: onFor("admin"), Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	got := seamServe(seamHandler(t, legacyStore(true, "member"), userClaims(nil), unconfigured), http.MethodPost, "/api/v1/telemetry/opt-in")
	if got.status != http.StatusServiceUnavailable || got.body != enforceNoDecisionPointBody || got.header.Get("Retry-After") != "" {
		t.Fatalf("no decision point: %d %q Retry-After %q, want 503 %q and no Retry-After",
			got.status, got.body, got.header.Get("Retry-After"), enforceNoDecisionPointBody)
	}
}

// Admin cell on, a decision point that does not answer: the platform question keeps the PDP-down answer, a 503 with
// Retry-After, byte for byte.
func TestPlatformQuestionWithADeadDecisionPointKeepsTheRetryAnswer(t *testing.T) {
	for _, pattern := range platformQuestionRoutes(t) {
		t.Run(pattern, func(t *testing.T) {
			dead := newTestPDP(t, allowAll)
			deadEnforcer := productionEnforcer(t, dead, "admin")
			dead.srv.Close()
			got := serveCell(platformQuestionHandler(t, deadEnforcer), pattern, cellBearer())
			if got.status != http.StatusServiceUnavailable || got.body != enforceUnavailableBody || got.header.Get("Retry-After") != enforceRetryAfter {
				t.Fatalf("dead PDP: %d %q Retry-After %q, want 503 %q Retry-After %s",
					got.status, got.body, got.header.Get("Retry-After"), enforceUnavailableBody, enforceRetryAfter)
			}
		})
	}
	dead := newTestPDP(t, allowAll)
	deadEnforcer := productionEnforcer(t, dead, "admin")
	dead.srv.Close()
	got := seamServe(seamHandler(t, legacyStore(true, "member"), userClaims(nil), deadEnforcer), http.MethodPost, "/api/v1/telemetry/opt-in")
	if got.status != http.StatusServiceUnavailable || got.body != enforceUnavailableBody || got.header.Get("Retry-After") != enforceRetryAfter {
		t.Fatalf("handler question, dead PDP: %d %q Retry-After %q, want 503 %q Retry-After %s",
			got.status, got.body, got.header.Get("Retry-After"), enforceUnavailableBody, enforceRetryAfter)
	}
}

// The no decision point reason has one writer: the literal is only the constant, and only writeEnforceRefusal reads
// the constant.
func TestTheNoDecisionPointReasonHasOneWriter(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	literal := strconv.Quote(enforceNoDecisionPointReason)
	literals, readers, declared := 0, map[string]int{}, map[*ast.Ident]bool{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			owner := "package level"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner = fn.Name.Name
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.ValueSpec:
					for _, id := range n.Names {
						declared[id] = true
						if id.Name == "enforceNoDecisionPointReason" {
							for _, v := range n.Values {
								ast.Inspect(v, func(m ast.Node) bool {
									if lit, ok := m.(*ast.BasicLit); ok && lit.Value == literal {
										literals--
									}
									return true
								})
							}
						}
					}
				case *ast.BasicLit:
					if n.Value == literal {
						literals++
					}
				case *ast.Ident:
					if n.Name == "enforceNoDecisionPointReason" && !declared[n] {
						readers[owner]++
					}
				}
				return true
			})
		}
	}
	if literals != 0 {
		t.Errorf("%d copies of %s outside its constant, want 0", literals, literal)
	}
	if len(readers) != 1 || readers["writeEnforceRefusal"] == 0 {
		t.Errorf("enforceNoDecisionPointReason is read by %v, want writeEnforceRefusal only", readers)
	}
}
