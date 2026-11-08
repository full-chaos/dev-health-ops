package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// CHAOS-12659 Q17: the three listeners' handlers each carry the process's one PDP gate. The internal listener's own
// routes (/query/proof-write, /query/run-operation) and the MCP listener's /query sit on route sets the plane handler
// does not wrap; a nil gate there answers Legacy for every GraphQL operation whatever cell is on.

// Read from server.go: BuildWithLookup hands its three mounted route sets, in mountQueryRouteSets's order, to
// gatedListenerHandlers, and its Plane serves exactly what gatedListenerHandlers returns.
func TestEveryListenerHandlerCarriesTheGate(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var build *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "BuildWithLookup" {
			build = fn
		}
	}
	if build == nil {
		t.Fatal("server.go has no BuildWithLookup")
	}
	identName := func(e ast.Expr) string {
		if id, ok := e.(*ast.Ident); ok {
			return id.Name
		}
		return ""
	}
	var mounts, wired, results []string
	var planes []*ast.CompositeLit
	ast.Inspect(build.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			if identName(node.Fun) == "mountQueryRouteSets" && len(node.Args) >= 4 {
				mounts = []string{identName(node.Args[1]), identName(node.Args[2]), identName(node.Args[3])}
			}
		case *ast.AssignStmt:
			if len(node.Rhs) == 1 && len(node.Lhs) == 4 {
				if call, ok := node.Rhs[0].(*ast.CallExpr); ok && identName(call.Fun) == "gatedListenerHandlers" && len(call.Args) == 4 {
					wired = []string{identName(call.Args[1]), identName(call.Args[2]), identName(call.Args[3])}
					results = []string{identName(node.Lhs[0]), identName(node.Lhs[1]), identName(node.Lhs[2])}
				}
			}
		case *ast.UnaryExpr:
			if lit, ok := node.X.(*ast.CompositeLit); ok && identName(lit.Type) == "Plane" {
				planes = append(planes, lit)
			}
		}
		return true
	})
	if len(mounts) != 3 || mounts[0] == "" || mounts[1] == "" || mounts[2] == "" {
		t.Fatalf("BuildWithLookup: mountQueryRouteSets route sets %v, want three named muxes", mounts)
	}
	if len(wired) != 3 || wired[0] != mounts[0] || wired[1] != mounts[1] || wired[2] != mounts[2] {
		t.Fatalf("BuildWithLookup: gatedListenerHandlers wires %v, want the mounted route sets %v in order", wired, mounts)
	}
	if len(planes) != 1 {
		t.Fatalf("BuildWithLookup builds %d Plane literals, want 1", len(planes))
	}
	fields := map[string]string{}
	for _, elt := range planes[0].Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			fields[identName(kv.Key)] = identName(kv.Value)
		}
	}
	for i, field := range []string{"Handler", "InternalHandler", "MCPHandler"} {
		if results[i] == "" || fields[field] != results[i] {
			t.Errorf("Plane.%s = %q, want gatedListenerHandlers' %q", field, fields[field], results[i])
		}
	}
}

// gatedListenerHandlers serves every route of each set with the gate it built on the request's context: the public set,
// the internal set's own route, the internal set's fallthrough to the public set, and the MCP set.
func TestListenerHandlersPutTheGateOnEveryRoute(t *testing.T) {
	seen := map[string]*pdp.Gate{}
	probe := func(name string) http.HandlerFunc {
		return func(_ http.ResponseWriter, r *http.Request) { seen[name] = pdp.FromContext(r.Context()) }
	}
	mux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	mux.HandleFunc("/public-probe", probe("public"))
	internalMux.HandleFunc("/query/run-operation", probe("internal own route"))
	mcpMux.HandleFunc("/query", probe("mcp"))
	handler, internal, mcp, gate := gatedListenerHandlers(knobs(map[string]string{}), mux, internalMux, mcpMux)
	if gate == nil {
		t.Fatal("gatedListenerHandlers built no gate")
	}
	for _, cell := range []struct {
		name    string
		handler http.Handler
		path    string
		probe   string
	}{
		{"public", handler, "/public-probe", "public"},
		{"internal own route", internal, "/query/run-operation", "internal own route"},
		{"internal fallthrough", internal, "/public-probe", "public"},
		{"mcp", mcp, "/query", "mcp"},
	} {
		clear(seen)
		cell.handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, cell.path, nil))
		got, ran := seen[cell.probe]
		if !ran || got != gate {
			t.Errorf("%s: ran %v, gate on the request %p, want the plane's gate %p", cell.name, ran, got, gate)
		}
	}
}
