package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// CHAOS-12659 Q17: the three listeners' handlers each carry the process's one PDP gate. The internal listener's own
// routes (/query/proof-write, /query/run-operation) and the MCP listener's /query sit on route sets the plane handler
// does not wrap, so BuildWithLookup must hand both out through withGate; a nil gate there answers Legacy for every
// GraphQL operation whatever cell is on. Read from server.go: the route sets are the ones mountQueryRouteSets mounts,
// the public set is the one gatedPlaneHandler wraps, and the Plane literal hands out each wrapped once.
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
	var mounts []string
	var handler, gate, gatedMux string
	var planes []*ast.CompositeLit
	ast.Inspect(build.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			if identName(node.Fun) == "mountQueryRouteSets" && len(node.Args) >= 4 {
				mounts = []string{identName(node.Args[1]), identName(node.Args[2]), identName(node.Args[3])}
			}
		case *ast.AssignStmt:
			if len(node.Lhs) == 2 && len(node.Rhs) == 1 {
				if call, ok := node.Rhs[0].(*ast.CallExpr); ok && identName(call.Fun) == "gatedPlaneHandler" && len(call.Args) == 2 {
					handler, gate, gatedMux = identName(node.Lhs[0]), identName(node.Lhs[1]), identName(call.Args[1])
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
	if handler == "" || gate == "" || gatedMux != mounts[0] {
		t.Fatalf("BuildWithLookup: gatedPlaneHandler wraps %q into (%q, %q), want the public set %q", gatedMux, handler, gate, mounts[0])
	}
	if len(planes) != 1 {
		t.Fatalf("BuildWithLookup builds %d Plane literals, want 1", len(planes))
	}
	fields := map[string]ast.Expr{}
	for _, elt := range planes[0].Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			fields[identName(kv.Key)] = kv.Value
		}
	}
	if got := identName(fields["Handler"]); got != handler {
		t.Errorf("Plane.Handler = %q, want the plane handler %q", got, handler)
	}
	for field, mux := range map[string]string{"InternalHandler": mounts[1], "MCPHandler": mounts[2]} {
		call, ok := fields[field].(*ast.CallExpr)
		if !ok || identName(call.Fun) != "withGate" || len(call.Args) != 2 || identName(call.Args[0]) != mux || identName(call.Args[1]) != gate {
			t.Errorf("Plane.%s is not withGate(%s, %s): the listener's own routes run with no PDP gate on their context", field, mux, gate)
		}
	}
}
