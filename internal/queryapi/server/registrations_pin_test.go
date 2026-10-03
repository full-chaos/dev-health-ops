package server

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// handlerRegistrations lists every mux registration call (`<receiver>.Handle(...)` or
// `<receiver>.HandleFunc(...)`) in the non-test source of this package as
// "file: receiver.Method(first argument)". It reads the syntax tree, so a constant pattern, a
// loop variable or a computed one is listed as written, not skipped.
func handlerRegistrations(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "Handle" && selector.Sel.Name != "HandleFunc") {
				return true
			}
			var receiver, argument bytes.Buffer
			_ = printer.Fprint(&receiver, fset, selector.X)
			_ = printer.Fprint(&argument, fset, call.Args[0])
			out = append(out, file+": "+receiver.String()+"."+selector.Sel.Name+"("+argument.String()+")")
			return true
		})
	}
	sort.Strings(out)
	return out
}

// pinnedHandlerRegistrations is the set of mux registrations the query-api has TODAY. The
// REST routes are the restGroups table (walked by the route-profile gate); the rest are the
// GraphQL-document, proof, MCP and operator transports, which have no Python twin and no
// endpoint-profile row yet: a new handler on a query-api mux is seen by no route-profile
// gate, so this list is the only thing that fails when one is added. The follow-up that walks
// them into the gate (rows whose class is read from each guard) retires this list.
var pinnedHandlerRegistrations = []string{
	"buildinfo_route.go: internalMux.HandleFunc(\"/query/proof-write\")",
	"buildinfo_route.go: mux.HandleFunc(\"/query/proof\")",
	"class_row_gate.go: internalMux.HandleFunc(runOperationPath)",
	"graphql_edge_route.go: mux.Handle(graphQLEdgePath)",
	"listener.go: mux.Handle(\"/\")",
	"listener.go: mux.Handle(\"/healthz\")",
	"listener.go: mux.Handle(\"/metrics\")",
	"listener.go: mux.Handle(\"/readyz\")",
	"mcp_proof_route.go: internalMux.Handle(\"/query/proof-mcp\")",
	"operator_compat.go: mux.HandleFunc(\"/healthz\")",
	"operator_compat.go: mux.HandleFunc(\"/metrics\")",
	"operator_compat.go: mux.HandleFunc(\"/readyz\")",
	"server.go: internalMux.Handle(\"/\")",
	"server.go: mcpMux.Handle(\"/query\")",
	"server.go: mux.HandleFunc(\"/api/v1/\")",
	"server.go: mux.HandleFunc(\"/buildinfo\")",
	"server.go: mux.HandleFunc(\"/query\")",
	"server.go: mux.HandleFunc(\"/registry\")",
	"server.go: mux.HandleFunc(mount.Pattern)",
}

func TestTheQueryAPIMuxRegistrationsAreThePinnedSet(t *testing.T) {
	got := handlerRegistrations(t)
	want := append([]string(nil), pinnedHandlerRegistrations...)
	sort.Strings(want)
	have := map[string]bool{}
	for _, entry := range got {
		have[entry] = true
	}
	pinned := map[string]bool{}
	for _, entry := range want {
		pinned[entry] = true
		if !have[entry] {
			t.Errorf("pinned registration is gone: %s (drop it from pinnedHandlerRegistrations)", entry)
		}
	}
	for _, entry := range got {
		if !pinned[entry] {
			t.Errorf("a new mux registration has no pin and no route-profile row: %s (add it to the route-profile gate's walk; the pinned list is the stopgap)", entry)
		}
	}
}
