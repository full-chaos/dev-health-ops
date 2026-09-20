package graph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// notImplementedDir holds one empty file per resolver whose body is a gqlgen
// "not implemented" stub, named "Receiver.Method". One file per name (not one
// shared list) so that porting a resolver -- deleting its own file -- never
// edits a line another port also edits, which is what let parallel ports merge
// without conflicts. A resolver that gains a real body must delete its file; a
// stub with no file fails the test, so an unbuilt field can never reach a
// router unnoticed.
const notImplementedDir = "testdata/not_implemented"

// notImplementedResolverNames lists the names the directory holds.
func notImplementedResolverNames(t *testing.T) []string {
	t.Helper()
	return namesInDir(t, notImplementedDir)
}

func namesInDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// stubResolvers returns "Receiver.Method" for every method in
// schema.resolvers.go whose body is a single panic(...) call carrying a
// "not implemented" message.
func stubResolvers(t *testing.T) map[string]struct{} {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "schema.resolvers.go", nil, 0)
	if err != nil {
		t.Fatalf("parse schema.resolvers.go: %v", err)
	}
	stubs := map[string]struct{}{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil || len(fn.Recv.List) != 1 {
			continue
		}
		if len(fn.Body.List) != 1 {
			continue
		}
		stmt, ok := fn.Body.List[0].(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := stmt.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "panic" {
			continue
		}
		if !callMentionsNotImplemented(call) {
			continue
		}
		recv := fn.Recv.List[0].Type
		if star, ok := recv.(*ast.StarExpr); ok {
			recv = star.X
		}
		recvIdent, ok := recv.(*ast.Ident)
		if !ok {
			continue
		}
		stubs[recvIdent.Name+"."+fn.Name.Name] = struct{}{}
	}
	return stubs
}

func callMentionsNotImplemented(call *ast.CallExpr) bool {
	found := false
	ast.Inspect(call, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && strings.Contains(lit.Value, "not implemented") {
			found = true
		}
		return !found
	})
	return found
}

func TestNotImplementedResolversMatchAllowlist(t *testing.T) {
	allowed := notImplementedResolverNames(t)
	notImplementedResolvers := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		notImplementedResolvers[name] = struct{}{}
	}
	stubs := stubResolvers(t)
	if len(stubs) == 0 && len(notImplementedResolvers) > 0 {
		t.Fatal("parsed no stub resolvers; the detector no longer recognises the stub shape")
	}

	var unlisted, built []string
	for name := range stubs {
		if _, ok := notImplementedResolvers[name]; !ok {
			unlisted = append(unlisted, name)
		}
	}
	for name := range notImplementedResolvers {
		if _, ok := stubs[name]; !ok {
			built = append(built, name)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(built)
	if len(unlisted) > 0 {
		t.Errorf("resolvers with a not-implemented body that have no file in %s: %v", notImplementedDir, unlisted)
	}
	if len(built) > 0 {
		t.Errorf("%s names resolvers that are built or absent; delete these files: %v", notImplementedDir, built)
	}
}
