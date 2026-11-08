package shadowvenue

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// requestCalls are the calls that build or send an HTTP request.
var requestCalls = []string{"NewRequest", "NewRequestWithContext", "Do", "Get", "Post", "PostForm", "Head", "RoundTrip"}

// Every request the venue driver sends is built in Driver.Login or Driver.send, on the origin Driver.origin returns
// (CheckVenueBase), and nowhere else in the driver or its command: no request path keeps a check of its own.
func TestEveryDriverRequestTakesItsOriginFromCheckVenueBase(t *testing.T) {
	var sites []string
	for _, dir := range []string{".", filepath.Join("..", "..", "cmd", "shadow-venue")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				usesOrigin := false
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "origin" {
							usesOrigin = true
						}
					}
					return true
				})
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || !slices.Contains(requestCalls, sel.Sel.Name) {
						return true
					}
					site := name + ":" + fn.Name.Name + ":" + sel.Sel.Name
					sites = append(sites, site)
					if strings.HasPrefix(sel.Sel.Name, "NewRequest") {
						if !usesOrigin || !urlFromOrigin(call) {
							t.Errorf("%s builds a request whose URL is not origin.String()+... from Driver.origin", site)
						}
					}
					return true
				})
			}
		}
	}
	slices.Sort(sites)
	want := []string{"drive.go:Login:Do", "drive.go:Login:NewRequestWithContext", "drive.go:send:Do", "drive.go:send:NewRequestWithContext"}
	if strings.Join(sites, " ") != strings.Join(want, " ") {
		t.Fatalf("request sites\n got %v\nwant %v", sites, want)
	}
}

// urlFromOrigin reports whether the URL argument of a NewRequest call is origin.String() + something.
func urlFromOrigin(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		bin, ok := arg.(*ast.BinaryExpr)
		if !ok || bin.Op != token.ADD {
			continue
		}
		left, ok := bin.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := left.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "String" {
			continue
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == "origin" {
			return true
		}
	}
	return false
}
