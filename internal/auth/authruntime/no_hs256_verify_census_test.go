package authruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// hs256VerifyAllowed is the one production file that may verify an HS256 JWT: the GitHub App install state, not an
// access token.
var hs256VerifyAllowed = map[string]bool{"internal/api/githubapp/state.go": true}

// No production file verifies an HS256 access token (CHAOS-13197; the HS256 token package internal/auth/edgetoken is
// deleted, CHAOS-13415): no file declares or calls CheckClaims (the users-row half of the HS256 access verify), and no
// file outside the allow list accepts HS256 in jwt.WithValidMethods.
func TestNoProductionFileVerifiesAnHS256AccessToken(t *testing.T) {
	root := "../../.."
	var bad []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if strings.Contains(rel, "/testsupport/") || strings.Contains(rel, "/testdata/") {
				return nil
			}
			found, err := hs256AccessVerifySites(path, rel)
			if err != nil {
				return err
			}
			bad = append(bad, found...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		t.Fatalf("production HS256 access-token verify paths remain:\n%s", strings.Join(bad, "\n"))
	}
}

func hs256AccessVerifySites(path, rel string) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var bad []string
	site := func(node ast.Node, what string) {
		bad = append(bad, rel+":"+strconv.Itoa(fset.Position(node.Pos()).Line)+": "+what)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncDecl:
			if n.Name.Name == "CheckClaims" {
				site(n, "declares CheckClaims")
			}
		case *ast.CallExpr:
			selector, ok := n.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch name := selector.Sel.Name; {
			case name == "CheckClaims":
				site(n, "calls CheckClaims")
			case name == "WithValidMethods" && !hs256VerifyAllowed[rel] && namesHS256(n):
				site(n, "accepts HS256 in jwt.WithValidMethods")
			}
		}
		return true
	})
	return bad, nil
}

func namesHS256(call *ast.CallExpr) bool {
	found := false
	ast.Inspect(call, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			if value, _ := strconv.Unquote(literal.Value); value == "HS256" {
				found = true
			}
		}
		return !found
	})
	return found
}
