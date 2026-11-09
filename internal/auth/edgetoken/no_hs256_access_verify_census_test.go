package edgetoken

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

const edgetokenImport = "github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"

// hs256VerifyAllowed is the one production file that may verify an HS256 JWT: the GitHub App install state, not an
// access token.
var hs256VerifyAllowed = map[string]bool{"internal/api/githubapp/state.go": true}

// verifierMethods is the whole method set of edgetoken.Verifier: the legacy refresh token logout still revokes
// (CHAOS-13125). An access verify method on it would be an HS256 access-token verify path.
var verifierMethods = map[string]bool{"VerifyRefresh": true}

// No production file verifies an HS256 access token (CHAOS-13197): edgetoken.Verifier verifies refresh tokens only, no
// file that imports edgetoken calls a Verify or VerifyType method, no file declares or calls CheckClaims (the users-row
// half of the HS256 access verify), and no file outside the allow list accepts HS256 in jwt.WithValidMethods.
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
	importsEdgetoken := false
	for _, spec := range file.Imports {
		if value, _ := strconv.Unquote(spec.Path.Value); value == edgetokenImport {
			importsEdgetoken = true
		}
	}
	inEdgetoken := strings.HasPrefix(rel, "internal/auth/edgetoken/") && !strings.Contains(strings.TrimPrefix(rel, "internal/auth/edgetoken/"), "/")
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
			if inEdgetoken && n.Recv != nil && receiverIs(n.Recv, "Verifier") && !verifierMethods[n.Name.Name] {
				site(n, "edgetoken.Verifier method "+n.Name.Name)
			}
		case *ast.CallExpr:
			selector, ok := n.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch name := selector.Sel.Name; {
			case name == "CheckClaims":
				site(n, "calls CheckClaims")
			case importsEdgetoken && (name == "Verify" || name == "VerifyType"):
				site(n, "calls "+name+" in a file that imports edgetoken")
			case name == "WithValidMethods" && !hs256VerifyAllowed[rel] && namesHS256(n):
				site(n, "accepts HS256 in jwt.WithValidMethods")
			}
		}
		return true
	})
	return bad, nil
}

func receiverIs(fields *ast.FieldList, name string) bool {
	for _, field := range fields.List {
		expr := field.Type
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		if ident, ok := expr.(*ast.Ident); ok && ident.Name == name {
			return true
		}
	}
	return false
}

func namesHS256(call *ast.CallExpr) bool {
	found := false
	ast.Inspect(call, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			if value, _ := strconv.Unquote(literal.Value); value == Algorithm {
				found = true
			}
		}
		return !found
	})
	return found
}
