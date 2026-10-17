package secrets

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The credential keys the code reads are found in the source tree itself: every
// string literal passed to a Secret(...) call anywhere under internal/ and every value of a
// credentialKeyMap composite literal. Each one that names a secret must be a
// declared credential field, or the short-secret warning would report it as undeclared.
func TestDeclaredCredentialFieldsCoverTheKeysTheCodeReads(t *testing.T) {
	roots := []string{"../.."}
	fset := token.NewFileSet()
	read := map[string]string{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CallExpr:
					selector, ok := typed.Fun.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != "Secret" || len(typed.Args) != 1 {
						return true
					}
					if literal, ok := typed.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING {
						value, _ := strconv.Unquote(literal.Value)
						read[value] = path
					}
				case *ast.ValueSpec:
					for index, name := range typed.Names {
						if name.Name != "credentialKeyMap" || index >= len(typed.Values) {
							continue
						}
						ast.Inspect(typed.Values[index], func(inner ast.Node) bool {
							pair, ok := inner.(*ast.KeyValueExpr)
							if !ok {
								return true
							}
							if literal, ok := pair.Value.(*ast.BasicLit); ok && literal.Kind == token.STRING {
								value, _ := strconv.Unquote(literal.Value)
								read[value] = path
							}
							return true
						})
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(read) < 10 {
		t.Fatalf("found only %d credential keys in the source tree (%v): the walk no longer sees the code that reads them", len(read), read)
	}
	for key, path := range read {
		if !isSecretField(key) {
			continue // a key that holds no secret
		}
		if _, declared := declaredCredentialField(key); !declared {
			t.Errorf("%s reads the secret credential key %q, which is not in declaredCredentialFields", path, key)
		}
	}
}
