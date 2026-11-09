package authruntime

import (
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

// edgetokenImporters are the production files that may still import the HS256 package: go-api's local logout and
// start check (CHAOS-13386 deletes them). auth-service holds no HS256 key.
var edgetokenImporters = map[string]bool{
	"internal/api/session/session.go": true,
	"internal/apiservice/deps.go":     true,
	"internal/apiservice/service.go":  true,
}

// No non-test Go file outside the allow list imports internal/auth/edgetoken: auth-service verifies no HS256 token,
// and the token lifetimes and the retired alg are named where they are used.
func TestNoProductionFileImportsTheHS256TokenPackage(t *testing.T) {
	root := "../../.."
	var bad []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "testdata" {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "internal/auth/edgetoken/") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			if value, _ := strconv.Unquote(spec.Path.Value); value == edgetokenImport && !edgetokenImporters[rel] {
				bad = append(bad, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		t.Fatalf("production files import %s:\n%s", edgetokenImport, strings.Join(bad, "\n"))
	}
}
