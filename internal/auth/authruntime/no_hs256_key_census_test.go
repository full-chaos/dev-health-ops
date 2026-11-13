package authruntime

import (
	"errors"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const edgetokenDir = "internal/auth/edgetoken"

// The HS256 token package is gone: its directory does not exist and no Go file, test or not, imports it. auth-service
// and go-api verify no HS256 token, and the token lifetimes and the retired alg are named where they are used.
func TestNoGoFileImportsTheRetiredHS256TokenPackage(t *testing.T) {
	root := "../../.."
	if _, err := os.Stat(filepath.Join(root, edgetokenDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%s still exists (stat error %v)", edgetokenDir, err)
	}
	var bad []string
	fset := token.NewFileSet()
	for _, dir := range []string{"internal", "cmd", "deploy"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == "testdata" {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, spec := range file.Imports {
				if value, _ := strconv.Unquote(spec.Path.Value); value == "github.com/full-chaos/dev-health-ops/"+edgetokenDir {
					bad = append(bad, filepath.ToSlash(rel))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		t.Fatalf("Go files import %s:\n%s", edgetokenDir, strings.Join(bad, "\n"))
	}
}
