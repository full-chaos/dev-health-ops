package faultproxy

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

// The fault proxy is a test tool: no other Go package imports it, and no
// image, release or chart file names it.
func TestFaultProxyIsNeverShipped(t *testing.T) {
	root := repoRoot(t)
	own := []string{filepath.Join(root, "internal", "faultproxy"), filepath.Join(root, "cmd", "faultproxy")}
	skip := map[string]bool{".git": true, "node_modules": true, "third_party": true, ".build": true}
	fset := token.NewFileSet()
	var goFiles int
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skip[d.Name()] {
				return filepath.SkipDir
			}
			for _, o := range own {
				if path == o {
					return filepath.SkipDir
				}
			}
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if strings.HasSuffix(path, ".go") {
			goFiles++
			f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if perr != nil {
				return nil
			}
			for _, imp := range f.Imports {
				p, _ := strconv.Unquote(imp.Path.Value)
				if strings.HasSuffix(p, "/internal/faultproxy") {
					t.Errorf("%s imports the fault proxy", rel)
				}
			}
			return nil
		}
		base := d.Name()
		shipping := strings.HasPrefix(base, "Dockerfile") || strings.HasPrefix(base, ".goreleaser") ||
			strings.HasPrefix(rel, "deploy"+string(filepath.Separator)) || strings.HasPrefix(rel, "docker"+string(filepath.Separator))
		if !shipping {
			return nil
		}
		if data, rerr := os.ReadFile(path); rerr == nil && strings.Contains(string(data), "faultproxy") {
			t.Errorf("%s names the fault proxy", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if goFiles < 100 {
		t.Fatalf("walked only %d Go files; the guard is not looking at the repo", goFiles)
	}
}
