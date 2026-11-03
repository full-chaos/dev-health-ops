package pushprojection

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The consumer cannot validate or mutate a credential: neither projection
// package imports the customer-push domain or the public store, and neither
// names its token table.
func TestTheProjectionPackagesHaveNoPathToTheDomainStore(t *testing.T) {
	forbiddenImports := []string{
		"github.com/full-chaos/dev-health-ops/internal/apiservice",
		"github.com/full-chaos/dev-health-ops/internal/storage/postgres/publicstore",
	}
	for _, dir := range []string{".", filepath.Join("..", "outboxprojection")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		checked := 0
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			checked++
			file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range file.Imports {
				imported, _ := strconv.Unquote(spec.Path.Value)
				for _, bad := range forbiddenImports {
					if strings.HasPrefix(imported, bad) {
						t.Errorf("%s imports %s", path, imported)
					}
				}
			}
			for _, word := range []string{"external_ingest_tokens", "public.", "customerpush"} {
				if strings.Contains(string(data), word) {
					t.Errorf("%s names %q", path, word)
				}
			}
		}
		if checked == 0 {
			t.Fatalf("no source file found in %s; the guard would be vacuous", dir)
		}
	}
}
