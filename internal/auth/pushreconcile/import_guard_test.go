package pushreconcile

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// C15: the pure core has no path to the domain store, the public store or a database,
// reads no clock and writes nothing.
func TestC15ThePureCoreHasNoIOAndNoPathToTheDomainStore(t *testing.T) {
	forbidden := []string{
		"github.com/full-chaos/dev-health-ops/internal/apiservice",
		"github.com/full-chaos/dev-health-ops/internal/storage/postgres",
		"github.com/jackc/pgx",
		"database/sql",
		"net/http",
		"os",
		"context",
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		checked++
		file, err := parser.ParseFile(token.NewFileSet(), name, data, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			imported, _ := strconv.Unquote(spec.Path.Value)
			for _, bad := range forbidden {
				if imported == bad || strings.HasPrefix(imported, bad+"/") {
					t.Errorf("%s imports %s", name, imported)
				}
			}
		}
		for _, word := range []string{"time.Now", "external_ingest_tokens", "customerpush", "INSERT ", "UPDATE ", "DELETE "} {
			if strings.Contains(string(data), word) {
				t.Errorf("%s names %q", name, word)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no source file found; the guard would be vacuous")
	}
}
