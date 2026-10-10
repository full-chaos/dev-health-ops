package home

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A serving binary must not pull the job package (CHAOS-9094): the shared compute of
// the work-item metrics is in package workitemmetrics, which has no I/O and no job
// imports. This walks the non-test imports of package home through the module and
// refuses a path to the daily job.
func TestHomeDoesNotDependOnTheDailyJob(t *testing.T) {
	const module = "github.com/full-chaos/dev-health-ops/"
	const forbidden = module + "internal/jobs/metrics/daily"
	root := filepath.Join("..", "..", "..")
	seen := map[string]bool{}
	var walk func(importPath, via string)
	walk = func(importPath, via string) {
		if seen[importPath] || !strings.HasPrefix(importPath, module) {
			return
		}
		seen[importPath] = true
		if importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/") {
			t.Errorf("package home depends on the daily job through %s", via)
			return
		}
		dir := filepath.Join(root, strings.TrimPrefix(importPath, module))
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", filepath.Join(dir, name), err)
			}
			for _, spec := range file.Imports {
				walk(strings.Trim(spec.Path.Value, `"`), via+" -> "+strings.TrimPrefix(importPath, module))
			}
		}
	}
	walk(module+"internal/queryapi/home", "home")
	if len(seen) < 5 {
		t.Fatalf("the walk reached %d packages: it measured nothing", len(seen))
	}
}
