package datahealth

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The packages whose code persists a run or unit error category.
var sourceHealthWriterDirs = []string{
	"internal/providersync",
	"internal/syncdispatchruntime",
	"internal/syncreconciler",
	"internal/scheduler/sync",
	"internal/jobs/providerunit",
	"internal/jobruntime",
}

// Codes a writer package names only in a log line, never in a stored result.
var sourceHealthLoggedOnlyCategories = map[string]string{
	"jira_source_not_a_project": "planner log line (scheduler/sync planner.go, backfill_planner.go)",
	"ran_before_query_failed":   "materializer log line",
	"none":                      "the job runtime's no-error value, never a failure",
}

var categoryInText = regexp.MustCompile(`error_category["']?\s*[:,]\s*["']([a-z][a-z0-9_]*)["']`)

// writerCategories reads every category code of the writer packages from
// their source: constants named ...Category, the job runtime's typed
// categories, "error_category" keys in map literals and JSON or SQL text, and
// slog "error_category" attributes.
func writerCategories(t *testing.T) map[string][]string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]string{}
	add := func(code, where string) { found[code] = append(found[code], where) }
	unquote := func(e ast.Expr) (string, bool) {
		lit, ok := e.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return "", false
		}
		v, err := strconv.Unquote(lit.Value)
		return v, err == nil
	}
	for _, dir := range sourceHealthWriterDirs {
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, filepath.Join(root, dir), func(fi fs.FileInfo) bool {
			return !strings.HasSuffix(fi.Name(), "_test.go")
		}, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", dir, err)
		}
		for _, pkg := range pkgs {
			for path, file := range pkg.Files {
				where := func(n ast.Node) string {
					return filepath.Base(path) + ":" + strconv.Itoa(fset.Position(n.Pos()).Line)
				}
				ast.Inspect(file, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.ValueSpec:
						for i, name := range x.Names {
							if i >= len(x.Values) {
								continue
							}
							typed := false
							if id, ok := x.Type.(*ast.Ident); ok && id.Name == "ErrorCategory" {
								typed = true
							}
							if v, ok := unquote(x.Values[i]); ok && (strings.HasSuffix(name.Name, "Category") || typed) {
								add(v, where(x))
							}
						}
					case *ast.KeyValueExpr:
						if k, ok := unquote(x.Key); ok && k == "error_category" {
							if v, ok := unquote(x.Value); ok {
								add(v, where(x))
							}
						}
					case *ast.CallExpr:
						if len(x.Args) == 2 {
							if k, ok := unquote(x.Args[0]); ok && k == "error_category" {
								if v, ok := unquote(x.Args[1]); ok {
									add(v, where(x))
								}
							}
						}
					case *ast.BasicLit:
						if x.Kind == token.STRING {
							if text, err := strconv.Unquote(x.Value); err == nil {
								for _, m := range categoryInText.FindAllStringSubmatch(text, -1) {
									add(m[1], where(x))
								}
							}
						}
					}
					return true
				})
			}
		}
	}
	return found
}

func TestSourceHealthStagesCoverEveryWriter(t *testing.T) {
	found := writerCategories(t)

	// The census must see the writers it exists for.
	for _, code := range []string{"worker_lost", "worker_lost_retry_exhausted", "terminal_river_delivery", "budget_deferred",
		"provider_dataset_unavailable", "provider_rate_limited", "feature_disabled", "auth", "timeout", "dispatch_denied", "pagerduty_sync_disabled"} {
		if _, ok := found[code]; !ok {
			t.Errorf("census did not find %q: the scan no longer reaches its writer", code)
		}
	}
	if len(found) < 40 {
		t.Fatalf("census found only %d codes: it measures too little", len(found))
	}

	var missing []string
	for code, where := range found {
		if _, logged := sourceHealthLoggedOnlyCategories[code]; logged {
			if sourceHealthStages[code] {
				t.Errorf("%q is listed as log-only and as a stage", code)
			}
			continue
		}
		if !sourceHealthStages[code] {
			missing = append(missing, code+" ("+strings.Join(where[:1], "")+")")
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("a writer persists categories that sourceHealthStages does not name (they would read \"other\"): %s", strings.Join(missing, ", "))
	}
	for code := range sourceHealthLoggedOnlyCategories {
		if _, ok := found[code]; !ok {
			t.Errorf("log-only entry %q no longer appears in a writer: remove it", code)
		}
	}
	for code := range sourceHealthStages {
		if _, ok := found[code]; !ok {
			t.Errorf("stage %q is named but no writer package persists it: remove it or widen the scan", code)
		}
	}
}
