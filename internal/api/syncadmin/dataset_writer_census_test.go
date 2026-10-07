package syncadmin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// datasetStateWriters is every Go function of the production tree that
// executes a statement able to set integration_datasets.is_enabled: an
// INSERT into the table, or an UPDATE that sets the column. The dataset row
// is the single owner of "which datasets run" (CHAOS-8816), so a new writer
// is a design change: it is added here on purpose, with its reason, or not
// at all. Options-only UPDATEs do not set the column and are not writers.
var datasetStateWriters = map[string]string{
	"internal/api/syncadmin.applySelectionChange":              "the config save: the rows of the targets the user checked or unchecked",
	"internal/api/syncadmin.createPlannerManagedConfig":        "the config create: the rows of the new integration's selected targets",
	"internal/api/syncadmin.repairPagerDutyDatasets":           "the config create for PagerDuty: the operational set on, every other row off",
	"internal/api/integrationsadmin.applyDataset":              "the integration dataset endpoint: one row created or switched",
	"internal/scheduler/sync.ensureExplicitlyRequestedDataset": "plan time: an explicitly requested key that has no row gets an enabled row",
	"internal/scheduler/sync.applyDomainPlanMutations":         "plan time: the automatic security row (insert only) and the PagerDuty repair",
	"internal/fixturescli.Seed":                                "the fixtures verb: an enabled row for the synthetic dataset when missing",
}

var datasetStateWrite = regexp.MustCompile(`(?is)\binsert\s+into\s+(public\.)?integration_datasets\b|\bupdate\s+(public\.)?integration_datasets\s+set\b[^;]*\bis_enabled\b`)

// datasetStateWriterFunctions scans the Go files under roots (no _test.go
// file) and returns "<package dir>.<function>" for every function that holds
// a string literal matching datasetStateWrite, and the number of files read.
func datasetStateWriterFunctions(t *testing.T, repoRoot string, roots ...string) ([]string, int) {
	t.Helper()
	found := map[string]bool{}
	files := 0
	for _, root := range roots {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files++
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repoRoot, filepath.Dir(path))
			if err != nil {
				return err
			}
			for _, declaration := range parsed.Decls {
				name := "<package level>"
				if function, ok := declaration.(*ast.FuncDecl); ok {
					name = function.Name.Name
				}
				ast.Inspect(declaration, func(node ast.Node) bool {
					literal, ok := node.(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING {
						return true
					}
					text, err := strconv.Unquote(literal.Value)
					if err == nil && datasetStateWrite.MatchString(text) {
						found[filepath.ToSlash(relative)+"."+name] = true
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	out := make([]string, 0, len(found))
	for name := range found {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, files
}

// TestEveryWriterOfTheDatasetStateIsNamed is the writer census: the
// functions that can set integration_datasets.is_enabled are exactly the
// named list. A scan that read no file, or found no writer, fails: it
// measured nothing.
func TestEveryWriterOfTheDatasetStateIsNamed(t *testing.T) {
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("no caller file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	got, files := datasetStateWriterFunctions(t, repoRoot, "internal", "cmd")
	if files < 100 || len(got) == 0 {
		t.Fatalf("the scan read %d files and found %d writers: it measured nothing", files, len(got))
	}
	want := make([]string, 0, len(datasetStateWriters))
	for name, reason := range datasetStateWriters {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("writer %s has no reason", name)
		}
		want = append(want, name)
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the functions that can set integration_datasets.is_enabled changed.\n got  %v\n want %v\n"+
			"A new writer needs a decision: the dataset row has one owner (CHAOS-8816).", got, want)
	}
}

// TestDatasetStateWritePatternMatchesTheStatementsItMustFind keeps the scan
// honest: the pattern finds each statement shape in use and ignores an
// options-only UPDATE and a read.
func TestDatasetStateWritePatternMatchesTheStatementsItMustFind(t *testing.T) {
	for text, want := range map[string]bool{
		"INSERT INTO integration_datasets (id, org_id) VALUES ($1, $2)":                               true,
		"\nINSERT INTO public.integration_datasets (id,org_id,integration_id,dataset_key,is_enabled)": true,
		"UPDATE integration_datasets SET is_enabled = false\nWHERE org_id = $1":                       true,
		"UPDATE public.integration_datasets SET is_enabled=$2 WHERE id=$1 RETURNING ":                 true,
		"UPDATE integration_datasets SET is_enabled = true, options = $2::json WHERE id = $1":         true,
		"UPDATE integration_datasets SET options = $2::json WHERE id = $1":                            false,
		"SELECT dataset_key FROM integration_datasets WHERE is_enabled IS true":                       false,
		"UPDATE integration_sources SET is_enabled = false WHERE id = $1":                             false,
		"DELETE FROM integration_datasets WHERE integration_id = $1":                                  false,
	} {
		if got := datasetStateWrite.MatchString(text); got != want {
			t.Errorf("%q: match %v, want %v", text, got, want)
		}
	}
}
