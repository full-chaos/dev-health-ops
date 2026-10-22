package llmorgsettings

import (
	"go/ast"
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

// A reader of a stored base_url must reach the SSRF validator through
// CheckBaseURL / ValidateBaseURLForKeys, which refuse a key-bearing url before
// any lookup. This guard is text-based because "every future reader" cannot be
// enumerated by a behaviour test: it fails on any non-test Go file of the root
// module that names ValidateBaseURL or ValidateBaseURLChecked, resolving the
// import name of this package per file (alias and dot imports included), unless
// the file is in the allow-list with its reason. The behaviour table in
// keycheck_test.go proves each known reader; a wrapper in another package that
// re-exports the validator is outside what a text scan can see.

const llmorgsettingsImport = "github.com/full-chaos/dev-health-ops/internal/llmorgsettings"

var guardedValidators = map[string]bool{"ValidateBaseURL": true, "ValidateBaseURLChecked": true, "validateBaseURL": true}

// guardRow is one allow-listed file: why, and exactly how many mentions it may
// hold (0 = the package's own door files, any count), so a second call added to
// an allow-listed file fails the guard.
type guardRow struct {
	reason string
	hits   int
}

// guardAllowList holds every file that may name a guarded validator.
var guardAllowList = map[string]guardRow{
	"internal/llmorgsettings/ssrf.go":     {"defines the validators", 0},
	"internal/llmorgsettings/keycheck.go": {"ValidateBaseURLForKeys is the guarded door: the key compare runs before the validator", 0},
	"internal/apiservice/admin/llmmodels.go": {"handlers.checkBaseURL hands the validator to CheckBaseURL as its validate argument; " +
		"every admin reader goes through checkBaseURL", 1},
	"internal/queryapi/investmentexplain/provider_org.go": {"validateRawBaseURL is called only after StoredBaseURLCarriesKey " +
		"in the fallback log; a key hit logs the refusal class and returns", 1},
}

type guardHit struct {
	file, name string
}

// scanGuardedValidators returns the guarded validator names one file mentions.
func scanGuardedValidators(file string, src []byte, inOwnPackage bool) ([]guardHit, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, src, 0)
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	dot := false
	for _, spec := range parsed.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != llmorgsettingsImport {
			continue
		}
		switch {
		case spec.Name == nil:
			names["llmorgsettings"] = true
		case spec.Name.Name == ".":
			dot = true
		case spec.Name.Name != "_":
			names[spec.Name.Name] = true
		}
	}
	var hits []guardHit
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			if x, ok := n.X.(*ast.Ident); ok && names[x.Name] && guardedValidators[n.Sel.Name] {
				hits = append(hits, guardHit{file, n.Sel.Name})
			}
		case *ast.Ident:
			if (dot || inOwnPackage) && guardedValidators[n.Name] {
				hits = append(hits, guardHit{file, n.Name})
			}
		}
		return true
	})
	return hits, nil
}

func guardRoot(t *testing.T) string {
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
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func TestOnlyAllowListedFilesNameTheBaseURLValidators(t *testing.T) {
	root := guardRoot(t)
	hitFiles := map[string]int{}
	var unlisted []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == root {
				return nil
			}
			name := d.Name()
			if name == "node_modules" || name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				return filepath.SkipDir
			}
			if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(strings.ToLower(string(src)), "validatebaseurl") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		own := strings.HasPrefix(rel, "internal/llmorgsettings/") && !strings.Contains(strings.TrimPrefix(rel, "internal/llmorgsettings/"), "/")
		hits, err := scanGuardedValidators(rel, src, own)
		if err != nil {
			return err
		}
		if len(hits) == 0 {
			return nil
		}
		hitFiles[rel] = len(hits)
		row, ok := guardAllowList[rel]
		if !ok || (row.hits != 0 && row.hits != len(hits)) {
			unlisted = append(unlisted, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(unlisted)
	if len(unlisted) > 0 {
		t.Fatalf("these files name a base_url validator outside the key-checked door (use llmorgsettings.ValidateBaseURLForKeys or CheckBaseURL with the key in hand), or add an allow-list row with a reason, or fix the mention count of a listed file: %v", unlisted)
	}
	for file := range guardAllowList {
		if hitFiles[file] == 0 {
			t.Fatalf("allow-list row %q no longer names a validator; delete it", file)
		}
	}
}

func TestGuardResolvesTheImportNamePerFile(t *testing.T) {
	cases := map[string]struct {
		src     string
		own     bool
		wantHit bool
	}{
		"plain selector": {`package x
import "github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
func f() { llmorgsettings.ValidateBaseURLChecked(nil, "") }`, false, true},
		"alias selector": {`package x
import lo "github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
func f() { lo.ValidateBaseURL(nil, "") }`, false, true},
		"function value": {`package x
import "github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
var v = llmorgsettings.ValidateBaseURLChecked`, false, true},
		"dot import": {`package x
import . "github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
func f() { ValidateBaseURLChecked(nil, "") }`, false, true},
		"own package bare call": {`package llmorgsettings
func f() { ValidateBaseURLChecked(nil, "") }`, true, true},
		"guarded door": {`package x
import "github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
func f() { llmorgsettings.ValidateBaseURLForKeys(nil, "", "k") }`, false, false},
		"another package of that name": {`package x
import "example.com/other/llmorgsettings"
func f() { llmorgsettings.ValidateBaseURLChecked(nil, "") }`, false, false},
		"alias hides the plain name": {`package x
import lo "github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
func f() { llmorgsettings.ValidateBaseURLChecked(nil, "") }`, false, false},
	}
	for name, tc := range cases {
		hits, err := scanGuardedValidators("x.go", []byte(tc.src), tc.own)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if (len(hits) > 0) != tc.wantHit {
			t.Fatalf("%s: hits=%v, want hit=%v", name, hits, tc.wantHit)
		}
	}
}
