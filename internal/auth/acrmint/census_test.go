package acrmint

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const acrmintImportPath = "github.com/full-chaos/dev-health-ops/internal/auth/acrmint"

// newReferences is the selector references to acrmint.New in one file's source,
// under whatever name the file imports the package. A selector catches a function
// value as well as a call.
func newReferences(t *testing.T, name string, source any) int {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	local := ""
	for _, spec := range parsed.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if path == acrmintImportPath {
			local = "acrmint"
			if spec.Name != nil {
				local = spec.Name.Name
			}
		}
	}
	if local == "" {
		return 0
	}
	found := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "New" {
			if x, ok := selector.X.(*ast.Ident); ok && x.Name == local {
				found++
			}
		}
		return true
	})
	return found
}

// productionNewReferences is every non-test Go file of the module, outside this
// package, that references acrmint.New. The walk fails loudly when it reads no file,
// so an empty answer is never a walk that saw nothing.
func productionNewReferences(t *testing.T, root string) []string {
	t.Helper()
	var references []string
	files := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "testdata", "vendor", ".venv", "third_party":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.Dir(rel) == filepath.Join("internal", "auth", "acrmint") {
			return nil
		}
		files++
		if newReferences(t, path, nil) > 0 {
			references = append(references, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 1000 {
		t.Fatalf("the walk read %d Go files under %s; the census needs the whole module", files, root)
	}
	return references
}

// The census counts a call, a function value and an aliased import, and an import
// of the package with no reference to New is not one: O3 imports it for
// PGImpersonation.
func TestTheCensusFindsEveryFormOfAReferenceToNew(t *testing.T) {
	for name, tc := range map[string]struct {
		source string
		want   int
	}{
		"a call":           {`package p; import "` + acrmintImportPath + `"; var _, _ = acrmint.New(acrmint.Config{})`, 1},
		"a function value": {`package p; import "` + acrmintImportPath + `"; var f = acrmint.New`, 1},
		"an alias":         {`package p; import m "` + acrmintImportPath + `"; var f = m.New`, 1},
		"the checker":      {`package p; import "` + acrmintImportPath + `"; var c = acrmint.PGImpersonation{}`, 0},
		"another New":      {`package p; import "errors"; var e = errors.New("x")`, 0},
	} {
		if got := newReferences(t, name, tc.source); got != tc.want {
			t.Errorf("%s: %d references, want %d", name, got, tc.want)
		}
	}
}

// TestTheACRMintIsDarkAndItsClassStaysUnconsumed is acp's condition 6 on the O2 plan,
// as restated on the O3 plan (Q5): before O4, no production file references
// acrmint.New and the user_access_token class row stays "unconsumed". O4's token
// endpoint changes both, and this census, in one change: a reference alone, or a
// status flip alone, fails here. O3 imports the package for the actor check, so an
// import is not a reference.
func TestTheACRMintIsDarkAndItsClassStaysUnconsumed(t *testing.T) {
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	if references := productionNewReferences(t, root); len(references) != 0 {
		t.Errorf("production files reference acrmint.New before O4: %v (O4 updates this census and the class row together)", references)
	}
	raw, err := os.ReadFile(filepath.Join(contracts.ContractsDir(root), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Status  string `json:"status"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, class := range registry.Classes {
		if class.ClassID == contracts.UserAccessTokenClass {
			found = true
			if class.Status != "unconsumed" {
				t.Errorf("user_access_token status %q before O4; want unconsumed", class.Status)
			}
		}
	}
	if !found {
		t.Fatal("the registry has no user_access_token row")
	}
}
