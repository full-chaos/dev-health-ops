package acrmint

import (
	"encoding/json"
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

// productionImporters is every non-test Go file of the module, outside this
// package, that imports acrmint. The walk fails loudly when it reads no file,
// so an empty answer is never a walk that saw nothing.
func productionImporters(t *testing.T, root string) []string {
	t.Helper()
	var importers []string
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
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range parsed.Imports {
			if p, _ := strconv.Unquote(spec.Path.Value); p == acrmintImportPath {
				importers = append(importers, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 1000 {
		t.Fatalf("the walk read %d Go files under %s; the census needs the whole module", files, root)
	}
	return importers
}

// TestTheACRMintIsDarkAndItsClassStaysUnconsumed is acp's condition 6 on the
// O2 plan: until the authorization-server token endpoint (CHAOS-12082 step O3)
// constructs a Minter, no production file imports this package and the
// user_access_token class row stays "unconsumed". O3 changes both, and this
// census, in one change: a production importer alone, or a status flip alone,
// fails here.
func TestTheACRMintIsDarkAndItsClassStaysUnconsumed(t *testing.T) {
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	if importers := productionImporters(t, root); len(importers) != 0 {
		t.Errorf("production files import acrmint before O3: %v (O3 updates this census and the class row together)", importers)
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
				t.Errorf("user_access_token status %q before O3; want unconsumed", class.Status)
			}
		}
	}
	if !found {
		t.Fatal("the registry has no user_access_token row")
	}
}
