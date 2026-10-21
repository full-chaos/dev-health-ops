package token

import (
	"errors"
	"go/parser"
	gotoken "go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/full-chaos/dev-health-ops"

// keystoreImportViolations lists the Go files under root, outside
// internal/token and internal/auth/keystore, that import the keystore package.
func keystoreImportViolations(root string) ([]string, error) {
	keystorePath := module + "/internal/auth/keystore"
	allowed := []string{"internal/token", "internal/auth/keystore"}
	var violations []string
	files := gotoken.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		parsed, err := parser.ParseFile(files, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for _, spec := range parsed.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil || imported != keystorePath {
				continue
			}
			inside := false
			for _, prefix := range allowed {
				if strings.HasPrefix(rel, prefix+"/") {
					inside = true
				}
			}
			if !inside {
				violations = append(violations, rel)
			}
		}
		return nil
	})
	return violations, err
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test directory")
		}
		dir = parent
	}
}

// TestOnlyTokenImportsKeystore: ACP-ADR-02 §1, no caller outside
// internal/token reaches key custody.
func TestOnlyTokenImportsKeystore(t *testing.T) {
	violations, err := keystoreImportViolations(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range violations {
		t.Errorf("%s imports internal/auth/keystore; only internal/token may", path)
	}
}

// TestTheImportBoundaryRefusesAPlantedImport: the scan finds a direct keystore
// import outside internal/token and admits the two allowed trees.
func TestTheImportBoundaryRefusesAPlantedImport(t *testing.T) {
	root := t.TempDir()
	plant := func(rel, source string) {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	importing := func(pkg string) string {
		return "package " + pkg + "\n\nimport _ \"" + module + "/internal/auth/keystore\"\n"
	}
	plant("internal/auth/authruntime/planted.go", importing("authruntime"))
	plant("cmd/tool/planted_test.go", importing("main"))
	plant("internal/token/allowed.go", importing("token"))
	plant("internal/auth/keystore/allowed_test.go", importing("keystore"))
	plant("internal/tokenizer/planted.go", importing("tokenizer"))
	plant("internal/node_modules/evil/evil.go", importing("evil"))
	violations, err := keystoreImportViolations(root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"internal/auth/authruntime/planted.go": true,
		"cmd/tool/planted_test.go":             true,
		"internal/tokenizer/planted.go":        true,
		"internal/node_modules/evil/evil.go":   true,
	}
	if len(violations) != len(want) {
		t.Fatalf("violations %v, want %v", violations, want)
	}
	for _, path := range violations {
		if !want[path] {
			t.Errorf("unexpected violation %s", path)
		}
	}
}
