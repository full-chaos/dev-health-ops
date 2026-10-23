package credshape

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

const modulePath = "github.com/full-chaos/dev-health-ops"

func repoRoot(t testing.TB) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// Detectors that redact or sanitize text for logs and error strings. They do
// not refuse a value and use their own shape sets; each is listed with the
// reason it is not this detector.
var redactionDetectors = map[string]string{
	"internal/platform/logging/text.go":                 "log redaction, own vendor-key set",
	"internal/platform/errortext/matchers.go":           "error text redaction",
	"internal/jobs/investment/categorize/errors.go":     "LLM error text redaction",
	"internal/testsupport/venueoracle/golden_tokens.go": "frozen-oracle golden token check",
	"internal/pythonparity/errorsanitize.go":            "frozen Python parity port",
}

func TestOneCredentialShapeImplementation(t *testing.T) {
	scanner := defaultScanner(t)
	needles := []string{"eyJ", "-----BEGIN", "-----BEGIN "}
	for _, shape := range scanner.Shapes() {
		needles = append(needles, shape.Prefix)
	}
	root := repoRoot(t)
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && (entry.Name() == "testdata" || entry.Name() == "node_modules") {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			ast.Inspect(file, func(node ast.Node) bool {
				lit, ok := node.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				text, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				for _, needle := range needles {
					if strings.HasPrefix(text, needle) || (len(needle) >= 4 && strings.Contains(text, needle)) {
						found[rel] = true
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var unexpected []string
	for rel := range found {
		if strings.HasPrefix(rel, "internal/platform/credshape/") {
			continue
		}
		if _, ok := redactionDetectors[rel]; ok {
			continue
		}
		unexpected = append(unexpected, rel)
	}
	sort.Strings(unexpected)
	if len(unexpected) > 0 {
		t.Fatalf("credential shape literals outside credshape: %v", unexpected)
	}
	for rel := range redactionDetectors {
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			t.Errorf("exemption %s: %v", rel, err)
		}
	}
}

func TestCredshapeImportsNothingFromCigates(t *testing.T) {
	root := repoRoot(t)
	seen := map[string]bool{}
	var visit func(dir string, chain []string)
	visit = func(dir string, chain []string) {
		if seen[dir] {
			return
		}
		seen[dir] = true
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		fset := token.NewFileSet()
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				continue
			}
			file, err := parser.ParseFile(fset, filepath.Join(dir, entry.Name()), nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range file.Imports {
				path, _ := strconv.Unquote(spec.Path.Value)
				if !strings.HasPrefix(path, modulePath+"/") {
					continue
				}
				rel := strings.TrimPrefix(path, modulePath+"/")
				if strings.HasPrefix(rel, "internal/cigates") {
					t.Errorf("credshape reaches %s via %v", rel, append(chain, rel))
					continue
				}
				visit(filepath.Join(root, filepath.FromSlash(rel)), append(chain, rel))
			}
		}
	}
	visit(filepath.Join(root, "internal", "platform", "credshape"), nil)
}
