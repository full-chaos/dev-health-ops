package restorefence

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

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The push projection reader is an outbox consumer that starts no audit.Publisher:
// a file that builds one (pushprojection.NewReader) must register a fence floor
// input in the same file, so the census cannot be satisfied by another file of
// the package.
func TestEveryPushProjectionReaderHasAFenceFloorInputInTheSameFile(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	if got := unpairedReaderFiles(t, filepath.Join(root, "internal")); len(got) > 0 {
		t.Fatalf("these files build a push projection reader without restorefence.RegisterFloorInput in the same file: %v", got)
	}
}

func TestTheReaderCensusSeesAReaderWithoutAnInput(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const imp = "import (\n\t\"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection\"\n\t\"github.com/full-chaos/dev-health-ops/internal/auth/restorefence\"\n)\n"
	write("lone/lone.go", "package lone\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection\"\n\nfunc start() { _, _ = pushprojection.NewReader(pushprojection.Config{}) }\n")
	write("paired/paired.go", "package paired\n\n"+imp+"\nfunc start() {\n\t_, _ = pushprojection.NewReader(pushprojection.Config{})\n\trestorefence.RegisterFloorInput(nil)\n}\n")
	write("split/a.go", "package split\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection\"\n\nfunc start() { _, _ = pushprojection.NewReader(pushprojection.Config{}) }\n")
	write("split/b.go", "package split\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/restorefence\"\n\nfunc reg() { restorefence.RegisterFloorInput(nil) }\n")
	write("mention/m.go", "package mention\n\n// pushprojection.NewReader is only named here.\nvar _ = \"NewReader\"\n")
	got := unpairedReaderFiles(t, root)
	for i := range got {
		got[i] = filepath.ToSlash(got[i])
	}
	if strings.Join(got, ",") != "lone/lone.go,split/a.go" {
		t.Fatalf("unpaired reader files = %v, want lone/lone.go and split/a.go", got)
	}
}

const pushProjectionImportPath = "github.com/full-chaos/dev-health-ops/internal/auth/pushprojection"

// unpairedReaderFiles returns the non-test files under root that call
// pushprojection.NewReader and do not call restorefence.RegisterFloorInput.
func unpairedReaderFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(data), "NewReader") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
		if err != nil {
			return nil
		}
		names := map[string]string{}
		for _, spec := range file.Imports {
			p, _ := strconv.Unquote(spec.Path.Value)
			name := p[strings.LastIndex(p, "/")+1:]
			if spec.Name != nil {
				name = spec.Name.Name
			}
			names[name] = p
		}
		builds, registers := false, false
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch {
			case sel.Sel.Name == "NewReader" && names[id.Name] == pushProjectionImportPath:
				builds = true
			case sel.Sel.Name == "RegisterFloorInput" && names[id.Name] == restorefenceImportPath:
				registers = true
			}
			return true
		})
		if builds && !registers {
			rel, _ := filepath.Rel(root, path)
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}
