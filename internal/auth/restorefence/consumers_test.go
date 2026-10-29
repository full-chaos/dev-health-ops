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

const (
	auditImportPath        = "github.com/full-chaos/dev-health-ops/internal/audit"
	restorefenceImportPath = "github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
)

// outboxConsumerDirs returns the package directories (relative to root) of
// non-test Go files that start an outbox publisher (audit.NewPublisher) or
// implement audit.Sink (a Deliver method taking audit.Event), and
// the directories that register a fence floor input
// (restorefence.RegisterFloorInput). Package audit itself is skipped.
func outboxConsumerDirs(t *testing.T, root string) (publishers, registrars []string) {
	t.Helper()
	pub, reg := map[string]bool{}, map[string]bool{}
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
		if err != nil {
			return err
		}
		if !strings.Contains(string(data), "NewPublisher") && !strings.Contains(string(data), "RegisterFloorInput") && !strings.Contains(string(data), "Deliver(") {
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
		dir, _ := filepath.Rel(root, filepath.Dir(path))
		dir = filepath.ToSlash(dir)
		ast.Inspect(file, func(n ast.Node) bool {
			if fn, ok := n.(*ast.FuncDecl); ok && fn.Name.Name == "Deliver" && fn.Recv != nil && fn.Type.Params != nil {
				for _, field := range fn.Type.Params.List {
					if sel, ok := field.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "Event" {
						if id, ok := sel.X.(*ast.Ident); ok && names[id.Name] == auditImportPath {
							pub[dir] = true
						}
					}
				}
			}
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
			case sel.Sel.Name == "NewPublisher" && names[id.Name] == auditImportPath:
				pub[dir] = true
			case sel.Sel.Name == "RegisterFloorInput" && names[id.Name] == restorefenceImportPath:
				reg[dir] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for d := range pub {
		publishers = append(publishers, d)
	}
	for d := range reg {
		registrars = append(registrars, d)
	}
	sort.Strings(publishers)
	sort.Strings(registrars)
	return publishers, registrars
}

// unregisteredConsumers names the publisher directories without a registered
// floor input in the same directory.
func unregisteredConsumers(publishers, registrars []string) []string {
	have := map[string]bool{}
	for _, r := range registrars {
		have[r] = true
	}
	var out []string
	for _, p := range publishers {
		if !have[p] {
			out = append(out, p)
		}
	}
	return out
}

// The first consumer of the auth outbox must hand its high-water marks to the
// fence: the floor is the maximum of every input, and a consumer that is not an
// input could see a revision re-issued with other content after a restore.
// Today no production code starts a publisher (runtime.go builds the loop with
// no sink), so this passes; it fails the day one does without registering.
func TestEveryOutboxConsumerHasAFenceFloorInput(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	publishers, registrars := outboxConsumerDirs(t, filepath.Join(root, "internal"))
	if missing := unregisteredConsumers(publishers, registrars); len(missing) > 0 {
		t.Fatalf("these packages start an outbox publisher without restorefence.RegisterFloorInput: %v", missing)
	}
}

func TestTheConsumerCensusSeesAPublisherWithoutAnInput(t *testing.T) {
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
	write("lone/lone.go", "package lone\n\nimport \"github.com/full-chaos/dev-health-ops/internal/audit\"\n\nfunc start() { _, _ = audit.NewPublisher(nil, nil, audit.PublisherConfig{}) }\n")
	write("paired/paired.go", "package paired\n\nimport (\n\t\"github.com/full-chaos/dev-health-ops/internal/audit\"\n\t\"github.com/full-chaos/dev-health-ops/internal/auth/restorefence\"\n)\n\nfunc start() {\n\t_, _ = audit.NewPublisher(nil, nil, audit.PublisherConfig{})\n\trestorefence.RegisterFloorInput(nil)\n}\n")
	write("sink/sink.go", "package sink\n\nimport (\n\t\"context\"\n\n\t\"github.com/full-chaos/dev-health-ops/internal/audit\"\n)\n\ntype feed struct{}\n\nfunc (feed) Deliver(ctx context.Context, e audit.Event) error { return nil }\n")
	write("mention/mention.go", "package mention\n\n// audit.NewPublisher is only named in this comment.\nvar _ = \"NewPublisher\"\n")
	publishers, registrars := outboxConsumerDirs(t, root)
	if got := strings.Join(unregisteredConsumers(publishers, registrars), ","); got != "lone,sink" {
		t.Fatalf("unregistered consumers = %q, want lone,sink", got)
	}
}
