package queuecreds_test

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var censusDirs = []string{"internal/joboutbox", "internal/syncreconciler", "internal/joboperator", "internal/jobruntime", "internal/workersctl"}

type srcFile struct {
	path string
	file *ast.File
	src  []byte
}

// formatCensus parses the Go files of the census directories under root and
// returns one finding per queue-row value formatted into an error or log
// output without a guard, and the classified outputs it met.
func formatCensus(t *testing.T, root string, dirs []string) ([]string, map[string]bool) {
	t.Helper()
	fset := token.NewFileSet()
	var findings []string
	seen := map[string]bool{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		var pkg []srcFile
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, dir, entry.Name())
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			file, err := parser.ParseFile(fset, path, source, 0)
			if err != nil {
				t.Fatal(err)
			}
			pkg = append(pkg, srcFile{dir + "/" + entry.Name(), file, source})
		}
		findings = append(findings, unguardedOutputs(fset, pkg, seen)...)
	}
	sort.Strings(findings)
	return findings, seen
}

func unguardedOutputs(fset *token.FileSet, pkg []srcFile, seen map[string]bool) []string {
	var findings []string
	for _, sf := range pkg {
		for _, decl := range sf.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			body := string(sf.src[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset])
			where := sf.path + "#" + fn.Name.Name
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || !outputCall.MatchString(exprText(fset, sf.src, call.Fun)) {
					return true
				}
				for _, arg := range call.Args {
					ast.Inspect(arg, func(inner ast.Node) bool {
						name := ""
						switch typed := inner.(type) {
						case *ast.Ident:
							name = typed.Name
						case *ast.SelectorExpr:
							name = typed.Sel.Name
						}
						if name == "" || !rowKeyName.MatchString(name) {
							return true
						}
						if strings.Contains(body, guardCall) {
							return true
						}
						if _, ok := decodedRowFieldOutputs[where]; ok {
							seen[where] = true
							return true
						}
						findings = append(findings, where+" formats "+name+" into an error or log line without "+guardCall)
						return true
					})
				}
				return true
			})
		}
	}
	return findings
}

func treeSHA(t *testing.T, root string, dirs []string) string {
	t.Helper()
	sum := sha256.New()
	for _, dir := range dirs {
		_ = filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				t.Fatal(rerr)
			}
			sum.Write([]byte(path))
			sum.Write(data)
			return nil
		})
	}
	return hex.EncodeToString(sum.Sum(nil))
}

const fixtureHead = `package fx

import (
	"fmt"
	"log"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

type Row struct {
	DedupeKey     string
	CorrelationID string
	N             int
}

`

// plantCensus writes the files under a temporary tree and runs the census on it.
func plantCensus(t *testing.T, files map[string]string) []string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(fixtureHead+body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	findings, _ := formatCensus(t, root, []string{"internal/joboutbox"})
	return findings
}

var formatPlants = []struct {
	name  string
	body  string
	clean string
}{
	{"F01-renamed-local",
		"func f(row Row) { k := row.DedupeKey; slog.Info(\"x\", \"k\", k) }\n",
		"func f(row Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; slog.Info(\"x\", \"k\", k) }\n"},
	{"F02-slog-default",
		"func f(row Row) { slog.Default().Warn(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(row Row) { slog.Default().Warn(\"x\", \"k\", row.N) }\n"},
	{"F03-helper-return",
		"func keyOf(r Row) string { return r.DedupeKey }\nfunc f(r Row) { slog.Info(\"x\", \"k\", keyOf(r)) }\n",
		"func keyOf(r Row) string { return r.DedupeKey }\nfunc f(r Row) { if credshape.AnyCredentialShaped(keyOf(r)) { return }; slog.Info(\"x\", \"k\", keyOf(r)) }\n"},
	{"F04-whole-struct",
		"func f(r Row) error { return fmt.Errorf(\"bad %+v\", r) }\n",
		"func f(r Row) error { return fmt.Errorf(\"bad %d\", r.N) }\n"},
	{"F05-format-before-guard",
		"func f(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey); if credshape.AnyCredentialShaped(row.DedupeKey) { return } }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n"},
	{"F06-guard-in-other-branch",
		"func f(row Row, c bool) { if c { if credshape.AnyCredentialShaped(row.DedupeKey) { return } } else { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n",
		"func f(row Row, c bool) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; if c { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n"},
	{"F07-guard-of-other-value",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.CorrelationID) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n"},
	{"F08-logger-with",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"k\", row.DedupeKey); _ = l }\n",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"k\", row.N); _ = l }\n"},
	{"F09-log-printf",
		"func f(row Row) { log.Printf(\"x %s\", row.DedupeKey) }\n",
		"func f(row Row) { log.Printf(\"x %d\", row.N) }\n"},
	{"F10-fmt-print",
		"func f(row Row) { fmt.Print(row.DedupeKey) }\n",
		"func f(row Row) { fmt.Print(row.N) }\n"},
	{"F11-span-attribute",
		"func f(span Span, row Row) { span.SetAttributes(attribute.String(\"k\", row.DedupeKey)) }\n",
		"func f(span Span, row Row) { span.SetAttributes(attribute.Int(\"k\", row.N)) }\n"},
	{"F12-metric-label",
		"func f(counter Vec, row Row) { counter.WithLabelValues(row.DedupeKey).Inc() }\n",
		"func f(counter Vec, row Row) { counter.WithLabelValues(\"fixed\").Inc() }\n"},
}

func TestQueueRowFormatCensusFindsEachPlantedShape(t *testing.T) {
	root := repoRoot(t)
	before := treeSHA(t, root, censusDirs)
	for _, plant := range formatPlants {
		t.Run(plant.name, func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": plant.body}); len(got) == 0 {
				t.Errorf("%s: the census did not turn red", plant.name)
			}
		})
		t.Run(plant.name+"-clean", func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": plant.clean}); len(got) != 0 {
				t.Errorf("%s: the clean twin is reported: %v", plant.name, got)
			}
		})
	}
	if after := treeSHA(t, root, censusDirs); after != before {
		t.Errorf("the plants changed the real tree: sha256 %s != %s", before, after)
	}
}

func TestQueueRowFormatCensusRecursesIntoSubpackages(t *testing.T) {
	got := plantCensus(t, map[string]string{"internal/joboutbox/sub/deep/x.go": "func f(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n"})
	if len(got) == 0 {
		t.Error("R01-subpackage: a plant in a subpackage was not found")
	}
}

func TestQueueRowSQLCensusFindsArgsAtAnyPosition(t *testing.T) {
	cases := []struct {
		name, sql string
		want      bool
	}{
		{"S01-last-in-projection", "SELECT id, args FROM jobs", true},
		{"S02-returning", "UPDATE jobs SET state = 'x' RETURNING id, args", true},
		{"S03-first-in-projection", "SELECT args, id FROM jobs", true},
		{"S04-before-newline-from", "SELECT id,\n  args\nFROM jobs", true},
		{"S05-in-call", "SELECT coalesce(args) FROM jobs", true},
		{"S06-unrelated-column", "SELECT id, state FROM jobs", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := false
			for _, key := range sqlKeys(c.sql) {
				got = got || key == "args"
			}
			if got != c.want {
				t.Errorf("sqlKeys(%q) names args = %v, want %v", c.sql, got, c.want)
			}
		})
	}
}
