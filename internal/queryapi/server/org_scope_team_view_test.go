package server

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

// CHAOS-14376: a served reader of the team view takes rowview.ServedTeamSource or rowview.ServedTeamPick, whose own
// SQL scopes the read by org_id. rowview.TeamSource and rowview.TeamPick are scoped by their caller's where (the
// org scope census lists them as scoped elsewhere, for the job callers only), so no served function may name them.
//
// What it checks: the go/ast selector <import name>.TeamSource or <import name>.TeamPick, where <import name> is one of
// the file's names of the package .../internal/queryapi/rowview (each binding of the import, renamed ones included) and
// the identifier is bound to no local, parameter or receiver (ident.Obj is nil), in every non-test file of the served
// roots except the files of package rowview itself (its subpackages are walked). A blank or a dot binding of rowview
// in a served file fails closed: the selector cannot be bound to it. NOT pinned: reflection, and a reference the AST
// cannot bind without go/types (ticket 14348 binds the view helpers with go/types).

const rowviewImportPath = "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"

var unscopedTeamViewHelpers = map[string]bool{"TeamSource": true, "TeamPick": true}

// servedUnscopedTeamViewUses returns "file:line: <selector>" for each use of an unscoped team view helper in file, and
// a refusal for a blank or dot binding of rowview. file must be parsed with object resolution.
func servedUnscopedTeamViewUses(fset *token.FileSet, name string, file *ast.File) []string {
	locals := map[string]bool{}
	var uses []string
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != rowviewImportPath {
			continue
		}
		local := "rowview"
		if spec.Name != nil {
			local = spec.Name.Name
		}
		if local == "_" || local == "." {
			uses = append(uses, name+":"+strconv.Itoa(fset.Position(spec.Pos()).Line)+": a "+map[bool]string{true: "blank", false: "dot"}[local == "_"]+
				" import of rowview (name the import)")
			continue
		}
		locals[local] = true
	}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, isIdent := selector.X.(*ast.Ident); isIdent && pkg.Obj == nil && locals[pkg.Name] && unscopedTeamViewHelpers[selector.Sel.Name] {
			uses = append(uses, name+":"+strconv.Itoa(fset.Position(selector.Pos()).Line)+": "+pkg.Name+"."+selector.Sel.Name)
		}
		return true
	})
	return uses
}

// servedTeamViewScan reads every non-test Go file of the served roots under root (the files of package rowview itself
// excepted, its subdirectories walked) that imports rowview, and returns its refusals and the number of such files.
func servedTeamViewScan(root string, served []string) (uses []string, scanned int, err error) {
	rowviewDir := filepath.Join(root, "internal", "queryapi", "rowview")
	for _, dir := range served {
		walkErr := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || filepath.Dir(path) == rowviewDir {
				return nil
			}
			fset := token.NewFileSet()
			file, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly|parser.SkipObjectResolution)
			if parseErr != nil {
				return parseErr
			}
			imports := false
			for _, spec := range file.Imports {
				importPath, unquoteErr := strconv.Unquote(spec.Path.Value)
				imports = imports || (unquoteErr == nil && importPath == rowviewImportPath)
			}
			if !imports {
				return nil
			}
			full, parseErr := parser.ParseFile(fset, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			scanned++
			relative, _ := filepath.Rel(root, path)
			uses = append(uses, servedUnscopedTeamViewUses(fset, filepath.ToSlash(relative), full)...)
			return nil
		})
		if walkErr != nil {
			return nil, 0, walkErr
		}
	}
	sort.Strings(uses)
	return uses, scanned, nil
}

func TestNoServedReaderTakesTheUnscopedTeamView(t *testing.T) {
	uses, scanned, err := servedTeamViewScan(filepath.Join("..", "..", ".."), orgScopeCensusRoots)
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 10 {
		t.Fatalf("the scan found %d served files that import rowview: it read the tree wrong", scanned)
	}
	for _, use := range uses {
		t.Errorf("%s: a served reader takes rowview.ServedTeamSource or rowview.ServedTeamPick, which scope the read by org_id", use)
	}
}

// The refusal holds each import form the check binds, and accepts the served helpers.
func TestServedTeamViewRefusalHoldsEachImportForm(t *testing.T) {
	for _, cell := range []struct {
		name, source string
		refused      int
	}{
		{"plain import, TeamSource", `package p
import "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
func f() string { return rowview.TeamSource("t", "w") }`, 1},
		{"plain import, TeamPick as a value", `package p
import "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
var pick = rowview.TeamPick`, 1},
		{"renamed import", `package p
import rv "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
func f() string { return rv.TeamSource("t", "w") + rv.TeamPick("t", "n") }`, 2},
		{"served helpers", `package p
import "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
func f() string { return rowview.ServedTeamSource("t", "w") + rowview.ServedTeamPick("t", "w") }`, 0},
		{"another package's TeamSource", `package p
import rowview "example.com/other/rowview"
func f() string { return rowview.TeamSource("t", "w") }`, 0},
		{"a method on a parameter named like the import", `package p
import "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
type store struct{}
func (store) TeamSource(a, b string) string { return a }
func f(rowview store) string { return rowview.TeamSource("t", "w") }
var _ = rowview.ServedTeamSource`, 0},
		{"a method on a local named like the import", `package p
import "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
type store struct{}
func (store) TeamPick(a, b string) string { return a }
func f() string { rowview := store{}; return rowview.TeamPick("t", "w") }
var _ = rowview.ServedTeamSource`, 0},
		{"the first of two named bindings", `package p
import a "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
import b "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
func f() string { return a.TeamSource("t", "w") + b.ServedTeamSource("t", "w") }`, 1},
		{"a blank binding (fails closed) beside a used one", `package p
import rv "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
import _ "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
var pick = rv.TeamPick`, 2},
		{"a dot binding (fails closed)", `package p
import . "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
func f() string { return TeamSource("t", "w") }`, 1},
		{"a raw-string import path", "package p\nimport rv `github.com/full-chaos/dev-health-ops/internal/queryapi/rowview`\nfunc f() string { return rv.TeamSource(\"t\", \"w\") }", 1},
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "cell.go", cell.source, 0)
		if err != nil {
			t.Fatalf("%s: %v", cell.name, err)
		}
		if got := servedUnscopedTeamViewUses(fset, "cell.go", file); len(got) != cell.refused {
			t.Errorf("%s: refused %v, want %d", cell.name, got, cell.refused)
		}
	}
}

// The scan reads a file whose import path is spelled as a raw string, and walks the subpackages of rowview (only the
// files of package rowview itself are exempt).
func TestServedTeamViewScanReadsEveryImportSpellingAndTheRowviewSubpackages(t *testing.T) {
	root := t.TempDir()
	write := func(path, text string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/queryapi/rowview/rowview.go", "package rowview\n\nfunc TeamSource(a, b string) string { return TeamPick(a, b) }\n\nfunc TeamPick(a, b string) string { return a }\n")
	write("internal/queryapi/raw/raw.go", "package raw\n\nimport rv `"+rowviewImportPath+"`\n\nfunc F() string { return rv.TeamSource(\"t\", \"w\") }\n")
	write("internal/queryapi/escaped/escaped.go", "package escaped\n\nimport rv \"github.com/full-chaos/dev-health-ops/internal/queryapi/row\\x76iew\"\n\nvar pick = rv.TeamPick\n")
	write("internal/queryapi/rowview/sub/sub.go", "package sub\n\nimport \""+rowviewImportPath+"\"\n\nfunc F() string { return rowview.TeamPick(\"t\", \"w\") }\n")
	write("internal/queryapi/rowview/sub/deeper/deeper.go", "package deeper\n\nimport \""+rowviewImportPath+"\"\n\nvar source = rowview.TeamSource\n")
	uses, scanned, err := servedTeamViewScan(root, []string{"internal/queryapi"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"internal/queryapi/escaped/escaped.go:5: rv.TeamPick",
		"internal/queryapi/raw/raw.go:5: rv.TeamSource",
		"internal/queryapi/rowview/sub/deeper/deeper.go:5: rowview.TeamSource",
		"internal/queryapi/rowview/sub/sub.go:5: rowview.TeamPick",
	}
	if strings.Join(uses, "\n") != strings.Join(want, "\n") || scanned != 4 {
		t.Errorf("scanned %d files, refused %v; want 4 files, refused %v", scanned, uses, want)
	}
}
