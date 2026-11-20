package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
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
// What it checks: the go/ast selector <import name>.TeamSource or <import name>.TeamPick, where <import name> is the
// file's name of the package .../internal/queryapi/rowview (renamed imports included), in every non-test file of the
// served roots except package rowview itself. NOT pinned: a dot import of rowview (the bare name is not bound to
// the package without go/types; ticket 14348 binds the view helpers with go/types), and reflection.

const rowviewImportPath = "github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"

var unscopedTeamViewHelpers = map[string]bool{"TeamSource": true, "TeamPick": true}

// servedUnscopedTeamViewUses returns "file:line: <selector>" for each use of an unscoped team view helper in file.
func servedUnscopedTeamViewUses(fset *token.FileSet, name string, file *ast.File) []string {
	local := ""
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != rowviewImportPath {
			continue
		}
		local = "rowview"
		if spec.Name != nil {
			local = spec.Name.Name
		}
	}
	if local == "" || local == "_" || local == "." {
		return nil
	}
	var uses []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, isIdent := selector.X.(*ast.Ident); isIdent && pkg.Name == local && unscopedTeamViewHelpers[selector.Sel.Name] {
			uses = append(uses, name+":"+strconv.Itoa(fset.Position(selector.Pos()).Line)+": "+local+"."+selector.Sel.Name)
		}
		return true
	})
	return uses
}

func TestNoServedReaderTakesTheUnscopedTeamView(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	var uses []string
	scanned := 0
	for _, served := range orgScopeCensusRoots {
		walkErr := filepath.WalkDir(filepath.Join(root, served), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || filepath.ToSlash(path) == filepath.ToSlash(filepath.Join(root, "internal/queryapi/rowview")) {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			file, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly|parser.SkipObjectResolution)
			if parseErr != nil {
				return parseErr
			}
			imports := false
			for _, spec := range file.Imports {
				imports = imports || strings.Trim(spec.Path.Value, `"`) == rowviewImportPath
			}
			if !imports {
				return nil
			}
			full, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if parseErr != nil {
				return parseErr
			}
			scanned++
			relative, _ := filepath.Rel(root, path)
			uses = append(uses, servedUnscopedTeamViewUses(fset, filepath.ToSlash(relative), full)...)
			return nil
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if scanned < 10 {
		t.Fatalf("the scan found %d served files that import rowview: it read the tree wrong", scanned)
	}
	sort.Strings(uses)
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
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "cell.go", cell.source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", cell.name, err)
		}
		if got := servedUnscopedTeamViewUses(fset, "cell.go", file); len(got) != cell.refused {
			t.Errorf("%s: refused %v, want %d", cell.name, got, cell.refused)
		}
	}
}
