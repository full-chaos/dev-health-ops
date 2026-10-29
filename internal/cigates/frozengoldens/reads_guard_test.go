package frozengoldens

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The exemption's digest is the set of files the guard records while it runs; a read that does not go through the
// recorder (reads.go) would be a guard input outside the digest. So no other source of the package may read a file,
// list a directory, run git or touch the embedded manifest.
func TestEveryGuardReadGoesThroughTheRecorder(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") || source == "reads.go" {
			continue
		}
		checked++
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		problems, err := unrecordedReads(source, data)
		if err != nil {
			t.Fatal(err)
		}
		for _, problem := range problems {
			t.Error(problem)
		}
	}
	if checked == 0 {
		t.Fatal("no package source was checked")
	}
}

// readPaths are the import paths whose every use is a read; walkPaths name the functions of an import path that are.
var (
	readPaths = map[string]bool{"os": true, "os/exec": true, "io/fs": true, "io/ioutil": true}
	walkPaths = map[string]map[string]bool{"path/filepath": {"Glob": true, "Walk": true, "WalkDir": true}}
)

// unrecordedReads lists the reads in one package source that bypass the recorder. A package is known by its import
// path, whatever name the source gives it (an alias or a dot import), and a local identifier that shadows a package
// name is not that package.
func unrecordedReads(source string, data []byte) ([]string, error) {
	var problems []string
	if strings.Contains(string(data), "go:embed") {
		problems = append(problems, source+" embeds a file outside the recorder")
	}
	file, err := parser.ParseFile(token.NewFileSet(), source, data, 0)
	if err != nil {
		return nil, err
	}
	imported := map[string]string{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, err
		}
		name := path.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		switch {
		case name == "_":
		case name == "." && (readPaths[importPath] || walkPaths[importPath] != nil):
			problems = append(problems, fmt.Sprintf("%s dot-imports %s: every guard read goes through the recorder in reads.go", source, importPath))
		default:
			imported[name] = importPath
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			pkg, ok := n.X.(*ast.Ident)
			if !ok || pkg.Obj != nil {
				return true
			}
			if importPath, ok := imported[pkg.Name]; ok && (readPaths[importPath] || walkPaths[importPath][n.Sel.Name]) {
				problems = append(problems, fmt.Sprintf("%s uses %s.%s (%s): every guard read goes through the recorder in reads.go", source, pkg.Name, n.Sel.Name, importPath))
			}
		case *ast.Ident:
			if n.Name == "manifestFile" {
				problems = append(problems, source+" reads the embedded manifest outside the recorder")
			}
		}
		return true
	})
	return problems, nil
}

// The guard knows a read by the import path it comes from, not by the name the source gives the package: an alias or
// a dot import of os, os/exec, io/fs, io/ioutil or a path/filepath walk is the same read.
func TestTheRecorderGuardKnowsAReadByItsImportPath(t *testing.T) {
	for name, cell := range map[string]struct {
		source string
		read   bool
	}{
		"os aliased":              {"package x\nimport disk \"os\"\nfunc f(p string) ([]byte, error) { return disk.ReadFile(p) }\n", true},
		"os/exec aliased":         {"package x\nimport run \"os/exec\"\nfunc f() error { return run.Command(\"git\").Run() }\n", true},
		"io/fs aliased":           {"package x\nimport iofs \"io/fs\"\nfunc f(d iofs.FS) ([]byte, error) { return iofs.ReadFile(d, \"x\") }\n", true},
		"io/ioutil aliased":       {"package x\nimport old \"io/ioutil\"\nfunc f(p string) ([]byte, error) { return old.ReadFile(p) }\n", true},
		"path/filepath aliased":   {"package x\nimport fp \"path/filepath\"\nfunc f() ([]string, error) { return fp.Glob(\"*\") }\n", true},
		"os dot-imported":         {"package x\nimport . \"os\"\nfunc f(p string) ([]byte, error) { return ReadFile(p) }\n", true},
		"os by its own name":      {"package x\nimport \"os\"\nfunc f(p string) ([]byte, error) { return os.ReadFile(p) }\n", true},
		"path/filepath join only": {"package x\nimport fp \"path/filepath\"\nfunc f() string { return fp.Join(\"a\", \"b\") }\n", false},
		"a local named os":        {"package x\ntype t struct{ ReadFile int }\nfunc f() int { os := t{}; return os.ReadFile }\n", false},
	} {
		t.Run(name, func(t *testing.T) {
			problems, err := unrecordedReads("planted.go", []byte(cell.source))
			if err != nil {
				t.Fatal(err)
			}
			if cell.read && len(problems) == 0 {
				t.Errorf("the guard does not see the read in %q", cell.source)
			}
			if !cell.read && len(problems) > 0 {
				t.Errorf("the guard flags %q: %v", cell.source, problems)
			}
		})
	}
}
