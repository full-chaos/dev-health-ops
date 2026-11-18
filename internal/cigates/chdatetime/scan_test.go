package chdatetime_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type goFile struct {
	Rel     string
	Fset    *token.FileSet
	AST     *ast.File
	Imports map[string]string
}

type constDef struct {
	Expr ast.Expr
	File *goFile
}

type goPackage struct {
	Files  []*goFile
	Consts map[string][]constDef
}

// sources holds the parsed non-test Go packages and the SQL files of the tracked tree.
type sources struct {
	module string
	pkgs   map[string]*goPackage // by slash-separated directory relative to the root
	sql    []text
}

// outOfContract are the trees the guard does not read: migration history and baseline are DDL, testdata is fixtures.
func outOfContract(rel string) bool {
	for _, prefix := range []string{
		"internal/chmigrate/history/", "internal/chmigrate/archive/", "internal/chmigrate/rollback/", "internal/chmigrate/baseline/",
	} {
		if strings.HasPrefix(rel, prefix) {
			return true
		}
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "testdata" {
			return true
		}
	}
	return false
}

func trackedFiles(root string) ([]string, error) {
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.go", "*.sql").Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files in %s: %w", root, err)
	}
	var files []string
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel != "" {
			files = append(files, rel)
		}
	}
	return files, nil
}

func loadSources(root string) (*sources, error) {
	files, err := trackedFiles(root)
	if err != nil {
		return nil, err
	}
	return sourcesOf(root, files)
}

func sourcesOf(root string, files []string) (*sources, error) {
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return nil, err
	}
	src := &sources{pkgs: map[string]*goPackage{}}
	for _, line := range strings.Split(string(mod), "\n") {
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			src.module = strings.TrimSpace(rest)
			break
		}
	}
	if src.module == "" {
		return nil, fmt.Errorf("go.mod names no module")
	}
	for _, rel := range files {
		if outOfContract(rel) {
			continue
		}
		switch {
		case strings.HasSuffix(rel, ".sql"):
			body, err := os.ReadFile(filepath.Join(root, rel))
			if err != nil {
				return nil, err
			}
			src.sql = append(src.sql, text{File: rel, Line: 1, Body: string(body)})
		case strings.HasSuffix(rel, "_test.go"):
		default:
			if err := src.addGo(root, rel); err != nil {
				return nil, err
			}
		}
	}
	return src, nil
}

func (s *sources) addGo(root, rel string) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, 0)
	if err != nil {
		return fmt.Errorf("parse %s: %w", rel, err)
	}
	gf := &goFile{Rel: rel, Fset: fset, AST: file, Imports: map[string]string{}}
	for _, imp := range file.Imports {
		ipath, _ := strconv.Unquote(imp.Path.Value)
		name := path.Base(ipath)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		gf.Imports[name] = ipath
	}
	dir := path.Dir(rel)
	pkg := s.pkgs[dir]
	if pkg == nil {
		pkg = &goPackage{Consts: map[string][]constDef{}}
		s.pkgs[dir] = pkg
	}
	pkg.Files = append(pkg.Files, gf)
	ast.Inspect(file, func(n ast.Node) bool {
		decl, ok := n.(*ast.GenDecl)
		if !ok || decl.Tok != token.CONST {
			return true
		}
		for _, spec := range decl.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if i < len(vs.Values) {
					pkg.Consts[name.Name] = append(pkg.Consts[name.Name], constDef{vs.Values[i], gf})
				}
			}
		}
		return true
	})
	return nil
}

// eval folds a compile-time string expression: literals, +, string(...), and constants of the same package or of an
// imported package of this module. A constant that is declared more than once with different values is unresolved.
func (s *sources) eval(expr ast.Expr, file *goFile, depth int) (string, bool) {
	if depth > 24 {
		return "", false
	}
	switch node := expr.(type) {
	case *ast.BasicLit:
		if node.Kind != token.STRING {
			return "", false
		}
		v, err := strconv.Unquote(node.Value)
		return v, err == nil
	case *ast.ParenExpr:
		return s.eval(node.X, file, depth+1)
	case *ast.BinaryExpr:
		if node.Op != token.ADD {
			return "", false
		}
		left, okLeft := s.eval(node.X, file, depth+1)
		right, okRight := s.eval(node.Y, file, depth+1)
		return left + right, okLeft && okRight
	case *ast.CallExpr:
		if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "string" && len(node.Args) == 1 {
			return s.eval(node.Args[0], file, depth+1)
		}
	case *ast.Ident:
		return s.constValue(s.pkgs[path.Dir(file.Rel)], node.Name, depth)
	case *ast.SelectorExpr:
		id, ok := node.X.(*ast.Ident)
		if !ok {
			return "", false
		}
		ipath, ok := file.Imports[id.Name]
		if !ok || !strings.HasPrefix(ipath, s.module+"/") {
			return "", false
		}
		return s.constValue(s.pkgs[strings.TrimPrefix(ipath, s.module+"/")], node.Sel.Name, depth)
	}
	return "", false
}

func (s *sources) constValue(pkg *goPackage, name string, depth int) (string, bool) {
	if pkg == nil {
		return "", false
	}
	defs := pkg.Consts[name]
	if len(defs) == 0 {
		return "", false
	}
	var value string
	for i, def := range defs {
		v, ok := s.eval(def.Expr, def.File, depth+1)
		if !ok || i > 0 && v != value {
			return "", false
		}
		value = v
	}
	return value, true
}

// texts lists every SQL text of the sources: each .sql file and every Go string constant, a concatenation folded whole
// when every part resolves.
func (s *sources) texts() []text {
	out := append([]text(nil), s.sql...)
	dirs := make([]string, 0, len(s.pkgs))
	for dir := range s.pkgs {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		for _, gf := range s.pkgs[dir].Files {
			ast.Inspect(gf.AST, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.BinaryExpr:
					if body, ok := s.eval(node, gf, 0); ok {
						out = append(out, text{gf.Rel, gf.Fset.Position(node.Pos()).Line, body})
						return false
					}
				case *ast.BasicLit:
					if node.Kind == token.STRING {
						if body, err := strconv.Unquote(node.Value); err == nil {
							out = append(out, text{gf.Rel, gf.Fset.Position(node.Pos()).Line, body})
						}
					}
				}
				return true
			})
		}
	}
	return out
}

type result struct {
	Violations []hit
	Strict     int
	OnKeyed    int
	Flagged    map[string]int
}

// check applies the policy: a strict comparison of a keyed DateTime64 column against a DateTime bound, or against a
// bound the scan cannot classify, is a violation. A column no table keeps is only counted.
func check(cat catalog, texts []text) result {
	names := cat.names()
	keyed := cat.keyNames()
	res := result{Flagged: map[string]int{}}
	for _, t := range texts {
		for _, h := range strictHits(t, names) {
			res.Strict++
			if _, ok := keyed[h.Column]; !ok {
				if h.Class == classFlag || h.Class == classUnmeasured {
					res.Flagged[h.Column]++
				}
				continue
			}
			res.OnKeyed++
			if h.Class == classFlag || h.Class == classUnmeasured {
				res.Violations = append(res.Violations, h)
			}
		}
	}
	sort.Slice(res.Violations, func(i, j int) bool {
		a, b := res.Violations[i], res.Violations[j]
		return a.File < b.File || a.File == b.File && a.Line < b.Line
	})
	return res
}
