// Package gitmaintcensus fails when a Go source file runs git in a way that can start a detached
// `git maintenance run --auto` without turning that off for the call (CHAOS-11771, CHAOS-11767).
//
// On git >= 2.47 (maintenance autoDetach) commit, merge, fetch and similar commands start a detached
// maintenance process that can still write in .git while t.TempDir's RemoveAll runs, which ends in
// "unlinkat .git: directory not empty". The census parses every .go file of the module (all build tags)
// and checks each git exec site.
package gitmaintcensus

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const quietFlag = "maintenance.auto=false"

// runnerNames are the callee names that run a program named by their first argument:
// cmdSpec.run and freeSpec.run in internal/cigates and the plain run helpers.
var runnerNames = map[string]bool{
	"run": true, "runGit": true, "git": true, "gitRun": true, "mustGit": true, "execGit": true,
	"runCmd": true, "runCommand": true,
}

// readOnlySubcommands start no maintenance. init is in the list because it writes a fresh repository
// and never calls the auto-maintenance hook (only commit, merge, fetch, pull, rebase, am, gc and
// receive-pack do).
var readOnlySubcommands = map[string]bool{
	"log": true, "rev-parse": true, "show": true, "diff": true, "ls-files": true, "ls-tree": true,
	"cat-file": true, "status": true, "config": true, "merge-base": true, "rev-list": true,
	"for-each-ref": true, "describe": true, "symbolic-ref": true, "var": true, "version": true,
	"init": true,
}

var configReadForms = map[string]bool{
	"--get": true, "--get-all": true, "--get-regexp": true, "--get-urlmatch": true, "--list": true,
	"-l": true, "--show-origin": true, "--show-scope": true,
}

// allowlist names non-test helpers that build the git argv from their callers' arguments, so the census
// cannot see the subcommand. Key: slash path from the module root, then "|", then the function
// (Type.Method for a method). The census does not follow helper callers: a new triggering caller of an
// allowlisted helper is not caught. Production argv stays unchanged on purpose (CHAOS-11771 plan).
var allowlist = map[string]string{
	"internal/testsupport/ratchet/ratchet.go|git":          "read-only helper; callers: rev-parse, merge-base, show",
	"internal/cigates/frozengoldens/reads.go|reads.diff":   "read-only helper; the only git call is diff --name-only",
	"internal/cigates/frozengoldens/reads.go|reads.lsTree": "read-only helper; the only git call is ls-tree",
	"cmd/dev-health-migration-matrix/main.go|gitOutput":    "read-only helper; callers: rev-parse, merge-base, cat-file",
	"internal/cigates/routing/deps.go|ExecGit.Run":         "read-only helper; caller planes.go runs read-only plane queries",
	"internal/localgit/git.go|Repo.run":                    "read-only helper; callers: blame, diff, diff-tree, cat-file, rev-list, config --list, remote get-url",
}

// staleEntries lists the allowlist entries that matched no failing git exec site.
func staleEntries(allow map[string]string, used map[string]bool) []string {
	var out []string
	for key := range allow {
		if !used[key] {
			out = append(out, "stale allowlist entry matches no failing git exec site: "+key)
		}
	}
	sort.Strings(out)
	return out
}

type site struct {
	file   string
	line   int
	reason string
}

type unit struct {
	fset  *token.FileSet
	files map[string]*ast.File
}

func parseFile(t testing.TB, u *unit, path string) {
	t.Helper()
	f, err := parser.ParseFile(u.fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	u.files[path] = f
}

func hasQuiet(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if l, ok := x.(*ast.BasicLit); ok && l.Kind == token.STRING && strings.Contains(l.Value, quietFlag) {
			found = true
		}
		return !found
	})
	return found
}

// declsOf returns the package-level var/const names of the files whose initializer holds the flag, and
// the names of the package-level consts that equal "git".
func declsOf(files []*ast.File) (quiet map[string]bool, gitConst map[string]bool) {
	quiet, gitConst = map[string]bool{}, map[string]bool{}
	for _, f := range files {
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || (g.Tok != token.VAR && g.Tok != token.CONST) {
				continue
			}
			for _, s := range g.Specs {
				vs := s.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if i < len(vs.Values) {
						if hasQuiet(vs.Values[i]) {
							quiet[name.Name] = true
						}
						if l, ok := vs.Values[i].(*ast.BasicLit); ok && g.Tok == token.CONST && l.Value == `"git"` {
							gitConst[name.Name] = true
						}
					}
				}
			}
		}
	}
	return quiet, gitConst
}

func isGit(e ast.Expr, gitConst map[string]bool) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return x.Kind == token.STRING && x.Value == `"git"`
	case *ast.Ident:
		return gitConst[x.Name]
	}
	return false
}

func calleeName(c *ast.CallExpr) (name string, isExec bool) {
	switch fn := c.Fun.(type) {
	case *ast.Ident:
		return fn.Name, false
	case *ast.SelectorExpr:
		if x, ok := fn.X.(*ast.Ident); ok && x.Name == "exec" {
			return fn.Sel.Name, true
		}
		return fn.Sel.Name, false
	}
	return "", false
}

// gitArgs returns the git arguments of a site (program name excluded) and the index of the first one.
func gitSite(c *ast.CallExpr, gitConst map[string]bool) (args []ast.Expr, ok bool) {
	name, isExec := calleeName(c)
	switch {
	case isExec && name == "Command" && len(c.Args) > 0 && isGit(c.Args[0], gitConst):
		return c.Args[1:], true
	case isExec && name == "CommandContext" && len(c.Args) > 1 && isGit(c.Args[1], gitConst):
		return c.Args[2:], true
	case !isExec && runnerNames[name] && len(c.Args) > 0 && isGit(c.Args[0], gitConst):
		return c.Args[1:], true
	}
	return nil, false
}

// tok is one leading git argument: its literal text, or lit false when the argument is not a literal.
type tok struct {
	val string
	lit bool
}

// leadingTokens flattens the leading arguments, looking into the first operand of
// append([]string{...}, ...). A non-literal argument is one unknown token; the list ends at an
// expression that may hold any number of arguments (a spread or an append tail).
func leadingTokens(args []ast.Expr) []tok {
	var out []tok
	var walk func(es []ast.Expr) bool
	walk = func(es []ast.Expr) bool {
		for _, e := range es {
			switch x := e.(type) {
			case *ast.BasicLit:
				if x.Kind != token.STRING {
					out = append(out, tok{})
					continue
				}
				out = append(out, tok{val: strings.Trim(x.Value, "`\""), lit: true})
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "append" && len(x.Args) > 0 {
					if cl, ok := x.Args[0].(*ast.CompositeLit); ok {
						if !walk(cl.Elts) || len(x.Args) > 1 {
							return false
						}
						continue
					}
				}
				return false
			case *ast.CompositeLit:
				if !walk(x.Elts) {
					return false
				}
			case *ast.Ident, *ast.SelectorExpr, *ast.IndexExpr, *ast.BinaryExpr:
				out = append(out, tok{})
			default:
				return false
			}
		}
		return true
	}
	walk(args)
	return out
}

// subcommand skips the global options before the subcommand. ok is false when no literal subcommand
// is reached.
func subcommand(prefix []tok) (sub string, rest []string, ok bool) {
	for i := 0; i < len(prefix); i++ {
		p := prefix[i]
		if !p.lit {
			return "", nil, false
		}
		switch {
		case p.val == "-C" || p.val == "-c" || p.val == "--git-dir" || p.val == "--work-tree" || p.val == "--namespace":
			i++
		case strings.HasPrefix(p.val, "-"):
		default:
			for _, r := range prefix[i+1:] {
				rest = append(rest, r.val)
			}
			return p.val, rest, true
		}
	}
	return "", nil, false
}

func classify(c *ast.CallExpr, args []ast.Expr) (pass bool, reason string) {
	sub, rest, ok := subcommand(leadingTokens(args))
	if !ok {
		return false, "subcommand is not a literal and the call has no " + quietFlag
	}
	if !readOnlySubcommands[sub] {
		return false, "git " + sub + " can start maintenance and the call has no " + quietFlag
	}
	if sub == "config" {
		for _, r := range rest {
			if configReadForms[r] {
				return true, ""
			}
		}
		return false, "git config without a read form can write and the call has no " + quietFlag
	}
	return true, ""
}

// outermostFunc is the FuncDecl (or package-level FuncLit) that holds pos, nil for a package-level var.
func outermostFunc(f *ast.File, pos token.Pos) ast.Node {
	for _, d := range f.Decls {
		if d.Pos() <= pos && pos < d.End() {
			return d
		}
	}
	return nil
}

func mentions(n ast.Node, names map[string]bool) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if id, ok := x.(*ast.Ident); ok && names[id.Name] {
			found = true
		}
		return !found
	})
	return found
}

// funcName is "Type.Method" or "func" of the FuncDecl that holds the node, "" at package level.
func funcName(fn ast.Node) string {
	d, ok := fn.(*ast.FuncDecl)
	if !ok {
		return ""
	}
	if d.Recv != nil && len(d.Recv.List) > 0 {
		t := d.Recv.List[0].Type
		if st, ok := t.(*ast.StarExpr); ok {
			t = st.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return id.Name + "." + d.Name.Name
		}
	}
	return d.Name.Name
}

// failures returns every git exec site of the unit that can start maintenance and is not quieted.
// A failing site whose root-relative path and function are in allow is skipped and recorded in used.
func failures(u *unit, root string, allow map[string]string, used map[string]bool) []site {
	var all []*ast.File
	for _, f := range u.files {
		all = append(all, f)
	}
	quiet, gitConst := declsOf(all)
	var out []site
	for path, f := range u.files {
		ast.Inspect(f, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			args, ok := gitSite(c, gitConst)
			if !ok {
				return true
			}
			if hasQuiet(c) {
				return true
			}
			if fn := outermostFunc(f, c.Pos()); fn != nil && (hasQuiet(fn) || mentions(fn, quiet)) {
				return true
			}
			if pass, reason := classify(c, args); !pass {
				rel, _ := filepath.Rel(root, path)
				key := filepath.ToSlash(rel) + "|" + funcName(outermostFunc(f, c.Pos()))
				if _, ok := allow[key]; ok {
					used[key] = true
					return true
				}
				out = append(out, site{file: path, line: u.fset.Position(c.Pos()).Line, reason: reason})
			}
			return true
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].file != out[j].file {
			return out[i].file < out[j].file
		}
		return out[i].line < out[j].line
	})
	return out
}

func TestEveryGitExecSiteThatCanStartMaintenanceTurnsItOff(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	byDir := map[string]*unit{}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "vendor", "testdata", ".git", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		dir := filepath.Dir(p)
		if byDir[dir] == nil {
			byDir[dir] = &unit{fset: token.NewFileSet(), files: map[string]*ast.File{}}
		}
		parseFile(t, byDir[dir], p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var bad []string
	used := map[string]bool{}
	for _, u := range byDir {
		for _, s := range failures(u, root, allowlist, used) {
			rel, _ := filepath.Rel(root, s.file)
			bad = append(bad, fmt.Sprintf("%s:%d: %s", rel, s.line, s.reason))
		}
	}
	bad = append(bad, staleEntries(allowlist, used)...)
	sort.Strings(bad)
	for _, b := range bad {
		t.Error(b)
	}
}

func fixture(t *testing.T, name string) []site {
	t.Helper()
	u := &unit{fset: token.NewFileSet(), files: map[string]*ast.File{}}
	parseFile(t, u, filepath.Join("testdata", name))
	return failures(u, ".", nil, map[string]bool{})
}

func TestFixtureCells(t *testing.T) {
	cells := []struct {
		file string
		fail int
	}{
		{"raw_commit.go", 1},
		{"commit_with_flag.go", 0},
		{"read_only_log.go", 0},
		{"var_held_flag.go", 0},
		{"helper_with_flag.go", 0},
		{"nonliteral_subcommand.go", 1},
		{"provider_name_not_exec.go", 0},
	}
	for _, c := range cells {
		t.Run(c.file, func(t *testing.T) {
			if got := fixture(t, c.file); len(got) != c.fail {
				t.Fatalf("%s: want %d failing sites, got %d: %+v", c.file, c.fail, len(got), got)
			}
		})
	}
}

func TestAllowlistEntryThatMatchesNoSiteIsStale(t *testing.T) {
	u := &unit{fset: token.NewFileSet(), files: map[string]*ast.File{}}
	parseFile(t, u, filepath.Join("testdata", "raw_commit.go"))
	allow := map[string]string{
		"testdata/raw_commit.go|rawCommit": "used",
		"testdata/raw_commit.go|gone":      "stale",
	}
	used := map[string]bool{}
	if got := failures(u, ".", allow, used); len(got) != 0 {
		t.Fatalf("allowlisted site still fails: %+v", got)
	}
	stale := staleEntries(allow, used)
	if len(stale) != 1 || !strings.HasSuffix(stale[0], "testdata/raw_commit.go|gone") {
		t.Fatalf("stale = %v: only the entry that matches no site is stale", stale)
	}
}
