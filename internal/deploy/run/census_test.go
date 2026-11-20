package run

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// The census scope: the non-test .go files of internal/deploy/** and
// internal/deploycli. It pins, by AST token: (1) no read or write of the
// process env (os.Getenv, LookupEnv, Environ, ExpandEnv, Setenv, Unsetenv,
// Clearenv, syscall.Getenv/Environ/Setenv), so no env var opens a fake or
// changes a run; (2) os/exec only in run/run.go and no other process start
// (os.StartProcess, syscall.Exec, ForkExec, StartProcess); (3) no string
// literal whose whole text is a shell name (sh, bash, dash, zsh, ksh, ash,
// busybox, or a path ending in one), and none that holds a shell name followed
// by a -c style flag (-c, -lc, -ec): the literal is split on whitespace and on
// ; & | ( ) { } < > $, backtick, quotes and backslash, so the pair is found after
// ";", "&&", "|", "$(", "(" or quotes; a quoted lone shell name is refused too; (4) no dot import of os, os/exec or
// syscall; (5) every exec.Command / exec.CommandContext call names its binary
// with a string literal (the closed set of run.go). NOT pinned: a shell name with no -c style flag
// right after it in a mixed literal (such as "sh run.sh"); test files;
// reflect/unsafe/linkname/cgo calls; a shell name built from parts at run
// time. The run-time guard is run.Exec: it starts no tool off its allow list
// (git, helm, kubectl), whatever a literal holds.
var censusRoots = []string{"..", "../../deploycli"}

const execFile = "run/run.go"

var (
	envFuncs   = map[string][]string{"os": {"Getenv", "LookupEnv", "Environ", "ExpandEnv", "Setenv", "Unsetenv", "Clearenv"}, "syscall": {"Getenv", "Environ", "Setenv", "Unsetenv", "Clearenv"}}
	startFuncs = map[string][]string{"os": {"StartProcess"}, "syscall": {"Exec", "ForkExec", "StartProcess"}}
	shellName  = regexp.MustCompile(`^(.*/)?(sh|bash|dash|zsh|ksh|ash|busybox)$`)
	shellFlagC = regexp.MustCompile(`^-[a-z]*c[a-z]*$`)
)

func isShellSep(r rune) bool {
	return unicode.IsSpace(r) || strings.ContainsRune(";&|(){}<>$`\"'\\", r)
}

// shellLiteral reports a literal that holds a shell start: a lone shell token
// (a whole shell name or a quoted one), or a shell token followed by a -c style flag.
func shellLiteral(v string) bool {
	toks := strings.FieldsFunc(v, isShellSep)
	if len(toks) == 1 && shellName.MatchString(toks[0]) {
		return true
	}
	for i := 0; i+1 < len(toks); i++ {
		if shellName.MatchString(toks[i]) && shellFlagC.MatchString(toks[i+1]) {
			return true
		}
	}
	return false
}

type finding struct{ file, rule string }

// censusFile returns the findings of one file; rel is its path under internal/deploy (or deploycli/...).
func censusFile(rel string, src []byte) ([]finding, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var out []finding
	add := func(rule string) { out = append(out, finding{rel, rule}) }
	local := map[string]string{}
	for _, im := range f.Imports {
		path, _ := strconv.Unquote(im.Path.Value)
		name := path[strings.LastIndex(path, "/")+1:]
		if im.Name != nil {
			name = im.Name.Name
		}
		switch path {
		case "os", "os/exec", "syscall":
			if name == "." {
				add("dot import " + path)
			}
		}
		if path == "os/exec" && rel != execFile {
			add("os/exec outside " + execFile)
		}
		local[name] = path
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok || local[id.Name] != "os/exec" {
				return true
			}
			at := map[string]int{"Command": 0, "CommandContext": 1}
			if i, ok := at[sel.Sel.Name]; ok {
				if len(x.Args) <= i {
					add("exec name not a literal")
					return true
				}
				if lit, ok := x.Args[i].(*ast.BasicLit); !ok || lit.Kind != token.STRING {
					add("exec name not a literal")
				}
			}
		case *ast.SelectorExpr:
			id, ok := x.X.(*ast.Ident)
			if !ok {
				return true
			}
			pkg := local[id.Name]
			if slices.Contains(envFuncs[pkg], x.Sel.Name) {
				add("env access " + pkg + "." + x.Sel.Name)
			}
			if slices.Contains(startFuncs[pkg], x.Sel.Name) {
				add("process start " + pkg + "." + x.Sel.Name)
			}
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			v, err := strconv.Unquote(x.Value)
			if err != nil {
				return true
			}
			if shellLiteral(v) {
				add("shell literal")
			}
		}
		return true
	})
	return out, nil
}

func TestNoEnvNoShellNoOtherExecSite(t *testing.T) {
	var files []string
	var all []finding
	for _, root := range censusRoots {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == "testdata" {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			rel := filepath.ToSlash(p)
			rel = strings.TrimPrefix(strings.TrimPrefix(rel, "../../"), "../")
			src, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			got, err := censusFile(rel, src)
			if err != nil {
				return err
			}
			files = append(files, rel)
			all = append(all, got...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{execFile, "run/state.go", "run/step.go", "gitgate/gitgate.go", "helmrev/helmrev.go", "inputs/inputs.go", "selfpin/selfpin.go", "deploycli/deploycli.go"} {
		if !slices.Contains(files, want) {
			t.Fatalf("the census did not read %s (read %v)", want, files)
		}
	}
	for _, f := range all {
		t.Errorf("%s: %s", f.file, f.rule)
	}
}

// The census catches each planted form and passes a clean file.
func TestTheCensusCatchesEachPlantedForm(t *testing.T) {
	for name, c := range map[string]struct {
		rel, src, rule string
	}{
		"os.Getenv":                      {"gitgate/x.go", `package x; import "os"; var _ = os.Getenv("X")`, "env access os.Getenv"},
		"aliased LookupEnv":              {"gitgate/x.go", `package x; import o "os"; func f() { o.LookupEnv("X") }`, "env access os.LookupEnv"},
		"os.Environ value":               {"helmrev/x.go", `package x; import "os"; var f = os.Environ`, "env access os.Environ"},
		"syscall.Getenv":                 {"run/x.go", `package x; import "syscall"; func f() { syscall.Getenv("X") }`, "env access syscall.Getenv"},
		"os.Setenv":                      {"run/x.go", `package x; import "os"; func f() { os.Setenv("X", "1") }`, "env access os.Setenv"},
		"exec elsewhere":                 {"gitgate/x.go", `package x; import "os/exec"; var _ = exec.Command`, "os/exec outside " + execFile},
		"os.StartProcess":                {"run/x.go", `package x; import "os"; var _ = os.StartProcess`, "process start os.StartProcess"},
		"syscall.Exec":                   {"run/x.go", `package x; import "syscall"; var _ = syscall.Exec`, "process start syscall.Exec"},
		"aliased ForkExec":               {"run/x.go", `package x; import s "syscall"; var _ = s.ForkExec`, "process start syscall.ForkExec"},
		"sh literal":                     {"run/x.go", `package x; var a = []string{"sh"}`, "shell literal"},
		"bin bash literal":               {"run/x.go", "package x; const b = `/bin/bash`", "shell literal"},
		"sh -c in a string":              {"helmrev/x.go", `package x; const c = "kubectl x; sh  -c y"`, "shell literal"},
		"bash -c in a path":              {"helmrev/x.go", `package x; const c = "/usr/bin/bash -c y"`, "shell literal"},
		"; sh -c":                        {"run/x.go", `package x; const c = "x; sh -c y"`, "shell literal"},
		";sh -c no space":                {"run/x.go", `package x; const c = "x;sh -c y"`, "shell literal"},
		"|bash -c no space":              {"run/x.go", `package x; const c = "x|bash -c y"`, "shell literal"},
		"$(bash -c":                      {"run/x.go", `package x; const c = "echo $(bash -c id)"`, "shell literal"},
		"(sh -c":                         {"run/x.go", `package x; const c = "(sh -c id)"`, "shell literal"},
		"quoted bash quoted -c":          {"run/x.go", "package x; const c = `\"bash\" \"-c\" id`", "shell literal"},
		"&& sh -c":                       {"run/x.go", `package x; const c = "a && sh -c b"`, "shell literal"},
		"| bash -c":                      {"run/x.go", `package x; const c = "a | bash -c b"`, "shell literal"},
		"backtick sh -c":                 {"run/x.go", "package x; const c = \"`sh -c id`\"", "shell literal"},
		"bash -lc":                       {"run/x.go", `package x; const c = "x; bash -lc y"`, "shell literal"},
		"env sh -c":                      {"run/x.go", `package x; const c = "/usr/bin/env sh -c y"`, "shell literal"},
		"newline sh -c":                  {"run/x.go", `package x; const c = "x\nsh -c y"`, "shell literal"},
		"quoted lone bash":               {"run/x.go", `package x; const c = "'bash'"`, "shell literal"},
		"dot import os":                  {"run/x.go", `package x; import . "os"; var _ = Args`, "dot import os"},
		"dot import syscall":             {"run/x.go", `package x; import . "syscall"; var _ = Getpid`, "dot import syscall"},
		"exec.Command(name)":             {execFile, `package x; import "os/exec"; func f(n string) { exec.Command(n) }`, "exec name not a literal"},
		"exec.CommandContext(ctx, name)": {execFile, `package x; import ("context"; "os/exec"); func f(c context.Context, n string) { exec.CommandContext(c, n, "x") }`, "exec name not a literal"},
		"aliased exec, name from argv":   {execFile, `package x; import e "os/exec"; func f(a []string) { e.Command(a[0], a[1:]...) }`, "exec name not a literal"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := censusFile(c.rel, []byte(c.src))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(got, finding{c.rel, c.rule}) {
				t.Fatalf("want %q, got %v", c.rule, got)
			}
		})
	}
	clean := `package x; import ("context"; "os"; "os/exec"; "syscall"); var _ = os.Args; var _ = exec.Command("git", "x"); var _ = syscall.Umask; var a = []string{"git", "-C", "bash.d", "shell", "kubectl", "-c", "run bash docs", "sh run.sh", "x ; git -c maintenance.auto=false", "echo $(date)"}; func f(c context.Context, a []string) { exec.CommandContext(c, "helm", a...) }`
	if got, err := censusFile(execFile, []byte(clean)); err != nil || len(got) != 0 {
		t.Fatalf("a clean exec file: %v %v", got, err)
	}
}
