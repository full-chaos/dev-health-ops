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
)

// The census scope: the non-test .go files of internal/deploy/** and
// internal/deploycli. It pins, by AST token: (1) no read or write of the
// process env (os.Getenv, LookupEnv, Environ, ExpandEnv, Setenv, Unsetenv,
// Clearenv, syscall.Getenv/Environ/Setenv), so no env var opens a fake or
// changes a run; (2) os/exec only in run/run.go and no other process start
// (os.StartProcess, syscall.Exec, ForkExec, StartProcess); (3) no string
// literal whose whole text is a shell name (sh, bash, dash, zsh, ksh, ash,
// busybox, or a path ending in one), and none that holds `<shell> -c` at the
// start or after whitespace or "/"; (4) no dot import of os, os/exec or
// syscall. NOT pinned (ticket 14358): a shell name after ";" (and "&&", "|",
// "&"), after "$(", after "(", or in quotes inside a literal; test files;
// reflect/unsafe/linkname/cgo calls; a shell name built from parts at run
// time. The run-time guard is run.Exec: it starts no tool off its allow list
// (git, helm, kubectl), whatever a literal holds.
var censusRoots = []string{"..", "../../deploycli"}

const execFile = "run/run.go"

var (
	envFuncs   = map[string][]string{"os": {"Getenv", "LookupEnv", "Environ", "ExpandEnv", "Setenv", "Unsetenv", "Clearenv"}, "syscall": {"Getenv", "Environ", "Setenv", "Unsetenv", "Clearenv"}}
	startFuncs = map[string][]string{"os": {"StartProcess"}, "syscall": {"Exec", "ForkExec", "StartProcess"}}
	shellName  = regexp.MustCompile(`^(.*/)?(sh|bash|dash|zsh|ksh|ash|busybox)$`)
	shellDashC = regexp.MustCompile(`(^|[\s/])(sh|bash|dash|zsh|ksh|ash|busybox)\s+-c\b`)
)

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
			if shellName.MatchString(v) || shellDashC.MatchString(v) {
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
		"os.Getenv":          {"gitgate/x.go", `package x; import "os"; var _ = os.Getenv("X")`, "env access os.Getenv"},
		"aliased LookupEnv":  {"gitgate/x.go", `package x; import o "os"; func f() { o.LookupEnv("X") }`, "env access os.LookupEnv"},
		"os.Environ value":   {"helmrev/x.go", `package x; import "os"; var f = os.Environ`, "env access os.Environ"},
		"syscall.Getenv":     {"run/x.go", `package x; import "syscall"; func f() { syscall.Getenv("X") }`, "env access syscall.Getenv"},
		"os.Setenv":          {"run/x.go", `package x; import "os"; func f() { os.Setenv("X", "1") }`, "env access os.Setenv"},
		"exec elsewhere":     {"gitgate/x.go", `package x; import "os/exec"; var _ = exec.Command`, "os/exec outside " + execFile},
		"os.StartProcess":    {"run/x.go", `package x; import "os"; var _ = os.StartProcess`, "process start os.StartProcess"},
		"syscall.Exec":       {"run/x.go", `package x; import "syscall"; var _ = syscall.Exec`, "process start syscall.Exec"},
		"aliased ForkExec":   {"run/x.go", `package x; import s "syscall"; var _ = s.ForkExec`, "process start syscall.ForkExec"},
		"sh literal":         {"run/x.go", `package x; var a = []string{"sh"}`, "shell literal"},
		"bin bash literal":   {"run/x.go", "package x; const b = `/bin/bash`", "shell literal"},
		"sh -c in a string":  {"helmrev/x.go", `package x; const c = "kubectl x; sh  -c y"`, "shell literal"},
		"bash -c in a path":  {"helmrev/x.go", `package x; const c = "/usr/bin/bash -c y"`, "shell literal"},
		"dot import os":      {"run/x.go", `package x; import . "os"; var _ = Args`, "dot import os"},
		"dot import syscall": {"run/x.go", `package x; import . "syscall"; var _ = Getpid`, "dot import syscall"},
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
	clean := `package x; import ("os"; "os/exec"; "syscall"); var _ = os.Args; var _ = exec.Command; var _ = syscall.Umask; var a = []string{"git", "-C", "bash.d", "shell", "kubectl", "-c"}`
	if got, err := censusFile(execFile, []byte(clean)); err != nil || len(got) != 0 {
		t.Fatalf("a clean exec file: %v %v", got, err)
	}
}
