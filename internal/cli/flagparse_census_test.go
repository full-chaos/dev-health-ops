package cli_test

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const modulePrefix = "github.com/full-chaos/dev-health-ops/"

// flagParseAllow lists the files (relative to the module root) and directories whose flag parsing is not
// through cli.ParseFlags/ParseFlagsError, each with its reason. A row that matches nothing fails.
var flagParseAllow = map[string]string{
	"internal/cli/flags.go":              "the helper itself: the one place that calls (*flag.FlagSet).Parse with the output discarded",
	"internal/pushlifecyclecli/flags.go": "its own parseFlags with the same behaviour (CHAOS-13049); moves to cli.ParseFlags in a follow-up once that change is on main",
	"internal/workersctl":                "every set comes from quietFlags (output io.Discard) and every Parse result is only compared with nil: no parse error text is ever printed",
}

// TestEveryDhoFlagParseGoesThroughTheHelper walks, by type, every non-test package the dho binary reaches in
// this module: a (*flag.FlagSet).Parse or flag.Parse call outside flagParseAllow, and a flag.NewFlagSet that
// is not ContinueOnError, fail (the flag package echoes the typed value of a rejected flag).
func TestEveryDhoFlagParseGoesThroughTheHelper(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	reached := dhoReachedPackages(t, root)
	if len(reached) < 100 {
		t.Fatalf("only %d module packages reached by cmd/dho: the walk did not run", len(reached))
	}
	pkgs := loadTyped(t, root, reached...)
	problems := flagParseProblems(root, pkgs, flagParseAllow)
	for _, p := range problems {
		t.Error(p)
	}
	if len(problems) > 0 {
		t.Log("fix: parse with cli.ParseFlags(flags, stderr, args) (or cli.ParseFlagsError for a verb that returns an error)")
	}
}

// TestFlagParseCensusFailsOnAPlantedSite proves the census sees a new site: a package outside the allowlist
// that parses a FlagSet directly, calls flag.Parse, or builds an ExitOnError set.
func TestFlagParseCensusFailsOnAPlantedSite(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module planted\n\ngo 1.22\n")
	write("planted.go", `package planted

import (
	"flag"
	"os"
)

func Direct(args []string) error {
	flags := flag.NewFlagSet("direct", flag.ContinueOnError)
	return flags.Parse(args)
}

func Aliased(args []string) error {
	set := flag.NewFlagSet("aliased", flag.ContinueOnError)
	parse := set.Parse
	return parse(args)
}

func Global() { flag.Parse() }

func Exiting() *flag.FlagSet { return flag.NewFlagSet("exit", flag.ExitOnError) }

var _ = os.Args
`)
	pkgs := loadTyped(t, dir, "./...")
	problems := flagParseProblems(dir, pkgs, flagParseAllow)
	want := []string{"planted.go:10", "planted.go:15", "planted.go:19", "planted.go:21"}
	for _, w := range want {
		found := false
		for _, p := range problems {
			if strings.Contains(p, w) {
				found = true
			}
		}
		if !found {
			t.Errorf("no problem for %s in %q", w, problems)
		}
	}
}

func TestFlagParseCensusFailsOnAStaleAllowRow(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	pkgs := loadTyped(t, root, "./internal/cli")
	problems := flagParseProblems(root, pkgs, map[string]string{"internal/nowhere": "matches nothing"})
	stale := false
	for _, p := range problems {
		if strings.Contains(p, "internal/nowhere") {
			stale = true
		}
	}
	if !stale {
		t.Fatalf("a row that matches nothing was accepted: %q", problems)
	}
}

func dhoReachedPackages(t *testing.T, root string) []string {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{
		Dir:  root,
		Env:  append(os.Environ(), "GOOS=linux", "GOARCH=amd64"),
		Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps,
	}, "./cmd/dho")
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	seen := map[string]bool{}
	packages.Visit(loaded, nil, func(p *packages.Package) {
		if strings.HasPrefix(p.PkgPath, modulePrefix) {
			seen[p.PkgPath] = true
		}
	})
	var out []string
	for path := range seen {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func loadTyped(t *testing.T, dir string, patterns ...string) []*packages.Package {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{
		Dir:  dir,
		Env:  append(os.Environ(), "GOOS=linux", "GOARCH=amd64"),
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}, patterns...)
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	for _, p := range loaded {
		for _, e := range p.Errors {
			t.Fatalf("load error in %s: %v", p.PkgPath, e)
		}
	}
	return loaded
}

func flagParseProblems(root string, pkgs []*packages.Package, allow map[string]string) []string {
	var problems []string
	used := map[string]bool{}
	allowed := func(rel string) bool {
		for row := range allow {
			if rel == row || strings.HasPrefix(rel, row+"/") {
				used[row] = true
				return true
			}
		}
		return false
	}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			rel, err := filepath.Rel(root, pkg.Fset.Position(file.Pos()).Filename)
			if err != nil {
				rel = pkg.Fset.Position(file.Pos()).Filename
			}
			rel = filepath.ToSlash(rel)
			at := func(n ast.Node) string { return fmt.Sprintf("%s:%d", rel, pkg.Fset.Position(n.Pos()).Line) }
			ast.Inspect(file, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					if flagFunc(pkg.TypesInfo, n.Fun) == "NewFlagSet" && len(n.Args) == 2 {
						if v := pkg.TypesInfo.Types[n.Args[1]].Value; v == nil || !constant.Compare(v, token.EQL, constant.MakeInt64(0)) {
							problems = append(problems, at(n)+": flag.NewFlagSet is not ContinueOnError: the flag package prints the rejected value and exits")
						}
					}
				case *ast.SelectorExpr:
					if fn := flagFunc(pkg.TypesInfo, n); (fn == "Parse" || fn == "(*FlagSet).Parse") && !allowed(rel) {
						problems = append(problems, at(n)+": "+fn+" outside cli.ParseFlags / cli.ParseFlagsError")
					}
				}
				return true
			})
		}
	}
	for row := range allow {
		if !used[row] {
			problems = append(problems, "allow row "+row+" matches no Parse call: remove it")
		}
	}
	sort.Strings(problems)
	return problems
}

// flagFunc names the flag-package function or method e resolves to ("Parse", "NewFlagSet",
// "(*FlagSet).Parse"), or "".
func flagFunc(info *types.Info, e ast.Expr) string {
	var id *ast.Ident
	switch e := e.(type) {
	case *ast.SelectorExpr:
		id = e.Sel
	case *ast.Ident:
		id = e
	default:
		return ""
	}
	fn, ok := info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "flag" {
		return ""
	}
	switch fn.Name() {
	case "Parse":
		if sig := fn.Type().(*types.Signature); sig.Recv() != nil {
			return "(*FlagSet).Parse"
		}
		return "Parse"
	case "NewFlagSet":
		return "NewFlagSet"
	}
	return ""
}
