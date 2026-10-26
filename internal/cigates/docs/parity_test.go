package docs_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// `make docs:check` claims to be what CI enforces for the docs. This test compares the two check sets and fails when a
// check exists on one side only, or when CI runs the same check twice (the site is built and checked once per revision).

const (
	makefilePath    = "Makefile"
	buildScriptPath = "internal/cigates/docsbuild/docsbuild.go"
	mkdocsBuild     = "mkdocs build"
	contentGoTest   = "go test ./internal/cigates/docs/..."
	docsPrepare     = "docs-prepare"
)

// fastLoopOnly are the checks `docs:check-fast` runs that Docs Guards does not: the drift gates run in docs-drift.yml and
// the source-link check is the inner-loop form of the rendered-link check.
var fastLoopOnly = map[string]bool{
	"check-investment-drift": true, "check-queue-mapping-drift": true, "check-docs-links": true,
}

var (
	cigateVerb    = regexp.MustCompile(`cmd/cigate (docs-prepare|docs ([a-z][a-z-]*))`)
	goTestDocs    = regexp.MustCompile(`^go test .*\./internal/cigates/docs/\.\.\.`)
	mkdocsCmd     = regexp.MustCompile(`mkdocs build\b`)
	makeSubTarget = regexp.MustCompile(`\$\(MAKE\)[^\n]* (docs\\:[a-z-]+)`)
)

// preChain and postChain are what `cigate docs-build pre|post --mode preview --full-check` run; MkDocs runs between them as
// its own command line. The test reads the Go source as text only (never runs it) and fails when a verb is not named in it.
var (
	preChain    = []string{"validate-publication"}
	postChain   = []string{"check-built-site-links", "check-docs-search", "check-docs-accessibility", "check-candidate-facts", docsPrepare}
	scriptChain = append(append(append([]string{}, preChain...), mkdocsBuild), postChain...)
)

// ciOnly are checks CI runs and `make docs:check` need not: the preview job compares the stamp of the site it downloaded.
var ciOnly = map[string]bool{"check-validated-site": true}

// checksOf lists the checks one command line runs. A run of docs-build pre or post expands to its chain.
func checksOf(line string) []string {
	switch {
	case strings.Contains(line, "cmd/cigate docs-build"):
		if !strings.Contains(line, "--full-check") {
			return nil
		}
		switch {
		case strings.Contains(line, "docs-build pre "):
			return append([]string{}, preChain...)
		case strings.Contains(line, "docs-build post "):
			return append([]string{}, postChain...)
		}
		return nil
	case mkdocsCmd.MatchString(line):
		return []string{mkdocsBuild}
	case goTestDocs.MatchString(line):
		return []string{contentGoTest}
	}
	if m := cigateVerb.FindStringSubmatch(line); m != nil {
		if m[2] == "" {
			return []string{docsPrepare}
		}
		return []string{m[2]}
	}
	return nil
}

func ciChecks(d docTree) ([]string, []string) {
	wf, err := parseYAML(d, docsGuardsPath)
	if err != nil {
		return nil, []string{err.Error()}
	}
	var out []string
	for job := range asMap(wf["jobs"]) {
		for _, s := range workflowSteps(wf, job) {
			for _, line := range commandLines(asString(s["run"])) {
				out = append(out, checksOf(line)...)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func makeRecipe(makefile, target string, seen map[string]bool) []string {
	if seen[target] {
		return nil
	}
	seen[target] = true
	var recipe []string
	in := false
	for _, raw := range strings.Split(makefile, "\n") {
		switch {
		case strings.HasPrefix(raw, target+":"):
			in = true
		case in && strings.HasPrefix(raw, "\t"):
			line := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "@"))
			if m := makeSubTarget.FindStringSubmatch(line); m != nil {
				recipe = append(recipe, makeRecipe(makefile, m[1], seen)...)
				continue
			}
			recipe = append(recipe, line)
		case in && !strings.HasPrefix(raw, "#") && strings.TrimSpace(raw) != "":
			in = false
		}
	}
	return recipe
}

func localChecks(makefile string) []string {
	var out []string
	for _, line := range makeRecipe(makefile, `docs\:check`, map[string]bool{}) {
		out = append(out, checksOf(line)...)
	}
	sort.Strings(out)
	return out
}

func parityGuard(d docTree) []string {
	makefile, err := d.read(makefilePath)
	if err != nil {
		return []string{err.Error()}
	}
	script, err := d.read(buildScriptPath)
	if err != nil {
		return []string{err.Error()}
	}
	ci, errs := ciChecks(d)
	if errs != nil {
		return errs
	}
	var out []string
	for _, verb := range scriptChain {
		if verb != mkdocsBuild && verb != docsPrepare && !strings.Contains(script, `"`+verb+`"`) {
			out = append(out, fmt.Sprintf("%s does not name %q: scriptChain in this test is out of date", buildScriptPath, verb))
		}
	}
	local := localChecks(makefile)
	seen := map[string]int{}
	for _, c := range ci {
		seen[c]++
	}
	for c, n := range seen {
		if n > 1 {
			out = append(out, fmt.Sprintf("docs-guards.yml runs %q %d times: the site is built and checked once per revision", c, n))
		}
	}
	have := map[string]bool{}
	for _, c := range local {
		have[c] = true
	}
	inCI := map[string]bool{}
	for _, c := range ci {
		inCI[c] = true
		if !have[c] && !ciOnly[c] {
			out = append(out, fmt.Sprintf("CI runs %q but `make docs:check` does not", c))
		}
	}
	for _, c := range local {
		if !inCI[c] && !fastLoopOnly[c] {
			out = append(out, fmt.Sprintf("`make docs:check` runs %q but docs-guards.yml does not", c))
		}
	}
	if len(ci) == 0 || len(local) == 0 {
		out = append(out, "no checks found on one side: the parity test reads nothing")
	}
	return out
}

func TestDocsCheckMatchesCI(t *testing.T) {
	assertGuard(t, parityGuard, []plantCase{
		{"a check is dropped from docs:check", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "check-code-prerequisites\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs check-freshness\n", "check-code-prerequisites\n")
		}, `CI runs "check-freshness" but`},
		{"a check is added to docs:check only", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "check-code-prerequisites\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs check-freshness\n", "check-code-prerequisites\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs check-freshness\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs check-new-thing\n")
		}, `runs "check-new-thing" but docs-guards.yml does not`},
		{"the build script stops naming a chain check", func(t *testing.T, d docTree) docTree {
			return d.edit(t, buildScriptPath, `"check-docs-search"`, `"check-docs-sear"`)
		}, `does not name "check-docs-search"`},
		{"a check is added to docs-guards.yml only", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Upload documentation quality evidence\n", "      - name: Check new thing\n        run: go run -trimpath -mod=readonly ./cmd/cigate docs check-new-thing\n\n      - name: Upload documentation quality evidence\n")
		}, `CI runs "check-new-thing" but`},
		{"the content go test is dropped from docs:check", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "\tgo test -mod=readonly -trimpath -count=1 ./internal/cigates/docs/...\n", "")
		}, `CI runs "go test ./internal/cigates/docs/..." but`},
		{"docs:check stops running the build script", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "check-freshness\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build pre --mode preview --full-check\n\t$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build post --mode preview --full-check\n", "check-freshness\n")
		}, "CI runs"},
		{"docs:check drops the pre step", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "check-freshness\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build pre --mode preview --full-check\n\t$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build post --mode preview --full-check\n", "check-freshness\n\t$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build post --mode preview --full-check\n")
		}, `CI runs "validate-publication" but`},
		{"docs:check drops the post step", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "check-freshness\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build pre --mode preview --full-check\n\t$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build post --mode preview --full-check\n", "check-freshness\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build pre --mode preview --full-check\n\t$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml\n")
		}, `CI runs "check-built-site-links" but`},
		{"docs:check drops the mkdocs build", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "check-freshness\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build pre --mode preview --full-check\n\t$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build post --mode preview --full-check\n", "check-freshness\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build pre --mode preview --full-check\n\tgo run -trimpath -mod=readonly ./cmd/cigate docs-build post --mode preview --full-check\n")
		}, `CI runs "mkdocs build" but`},
		{"a second job builds the site again", func(t *testing.T, d docTree) docTree {
			return d.appended(t, docsGuardsPath, "  rebuild:\n    runs-on: ubuntu-latest\n    steps:\n      - run: python -m mkdocs build --strict --config-file mkdocs.yml\n")
		}, `runs "mkdocs build" 2 times`},
	})
}

// No test of this package runs Python, the build script or a Makefile target (which runs the script): the docs checks are Go.
// pythonExecFindings parses one test file and reports each os/exec call that names an interpreter, a .py file, the docs build
// script or make.
func pythonExecFindings(name string, src []byte) []string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		return []string{name + ": " + err.Error()}
	}
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "exec" || !strings.HasPrefix(sel.Sel.Name, "Command") {
			return true
		}
		for _, arg := range call.Args {
			ast.Inspect(arg, func(m ast.Node) bool {
				lit, ok := m.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				v := strings.ToLower(lit.Value)
				for _, bad := range []string{"python", ".py", "build_docs_cloudflare", `"make"`, "mkdocs"} {
					if strings.Contains(v, bad) {
						out = append(out, fmt.Sprintf("%s: exec call names %s", fset.Position(lit.Pos()), lit.Value))
						break
					}
				}
				return true
			})
		}
		return true
	})
	return out
}

func TestDocsTestsExecuteNoPython(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files found: %v", err)
	}
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, finding := range pythonExecFindings(f, src) {
			t.Error(finding)
		}
	}
	planted := map[string]string{
		"python3":     `exec.Command("python3", "-c", "print(1)")`,
		"the script":  `exec.CommandContext(ctx, "sh", "-c", "scripts/build_docs_cloudflare.py --mode preview")`,
		"a py file":   `exec.Command("./check.py")`,
		"make":        `exec.Command("make", "docs:check")`,
		"mkdocs":      `exec.Command("mkdocs", "build")`,
		"interpreter": `exec.Command("/usr/bin/python", "x")`,
	}
	for name, call := range planted {
		src := "package p\nimport \"os/exec\"\nfunc f(ctx int) { _ = " + call + " }\n"
		if len(pythonExecFindings("planted.go", []byte(src))) == 0 {
			t.Errorf("planted exec of %s: the detector found nothing", name)
		}
	}
	if got := pythonExecFindings("clean.go", []byte("package p\nimport \"os/exec\"\nfunc f() { _ = exec.Command(\"git\", \"ls-files\") }\n")); len(got) != 0 {
		t.Errorf("a git call was flagged: %v", got)
	}
}
