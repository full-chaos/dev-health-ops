//go:build !race

package splitshards_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// The packages too big for one integration job run as name-partitioned test shards (ci/go_*_test_shards.tsv). Every test
// the compiled package registers (`go test -list`) must run in exactly one shard of `check_go.sh integration-shard TARGET
// SHARD --dry-run`: none in two shards, none in no shard, none named in a shard that does not compile.

func shardsOf(t *testing.T, manifest string) int {
	t.Helper()
	for _, l := range strings.Split(readText(t, manifest), "\n") {
		if rest, ok := strings.CutPrefix(l, "shards\t"); ok {
			n, err := strconv.Atoi(strings.TrimSpace(rest))
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("%s declares no shard count", manifest)
	return 0
}

func dryRun(t *testing.T, root, target string, shard int) cmdResult {
	t.Helper()
	return freeSpec{dir: root, env: inheritedEnv(nil)}.run("bash", "ci/check_go.sh", "integration-shard", target, strconv.Itoa(shard), "--dry-run")
}

func goTestList(t *testing.T, root, pkg string) map[string]bool {
	t.Helper()
	res := freeSpec{dir: root, env: inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")}.run("go", "test", "-mod=readonly", "-tags=integration", "-list", ".*", "./"+pkg)
	if res.code != 0 {
		t.Fatalf("go test -list %s: %s%s", pkg, res.stdout, res.stderr)
	}
	out := map[string]bool{}
	for _, l := range strings.Split(res.stdout, "\n") {
		if strings.HasPrefix(l, "Test") {
			out[l] = true
		}
	}
	return out
}

// goTestSignature is the signature rule of cmd/go for a test: one parameter of type *T or *x.T, no results, no type
// parameters (go test refuses to build a package whose Test-named function breaks it).
func goTestSignature(fn *ast.FuncDecl) bool {
	if fn.Type.TypeParams.NumFields() > 0 || fn.Type.Results.NumFields() > 0 || fn.Type.Params == nil ||
		len(fn.Type.Params.List) != 1 || len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}
	ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch x := ptr.X.(type) {
	case *ast.Ident:
		return x.Name == "T"
	case *ast.SelectorExpr:
		return x.Sel.Name == "T"
	}
	return false
}

// parsedTestFuncs parses one Go test file and returns its top-level tests (cmd/go's rule: "Test" or "Test" plus a rune
// that is not lower case, no receiver, goTestSignature; TestMain aside) and the first top-level init or TestMain.
func parsedTestFuncs(t *testing.T, path string) ([]string, string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var tests []string
	hook := ""
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		name := fn.Name.Name
		if name == "init" || name == "TestMain" {
			if hook == "" {
				hook = name
			}
			continue
		}
		if rest, ok := strings.CutPrefix(name, "Test"); ok && goTestSignature(fn) {
			if r, _ := utf8.DecodeRuneInString(rest); rest == "" || !unicode.IsLower(r) {
				tests = append(tests, name)
			}
		}
	}
	return tests, hook
}

// taggedOnly is the oracle for one split package, built apart from check_go.sh: the top-level tests of the test files
// that only the integration build lists (go list -tags=integration minus plain go list), and the first `func init` or
// `func TestMain` of such a file ("" when none). A hook changes the process the untagged tests run in, so that package
// keeps every test in its shards.
func taggedOnly(t *testing.T, root, pkg string) (map[string]bool, string) {
	t.Helper()
	files := func(tags ...string) []string {
		args := append([]string{"list", "-e", "-mod=readonly"}, tags...)
		args = append(args, "-f={{range .TestGoFiles}}{{println .}}{{end}}{{range .XTestGoFiles}}{{println .}}{{end}}", "./"+pkg)
		res := freeSpec{dir: root, env: inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")}.run("go", args...)
		if res.code != 0 {
			t.Fatalf("go list %s: %s%s", pkg, res.stdout, res.stderr)
		}
		return strings.Fields(res.stdout)
	}
	inPlain := map[string]bool{}
	for _, f := range files() {
		inPlain[f] = true
	}
	tagged, hook := map[string]bool{}, ""
	for _, f := range files("-tags=integration") {
		if inPlain[f] {
			continue
		}
		tests, h := parsedTestFuncs(t, filepath.Join(root, pkg, f))
		if h != "" && hook == "" {
			hook = pkg + "/" + f + " " + h
		}
		for _, n := range tests {
			tagged[n] = true
		}
	}
	if len(tagged) == 0 {
		t.Fatalf("%s: no integration-tagged top-level test found: a vacuous oracle", pkg)
	}
	return tagged, hook
}

func runLines(stdout, prefix string) []string {
	var out []string
	for _, l := range strings.Split(stdout, "\n") {
		if rest, ok := strings.CutPrefix(l, prefix); ok {
			out = append(out, rest)
		}
	}
	return out
}

func TestEveryTestRunsInExactlyOneSplitPackageShard(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	for _, c := range []struct{ name, manifest, prefix, pkg string }{
		{"daily", "ci/go_daily_test_shards.tsv", "  DAILY-TEST-RUN ", "internal/jobs/metrics/daily"},
		{"workerservice", "ci/go_workerservice_test_shards.tsv", "  WORKERSERVICE-TEST-RUN ", "internal/workerservice"},
		{"queryapiserver", "ci/go_queryapi_server_test_shards.tsv", "  QAPISERVER-TEST-RUN ", "internal/queryapi/server"},
		{"remaining", "ci/go_remaining_test_shards.tsv", "  REMAINING-TEST-RUN ", "internal/jobs/metrics/remaining"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var selected []string
			for shard := 1; shard <= shardsOf(t, filepath.Join(root, c.manifest)); shard++ {
				r := dryRun(t, root, c.name, shard)
				if r.code != 0 || !strings.Contains(r.stdout, fmt.Sprintf("%s test shard %d: DRY RUN", c.name, shard)) {
					t.Fatalf("shard %d: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
				}
				chosen := runLines(r.stdout, c.prefix)
				if len(chosen) == 0 {
					t.Fatalf("%s shard %d selected no tests", c.name, shard)
				}
				selected = append(selected, chosen...)
			}
			got := map[string]bool{}
			for _, s := range selected {
				got[s] = true
			}
			if len(selected) != len(got) {
				t.Errorf("a %s test is in two shards", c.name)
			}
			listed := goTestList(t, root, c.pkg)
			// Untagged tests run in the go-quality test and race legs (FULL on merge_group), so a shard runs only the
			// integration-tagged ones, unless a tag-only hook makes the package run whole.
			want, hook := taggedOnly(t, root, c.pkg)
			if hook != "" {
				want = listed
			}
			var missing, extra, untagged []string
			for k := range want {
				if !got[k] {
					missing = append(missing, k)
				}
			}
			for k := range got {
				switch {
				case !listed[k]:
					extra = append(extra, k)
				case !want[k]:
					untagged = append(untagged, k)
				}
			}
			sort.Strings(missing)
			sort.Strings(extra)
			sort.Strings(untagged)
			if len(missing) > 0 || len(extra) > 0 || len(untagged) > 0 {
				t.Errorf("in no shard: %v; in a shard but not compiled: %v; untagged test in a shard (%d): %v", missing[:min(5, len(missing))], extra[:min(5, len(extra))], len(untagged), untagged[:min(5, len(untagged))])
			}
		})
	}
}
