//go:build !race

package integrationshards_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11429: a package-level side effect in an integration-tagged test file (a `var` initializer that calls
// something outside the pure list, or a blank or dot import) changes the process the package's untagged tests run in
// under -tags=integration, like func init. Such a package must stay unfiltered, unless a reviewed row covers it.

const sideEffectTable = "ci/integration_side_effect_reviewed.tsv"

func taggedTestFile(pkg, decls string) string {
	return "//go:build integration\n\npackage " + pkg + "\n\nimport \"testing\"\n\n" + decls + "\nfunc TestTagged_" + pkg + "(t *testing.T) {}\n"
}

func plainTestFile(pkg string) string {
	return "package " + pkg + "\n\nimport \"testing\"\n\nfunc TestPlain_" + pkg + "(t *testing.T) {}\n"
}

func TestPackageLevelSideEffectsInTaggedFilesKeepThePackageUnfiltered(t *testing.T) {
	t.Parallel()
	files := map[string]string{sideEffectTable: "# fixture table\ng/tag_test.go\t" + helperKey + "\tread helper: returns a literal; no global write, no registration, no I/O\n" +
		"h/tag_test.go\t" + helperKey + "\tread helper: returns a literal; no global write, no registration, no I/O\n" +
		"k/tag_test.go\tvar-call:funclit#1\tread the first func literal: returns 1; no global write, no registration, no I/O\n" +
		"w/tag_test.go\tvar-call:(...)#1\tread the first parenthesized func literal: returns 1; no global write, no registration, no I/O\n" +
		"o\tdeny-import:image/png\tthe plain test binary of o loads image/png as well, so the tagged binary registers nothing new\n" +
		"r\tdeny-import:image/png\tclaims the plain test binary of r loads image/png, which it does not\n"}
	planted := map[string]string{
		"a": "func register() int { return 1 }\n\nvar _ = register()\n",
		"b": "func helper() int { return 1 }\n\nvar x = helper()\n",
		"c": "var _ = func() int { return 1 }()\n",
		"d": "import _ \"net/http/pprof\"\n",
		"e": "import . \"strings\"\n",
		"h": "func helper() int { return 1 }\n\nfunc other() int { return 2 }\n\nvar y = helper()\n\nvar z = other()\n",
		"i": "import \"strings\"\n\nvar shared int\n\nvar _ = strings.Map(func(r rune) rune { shared++; return r }, \"x\")\n",
		"j": "import \"time\"\n\nvar loc, _ = time.LoadLocation(\"UTC\")\n",
		"m": "import \"fmt\"\n\nvar _ = fmt.Sprintf(\"%v\", func() {})\n",
		"k": "var shared int\n\nvar _ = func() int { return 1 }()\n\nvar _ = func() int { shared++; return shared }()\n",
		"l": "var n = len(\"x\")\n",
		"s": "var buf = make([]byte, 3)\n\nvar _ = copy(buf, \"ab\")\n",
		"t": "var shared = []int{1}\n\nvar _ = append(shared[:0], 2)\n",
		"u": "import \"strconv\"\n\nvar shared = make([]byte, 0, 8)\n\nvar _ = strconv.AppendInt(shared, 1, 10)\n",
		"v": "var hits int\n\nvar fn = func() int { hits++; return hits }\n\nvar ptr = &fn\n\nvar _ = (*ptr)()\n",
		"w": "var hits int\n\nvar _ = (func() int { return 1 })()\n\nvar _ = (func() int { hits++; return hits })()\n",
		"n": "import \"expvar\"\n\nvar _ = expvar.Get\n",
		"p": "import \"image/gif\"\n\nvar _ = gif.Decode\n",
	}
	controls := map[string]string{
		"f": "var re = regexp.MustCompile(`x`)\n\nvar at = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)\n\nvar err = errors.New(\"x\")\n\n" +
			"var buf = make([]byte, 4)\n\nvar n = int64(3)\n\nvar later = func() int { return 1 }\n\nvar names = []string{\"x\"}\n\nvar byName = map[string]int{}\n\nvar _ = fmt.Sprintf(\"%d\", 1)\n",
		"g": "func helper() int { return 1 }\n\nvar y = helper()\n",
		"o": "import \"image/png\"\n\nvar _ = png.Decode\n",
		"q": "import \"strings\"\n\nvar _ = strings.ToUpper\n",
		"x": "import \"strconv\"\n\ntype T struct{}\n\nvar a = (*int)(nil)\n\nvar b = (*T)(nil)\n\nvar c = strconv.Itoa(3)\n\nvar d = strconv.Quote(\"x\")\n",
	}
	selfChecked := map[string]string{"r": "import \"image/png\"\n\nvar _ = png.Decode\n"}
	want := map[string]string{
		"a": "a/tag_test.go " + registerKey, "b": "b/tag_test.go " + helperKey, "c": "c/tag_test.go var-call:funclit#1",
		"d": "d/tag_test.go blank-import:net/http/pprof", "e": "e/tag_test.go dot-import:strings", "h": "h/tag_test.go " + otherKey,
		"i": "i/tag_test.go var-call:strings.Map(func-arg)#1", "j": "j/tag_test.go var-call:time.LoadLocation#1",
		"m": "m/tag_test.go var-call:fmt.Sprintf(func-arg)#1", "k": "k/tag_test.go var-call:funclit#2",
		"l": "l/tag_test.go builtin-shadow:len",
		"n": "n/tag_test.go deny-import:expvar", "p": "p/tag_test.go deny-import:image/gif",
		"r": "r/tag_test.go deny-import:image/png",
		"s": "s/tag_test.go var-call:copy#1", "t": "t/tag_test.go var-call:append#1", "u": "u/tag_test.go var-call:strconv.AppendInt#1",
		"v": "v/tag_test.go var-call:(...)#1", "w": "w/tag_test.go var-call:(...)#2",
	}
	var pkgs []string
	for pkg, decls := range planted {
		files[pkg+"/plain_test.go"], files[pkg+"/tag_test.go"] = plainTestFile(pkg), taggedTestFile(pkg, decls)
		pkgs = append(pkgs, "./"+pkg)
	}
	for pkg, decls := range selfChecked {
		files[pkg+"/plain_test.go"], files[pkg+"/tag_test.go"] = plainTestFile(pkg), taggedTestFile(pkg, decls)
		pkgs = append(pkgs, "./"+pkg)
	}
	files["l/len.go"] = "package l\n\nfunc len(s string) int { return 0 }\n"
	for pkg, decls := range controls {
		imports := "import (\n\t\"errors\"\n\t\"fmt\"\n\t\"regexp\"\n\t\"time\"\n)\n\n"
		if pkg == "o" || pkg == "q" || pkg == "x" {
			imports = ""
		}
		files[pkg+"/plain_test.go"], files[pkg+"/tag_test.go"] = plainTestFile(pkg), taggedTestFile(pkg, imports+decls)
		pkgs = append(pkgs, "./"+pkg)
	}
	files["o/plain_test.go"] = "package o\n\nimport (\n\t\"image/png\"\n\t\"testing\"\n)\n\nfunc TestPlain_o(t *testing.T) { _ = png.Decode }\n"
	sort.Strings(pkgs)
	got := runDiscoveryFixture(t, files, "run_integration_packages --dry-run . fixture "+strings.Join(pkgs, " ")+`
for k in b g; do
  DAILY_INTEGRATION_PACKAGE_KEY=${k} DAILY_TEST_SHARD_COUNT=1
  discover_daily_tests
  printf 'SPLITHOOK %s [%s]\n' "${k}" "${DAILY_UNFILTERED}"
done
`)
	unfiltered := map[string]bool{}
	for _, l := range strings.Split(got, "\n") {
		if rest, ok := strings.CutPrefix(l, "  SHARD-UNFILTERED "); ok {
			key, finding, _ := strings.Cut(rest, " ")
			if want[key] != finding {
				t.Errorf("unexpected unfiltered line %q", l)
			}
			unfiltered[key] = true
		}
	}
	for key := range want {
		if !unfiltered[key] {
			t.Errorf("package %s has a package-level side effect (%s) in a tag-only file but is not unfiltered", key, want[key])
		}
		if unfiltered[key] && !strings.Contains(got, "  SHARD-TEST "+key+" TestPlain_"+key+"\n") {
			t.Errorf("package %s is unfiltered but its untagged test is not selected: it must run whole", key)
		}
	}
	for _, key := range []string{"f", "g", "o", "q", "x"} {
		if unfiltered[key] || !strings.Contains(got, "  SHARD-TEST "+key+" TestTagged_"+key+"\n") {
			t.Errorf("package %s has only pure or reviewed package-level calls and must stay filtered:\n%s", key, got)
		}
		if strings.Contains(got, "  SHARD-TEST "+key+" TestPlain_"+key) {
			t.Errorf("package %s runs its plain test in the shard", key)
		}
	}
	for _, l := range []string{"SPLITHOOK b [b/tag_test.go " + helperKey + "]", "SPLITHOOK g []"} {
		if !strings.Contains(got, l+"\n") {
			t.Errorf("split planner: missing %q", l)
		}
	}
}

func TestReviewedSideEffectTableRefusesAnEmptyReasonAndEveryMalformedRow(t *testing.T) {
	t.Parallel()
	for name, table := range map[string]string{
		"empty reason (third column blank)": "a/tag_test.go\tvar-call:helper\t\n",
		"two columns":                       "a/tag_test.go\tvar-call:helper\n",
		"repeated row":                      "a/tag_test.go\tvar-call:helper\tr\na/tag_test.go\tvar-call:helper\tr\n",
	} {
		res := discoveryHarness(t, map[string]string{sideEffectTable: table, "a/plain_test.go": plainTestFile("a"), "a/tag_test.go": taggedTestFile("a", "")},
			"", "run_integration_packages --dry-run . fixture ./a")
		if res.code == 0 || !strings.Contains(res.stdout+res.stderr, "gotestfuncs") {
			t.Errorf("%s: a malformed reviewed table was accepted (exit %d): %s%s", name, res.code, res.stdout, res.stderr)
		}
	}
}

func tagOnlyTestFiles(t *testing.T) []string {
	t.Helper()
	root := rootOf(t)
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	mod := freeSpec{dir: root, env: env}.run("go", "list", "-mod=readonly", "-m")
	if mod.code != 0 {
		t.Fatalf("go list -m: %s%s", mod.stdout, mod.stderr)
	}
	modpath := strings.TrimSpace(mod.stdout)
	list := func(tags ...string) map[string][]string {
		args := append([]string{"list", "-e", "-mod=readonly"}, tags...)
		args = append(args, "-f={{.ImportPath}}{{range .TestGoFiles}}\t{{.}}{{end}}{{range .XTestGoFiles}}\t{{.}}{{end}}")
		for _, k := range sortedKeys(expectedPackages(t)) {
			args = append(args, "./"+k)
		}
		res := freeSpec{dir: root, env: env}.run("go", args...)
		if res.code != 0 {
			t.Fatalf("go list %v: %s%s", tags, res.stdout, res.stderr)
		}
		out := map[string][]string{}
		for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
			f := strings.Split(l, "\t")
			out[strings.TrimPrefix(f[0], modpath+"/")] = f[1:]
		}
		return out
	}
	withTag, plain := list("-tags=integration"), list()
	var files []string
	for pkg, listed := range withTag {
		inPlain := map[string]bool{}
		for _, f := range plain[pkg] {
			inPlain[f] = true
		}
		for _, f := range listed {
			if !inPlain[f] {
				files = append(files, filepath.Join(root, pkg, f))
			}
		}
	}
	sort.Strings(files)
	if len(files) < 500 {
		t.Fatalf("only %d tag-only test files listed: a truncated listing cannot make the comparisons below true", len(files))
	}
	return files
}

// unfilteredTestPackages are the packages the integration shards run whole: seven TestMain packages (the auth workload
// ones joined on main with CHAOS-10980) and two init packages,
// which no reviewed row can cover, internal/fixturescli, whose init registers demo repositories, and three packages whose
// tag-only files import the clickhouse-go/v2 root package (it registers the "clickhouse" database/sql driver) while their
// plain test binary does not load it. The list is written out on purpose: a new entry means a package gave up its filter,
// and that must be a decision.
var unfilteredTestPackages = []string{
	"internal/auth/identity", "internal/auth/identity/oidc", "internal/auth/identity/saml", "internal/auth/membership", "internal/auth/org",
	"internal/auth/usersbackfill", "internal/auth/workload", "internal/auth/workloadexchange", "internal/fixturescli", "internal/jobs/metrics/changefailure", "internal/jobs/metrics/remaining", "internal/queryapi/scancensus", "internal/storage/clickhouse/liverow", "internal/storage/postgres",
}

func TestRealTreeKeepsExactlyTheHookPackagesUnfilteredAndNoReviewedRowIsStale(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	files := tagOnlyTestFiles(t)
	run := func(flags ...string) []string {
		args := append(append([]string{"run", "-mod=readonly", "./ci/tools/gotestfuncs/main.go"}, flags...), files...)
		res := freeSpec{dir: root, env: env}.run("go", args...)
		if res.code != 0 {
			t.Fatalf("gotestfuncs %v: exit %d: %s%s", flags, res.code, res.stdout, res.stderr)
		}
		return strings.Split(strings.TrimSpace(res.stdout), "\n")
	}
	hooked := map[string]bool{}
	for _, l := range run("-root", root, "-reviewed", filepath.Join(root, sideEffectTable)) {
		if f := strings.Split(l, "\x1f"); len(f) == 3 && f[0] == "H" {
			hooked[filepath.ToSlash(strings.TrimPrefix(filepath.Dir(f[1]), root+string(filepath.Separator)))] = true
		}
	}
	if !sameSet(hooked, setOf(unfilteredTestPackages)) {
		t.Errorf("unfiltered packages = %v, want exactly %v (a package-level side effect needs a reviewed row, or the package gives up its filter)", sortedKeys(hooked), unfilteredTestPackages)
	}

	// A deny-import row is keyed by the package directory (the detector also requires the package's plain test binary to
	// load the import, or the finding returns and the package shows in the unfiltered set above).
	found := map[string]bool{}
	for _, l := range run("-findings") {
		if f := strings.Split(l, "\x1f"); len(f) == 3 && f[0] == "F" {
			file := strings.TrimPrefix(f[1], root+string(filepath.Separator))
			if strings.HasPrefix(f[2], "deny-import:") {
				file = filepath.ToSlash(filepath.Dir(file))
			}
			found[file+"\t"+f[2]] = true
		}
	}
	reviewed := map[string]bool{}
	for n, line := range strings.Split(readText(t, filepath.Join(root, sideEffectTable)), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) != 3 || strings.TrimSpace(cols[2]) == "" {
			t.Fatalf("%s:%d: want <path><TAB><finding><TAB><reason>", sideEffectTable, n+1)
		}
		key := cols[0] + "\t" + cols[1]
		reviewed[key] = true
		if !found[key] {
			t.Errorf("%s:%d: stale row: %s has no finding %s in an integration-only file", sideEffectTable, n+1, cols[0], cols[1])
		}
		if _, err := os.Stat(filepath.Join(root, cols[0])); err != nil {
			t.Errorf("%s:%d: %v", sideEffectTable, n+1, err)
		}
	}
	for key := range found {
		path, what, _ := strings.Cut(key, "\t")
		dir := filepath.ToSlash(filepath.Dir(path))
		if strings.HasPrefix(what, "deny-import:") {
			dir = path
		}
		if !reviewed[key] && !hooked[dir] {
			t.Errorf("finding %q is neither reviewed nor in an unfiltered package", key)
		}
	}
}

func setOf(list []string) map[string]bool {
	m := map[string]bool{}
	for _, s := range list {
		m[s] = true
	}
	return m
}

func TestGotestfuncsMatchesReviewedRowsForRelativeFileArguments(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	tool := filepath.Join(root, "ci", "tools", "gotestfuncs", "main.go")
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	for name, reviewed := range map[string]bool{"a reviewed row suppresses the finding": true, "no row keeps the finding": false} {
		dir := t.TempDir()
		table := ""
		if reviewed {
			table = "tag_test.go\t" + helperKey + "\tread helper: returns a literal; no global write, no registration, no I/O\n"
		}
		for rel, text := range map[string]string{
			"go.mod":      "module fixture\n\ngo 1.22\n",
			"tag_test.go": taggedTestFile("a", "func helper() int { return 1 }\n\nvar x = helper()\n"),
			"table.tsv":   table,
		} {
			if err := os.WriteFile(filepath.Join(dir, rel), []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		res := freeSpec{dir: dir, env: env}.run("go", "run", tool, "-root", dir, "-reviewed", filepath.Join(dir, "table.tsv"), "tag_test.go")
		if res.code != 0 {
			t.Fatalf("%s: exit %d: %s%s", name, res.code, res.stdout, res.stderr)
		}
		if got := strings.Contains(res.stdout, "var-call:helper"); got == reviewed {
			t.Errorf("%s: the relative file argument and the absolute -root disagree: %q", name, res.stdout)
		}
	}
}

func TestOracleAcceptsAnEffectOnlyReceiptOnlyWhenTheSameFindingIsParsedIndependently(t *testing.T) {
	t.Parallel()
	facts := taggedFacts{
		tagged:   map[string]map[string]bool{"k": {"TestA": true}, "n": {"TestB": true}},
		untagged: map[string]map[string]bool{"k": {}, "n": {}},
		hook:     map[string]string{},
		effect:   map[string]string{"k": "k/f_test.go var-call:helper"},
	}
	keys := map[string]bool{"k": true}
	// An unfiltered package selects every name it declares (SHARD-TEST lines for all of them).
	whole := "  SHARD-TEST k TestA\n  SHARD-REGEX . ^(TestA)$\n"
	if err := checkTaggedSelection("  SHARD-UNFILTERED k k/f_test.go var-call:helper\n"+whole, keys, facts); err != nil {
		t.Errorf("a correct effect-only receipt was refused: %v", err)
	}
	for name, stdout := range map[string]string{
		"a forged finding":  "  SHARD-UNFILTERED k k/f_test.go var-call:other\n" + whole,
		"a forged file":     "  SHARD-UNFILTERED k k/g_test.go var-call:helper\n" + whole,
		"a missing receipt": "  SHARD-TEST k TestA\n  SHARD-REGEX . ^(TestA)$\n",
		"a receipt for a package without an effect": "  SHARD-UNFILTERED n n/f_test.go var-call:helper\n",
	} {
		k := map[string]bool{"k": true, "n": name == "a receipt for a package without an effect"}
		if name == "a receipt for a package without an effect" {
			stdout += "  SHARD-UNFILTERED k k/f_test.go var-call:helper\n" + whole
		}
		if err := checkTaggedSelection(stdout, k, facts); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// denyImports is the frozen list of import paths whose init registers into a process-wide registry (acp ruling on
// CHAOS-11429): a test file that imports one by name is a side effect, as a blank import is. lib/pq and go-sqlite3 are
// not in go.mod, so no pinned init can be cited and they are not listed.
var denyImports = []string{
	"net/http/pprof", "expvar", "image/png", "image/jpeg", "image/gif",
	"github.com/jackc/pgx/v5/stdlib", "github.com/ClickHouse/clickhouse-go/v2", "github.com/prometheus/client_golang/prometheus",
}

func TestTheDenyListOfRegisteringImportsIsFrozenAndCited(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	res := freeSpec{dir: root, env: inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")}.run("go", "run", "-mod=readonly", "./ci/tools/gotestfuncs/main.go", "-deny-list")
	if res.code != 0 {
		t.Fatalf("gotestfuncs -deny-list: exit %d: %s%s", res.code, res.stdout, res.stderr)
	}
	var got []string
	for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
		path, cite, ok := strings.Cut(l, "\t")
		if !ok || !regexp.MustCompile(`\.go:\d+`).MatchString(cite) {
			t.Errorf("deny-list line %q has no file:line cite of the init registration", l)
		}
		got = append(got, path)
	}
	sort.Strings(got)
	want := append([]string(nil), denyImports...)
	sort.Strings(want)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("deny list = %v, want exactly %v", got, want)
	}
}

// The acp's reviewer fixtures: each runs `gotestfuncs -findings` on a tag-only file and compares every F record.
func TestGotestfuncsReviewerFixturesAreFindings(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	tool := filepath.Join(root, "ci", "tools", "gotestfuncs", "main.go")
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	const tag = "//go:build integration\n\n"
	for _, tc := range []struct {
		name   string
		files  map[string]string
		target string
		want   []string
	}{
		{"a: a type name that is also a var in the package is not a type", map[string]string{
			"p.go":      "package p\n\ntype T struct{}\n",
			"p_test.go": tag + "package p\n\nvar fn = func() int { return 1 }\n\nvar T = &fn\n\nvar _ = (*T)(1)\n",
		}, "p_test.go", []string{"var-call:(...)#1"}},
		{"b: a type declared only in another package's file is not a type", map[string]string{
			"ignored.go": "//go:build ignore\n\npackage main\n\ntype T struct{}\n",
			"p_test.go":  tag + "package p\n\nvar _ = (*T)(nil)\n",
		}, "p_test.go", []string{"var-call:(...)#1"}},
		{"b2: a type declared in a file of the same package is a type", map[string]string{
			"p.go":      "package p\n\ntype T struct{}\n",
			"p_test.go": tag + "package p\n\nvar _ = (*T)(nil)\n",
		}, "p_test.go", nil},
		{"c: Sprintf and Errorf run a String method of an argument", map[string]string{
			"p_test.go": tag + "package p\n\nimport \"fmt\"\n\nvar hits int\n\ntype S struct{}\n\nfunc (S) String() string { hits++; return \"x\" }\n\nvar _ = fmt.Sprintf(\"%v\", S{})\n\nvar _ = fmt.Errorf(\"%v\", S{})\n",
		}, "p_test.go", []string{"var-call:fmt.Sprintf#1", "var-call:fmt.Errorf#1"}},
		{"c2: Sprintf and Errorf with literal arguments are pure", map[string]string{
			"p_test.go": tag + "package p\n\nimport \"fmt\"\n\nvar _ = fmt.Sprintf(\"%d-%s\", 1, \"a\")\n\nvar _ = fmt.Errorf(\"x \" + \"y\")\n",
		}, "p_test.go", nil},
		{"d: unnamed callees are numbered each", map[string]string{
			"p_test.go": tag + "package p\n\nvar fns = []func() int{func() int { return 1 }, func() int { return 2 }}\n\nfunc id[T any](v T) T { return v }\n\nvar _ = fns[0]()\n\nvar _ = fns[1]()\n\nvar _ = id[int](1)\n",
		}, "p_test.go", []string{"var-call:?#1", "var-call:?#2", "var-call:?#3"}},
		{"e: two calls of one stdlib callee are numbered", map[string]string{
			"p_test.go": tag + "package p\n\nimport \"strconv\"\n\nvar b = make([]byte, 0, 8)\n\nvar _ = strconv.AppendInt(b, 1, 10)\n\nvar _ = strconv.AppendInt(b, 2, 10)\n",
		}, "p_test.go", []string{"var-call:strconv.AppendInt#1", "var-call:strconv.AppendInt#2"}},
		{"n: a package var named nil is not the literal nil", map[string]string{
			"p_test.go": tag + "package p\n\nimport \"fmt\"\n\nvar hits int\n\ntype S struct{}\n\nfunc (S) String() string { hits++; return \"x\" }\n\nvar nil = S{}\n\nvar _ = fmt.Sprintf(\"%v\", nil)\n",
		}, "p_test.go", []string{"builtin-shadow:nil", "var-call:fmt.Sprintf#1"}},
		{"n2: a package var named true is not the literal true", map[string]string{
			"p_test.go": tag + "package p\n\nimport \"fmt\"\n\nvar hits int\n\ntype S struct{}\n\nfunc (S) String() string { hits++; return \"x\" }\n\nvar true = S{}\n\nvar _ = fmt.Errorf(\"%v\", true)\n",
		}, "p_test.go", []string{"builtin-shadow:true", "var-call:fmt.Errorf#1"}},
		{"n3: a package type named int is a shadowed predeclared type", map[string]string{
			"p_test.go": tag + "package p\n\ntype S struct{}\n\ntype int = S\n\nvar _ = int(S{})\n",
		}, "p_test.go", []string{"builtin-shadow:int"}},
		{"n4: every universe name counts, not a hand list", map[string]string{
			"p_test.go": tag + "package p\n\ntype comparable struct{}\n\nfunc recover() int { return 1 }\n",
		}, "p_test.go", []string{"builtin-shadow:comparable", "builtin-shadow:recover"}},
		{"n5: an unshadowed true, false and nil stay literals", map[string]string{
			"p_test.go": tag + "package p\n\nimport \"fmt\"\n\nvar _ = fmt.Sprintf(\"%v %v %v\", true, false, nil)\n",
		}, "p_test.go", nil},
		{"g: a package func named append is a shadowed builtin", map[string]string{
			"a.go":      "package p\n\nfunc append(xs []int, v int) []int { return xs }\n",
			"p_test.go": tag + "package p\n\nvar _ = len(\"x\")\n",
		}, "p_test.go", []string{"builtin-shadow:append"}},
	} {
		dir := t.TempDir()
		tc.files["go.mod"] = "module fixture\n\ngo 1.22\n"
		for rel, text := range tc.files {
			if err := os.WriteFile(filepath.Join(dir, rel), []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		res := freeSpec{dir: dir, env: env}.run("go", "run", tool, "-findings", filepath.Join(dir, tc.target))
		if res.code != 0 {
			t.Fatalf("%s: exit %d: %s%s", tc.name, res.code, res.stdout, res.stderr)
		}
		var got []string
		for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
			if f := strings.Split(l, "\x1f"); len(f) == 3 && f[0] == "F" {
				got = append(got, f[2])
			}
		}
		if strings.Join(got, " ") != strings.Join(tc.want, " ") {
			t.Errorf("%s: findings %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The local helpers of the planted packages are keyed by name and the hash of their declaration.
var (
	helperKey   = "var-call:helper@" + declHash("func helper() int { return 1 }")
	registerKey = "var-call:register@" + declHash("func register() int { return 1 }")
	otherKey    = "var-call:other@" + declHash("func other() int { return 2 }")
)

func declHash(decl string) string {
	sum := sha256.Sum256([]byte(decl))
	return fmt.Sprintf("%x", sum[:4])
}

// acp's P2-C conditions: a callee is keyed by name only when it is a func declaration of the same package, and the key
// carries the hash of that declaration's source; a func-typed var is numbered; calls in a helper's arguments are findings.
func TestGotestfuncsKeysAHelperByNameAndBodyAndNumbersTheRest(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	tool := filepath.Join(root, "ci", "tools", "gotestfuncs", "main.go")
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	const tag = "//go:build integration\n\n"
	helper1, helper2 := "func helper() int { return 1 }", "func helper() int { return 2 }"
	vb, mutate := "func vb(s string) string { return s }", "func mutate() string { hits++; return \"x\" }"
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{"a func declaration is keyed by name and the hash of its source", map[string]string{
			"p_test.go": tag + "package p\n\n" + helper1 + "\n\nvar _ = helper()\n"}, []string{"var-call:helper@" + declHash(helper1)}},
		{"a body edit changes the key", map[string]string{
			"p_test.go": tag + "package p\n\n" + helper2 + "\n\nvar _ = helper()\n"}, []string{"var-call:helper@" + declHash(helper2)}},
		{"a declaration in another file of the package counts", map[string]string{
			"h.go":      "package p\n\n" + helper1 + "\n",
			"p_test.go": tag + "package p\n\nvar _ = helper()\n"}, []string{"var-call:helper@" + declHash(helper1)}},
		{"a declaration in another package's file does not", map[string]string{
			"ignored.go": "//go:build ignore\n\npackage main\n\n" + helper1 + "\n",
			"p_test.go":  tag + "package p\n\nvar helper func() int\n\nvar _ = helper()\n"}, []string{"var-call:helper#1"}},
		{"a func-typed var is numbered, not keyed by name", map[string]string{
			"p_test.go": tag + "package p\n\nvar f = func() int { return 1 }\n\nvar _ = f()\n\nvar _ = f()\n"}, []string{"var-call:f#1", "var-call:f#2"}},
		{"a call in a helper's arguments is its own finding", map[string]string{
			"p_test.go": tag + "package p\n\nvar hits int\n\n" + vb + "\n\n" + mutate + "\n\nvar _ = vb(mutate())\n"},
			[]string{"var-call:vb@" + declHash(vb), "var-call:mutate@" + declHash(mutate)}},
	} {
		dir := t.TempDir()
		tc.files["go.mod"] = "module fixture\n\ngo 1.22\n"
		for rel, text := range tc.files {
			if err := os.WriteFile(filepath.Join(dir, rel), []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		res := freeSpec{dir: dir, env: env}.run("go", "run", tool, "-findings", filepath.Join(dir, "p_test.go"))
		if res.code != 0 {
			t.Fatalf("%s: exit %d: %s%s", tc.name, res.code, res.stdout, res.stderr)
		}
		var got []string
		for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
			if f := strings.Split(l, "\x1f"); len(f) == 3 && f[0] == "F" {
				got = append(got, f[2])
			}
		}
		if strings.Join(got, " ") != strings.Join(tc.want, " ") {
			t.Errorf("%s: findings %v, want %v", tc.name, got, tc.want)
		}
	}
}
