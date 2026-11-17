package testshard_test

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13833: a race leg runs a package of ci/go_race_splits.tsv as PARTS parts of its top-level test names, cut by
// ci/go_race_split_weights.tsv with the cutter of the test legs (ci/go_race_shard.awk names mode, restpart = the last part).
// These cells hold the tables whole and the cut exact: every top-level test of the -race build in exactly one part, a test with
// no row red here (and, unmeasured, in the last part at run time, never dropped), the two heaviest tests in different parts, and
// the anchored -run pattern of each part matching its own names only.

type raceSplitTables struct {
	parts map[string]int
	rows  map[string]map[string]int
}

func tsvRows(t *testing.T, path string) [][]string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]string
	for _, l := range strings.Split(string(b), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		out = append(out, strings.Split(l, "\t"))
	}
	return out
}

func readRaceSplitTables(t *testing.T, root string) raceSplitTables {
	t.Helper()
	tab := raceSplitTables{parts: map[string]int{}, rows: map[string]map[string]int{}}
	for _, f := range tsvRows(t, filepath.Join(root, "ci", "go_race_splits.tsv")) {
		n, err := strconv.Atoi(f[len(f)-1])
		if len(f) != 2 || err != nil || n < 2 {
			t.Fatalf("ci/go_race_splits.tsv: row %q is not package<TAB>parts (parts >= 2)", strings.Join(f, "\t"))
		}
		tab.parts[f[0]] = n
	}
	for _, f := range tsvRows(t, filepath.Join(root, "ci", "go_race_split_weights.tsv")) {
		n, err := strconv.Atoi(f[len(f)-1])
		if len(f) != 3 || err != nil || n < 1 {
			t.Fatalf("ci/go_race_split_weights.tsv: row %q is not package<TAB>TestName<TAB>seconds (>= 1)", strings.Join(f, "\t"))
		}
		if tab.rows[f[0]] == nil {
			tab.rows[f[0]] = map[string]int{}
		}
		tab.rows[f[0]][f[1]] = n
	}
	return tab
}

// raceListedTests: the top-level Test functions of the package's _test.go files that the -race build compiles (build tag race
// on), sorted: what `go test -race -list` reports, read from the source without a race build.
func raceListedTests(t *testing.T, root, key string) []string {
	t.Helper()
	// The walk names its directory literally (internal/cigates/affected/walks.tsv): a new race split package adds its own case.
	var dir string
	switch key {
	case "internal/cigates/testshard":
		dir = filepath.Join(root, "internal", "cigates", "testshard")
	case "internal/api/policy":
		dir = filepath.Join(root, "internal", "api", "policy")
	case "internal/cigates/scripts":
		dir = filepath.Join(root, "internal", "cigates", "scripts")
	default:
		t.Fatalf("race split package %s has no listing walk here: add its directory", key)
	}
	ctx := build.Default
	ctx.BuildTags = []string{"race"}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	test := regexp.MustCompile(`^Test[^a-z]`)
	var names []string
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		if ok, err := ctx.MatchFile(dir, e.Name()); err != nil || !ok {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			name, ok := testFuncName(d)
			if ok && name != "TestMain" && (name == "Test" || test.MatchString(name)) {
				names = append(names, name)
			}
		}
	}
	sort.Strings(names)
	return names
}

// testFuncName: the name of a top-level func TestX(t *testing.T) declaration.
func testFuncName(d ast.Decl) (string, bool) {
	fd, ok := d.(*ast.FuncDecl)
	if !ok || fd.Recv != nil || fd.Type.Params == nil || len(fd.Type.Params.List) != 1 || len(fd.Type.Params.List[0].Names) > 1 {
		return "", false
	}
	star, ok := fd.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return "", false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "T" {
		return "", false
	}
	if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "testing" {
		return "", false
	}
	return fd.Name.Name, true
}

// raceSplitCut runs the race legs' cut (ci/go_race_shard.awk names mode with restpart = parts) over the listed names: part -> names.
func raceSplitCut(t *testing.T, root, key string, parts int, listed []string, weights string) map[int][]string {
	t.Helper()
	cmd := exec.Command("awk", "-v", "cutkey="+key, "-v", "cutparts="+strconv.Itoa(parts), "-v", "restpart="+strconv.Itoa(parts),
		"-v", "tests="+weights, "-f", filepath.Join(root, "ci", "go_race_shard.awk"))
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	cmd.Stdin = strings.NewReader(strings.Join(listed, "\n") + "\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("the race cut of %s: %v", key, err)
	}
	cut := map[int][]string{}
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		name, p, ok := strings.Cut(l, "\t")
		n, err := strconv.Atoi(p)
		if !ok || err != nil {
			t.Fatalf("the race cut of %s printed %q", key, l)
		}
		cut[n] = append(cut[n], name)
	}
	return cut
}

// anchoredPartPattern is the -run pattern check_go.sh gives a race part: its top-level names, anchored.
func anchoredPartPattern(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = regexp.QuoteMeta(n)
	}
	return "^(" + strings.Join(q, "|") + ")$"
}

// raceSplitDefects: every listed name in exactly one part (by the cut and by the parts' -run patterns), every part non-empty, every
// listed name with a row and every row a listed name, the two heaviest rows in different parts.
func raceSplitDefects(key string, parts int, listed []string, cut map[int][]string, rows map[string]int, pattern func([]string) string) []string {
	var out []string
	in := map[string]int{}
	for p := 1; p <= parts; p++ {
		if len(cut[p]) == 0 {
			out = append(out, fmt.Sprintf("%s part %d/%d selects no test", key, p, parts))
		}
		for _, n := range cut[p] {
			in[n]++
		}
	}
	for p := range cut {
		if p < 1 || p > parts {
			out = append(out, fmt.Sprintf("%s: a name is cut to part %d, outside 1..%d", key, p, parts))
		}
	}
	listedSet := map[string]bool{}
	for _, n := range listed {
		listedSet[n] = true
		switch c := in[n]; {
		case c == 0:
			out = append(out, fmt.Sprintf("%s: %s runs in no part", key, n))
		case c > 1:
			out = append(out, fmt.Sprintf("%s: %s runs in %d parts", key, n, c))
		}
		if _, ok := rows[n]; !ok {
			out = append(out, fmt.Sprintf("%s: %s has no row in ci/go_race_split_weights.tsv (a new test needs its row)", key, n))
		}
		matched := 0
		for p := 1; p <= parts; p++ {
			if len(cut[p]) > 0 && regexp.MustCompile(pattern(cut[p])).MatchString(n) {
				matched++
			}
		}
		if matched != 1 {
			out = append(out, fmt.Sprintf("%s: the -run patterns of %d parts match %s", key, matched, n))
		}
	}
	for n := range in {
		if !listedSet[n] {
			out = append(out, fmt.Sprintf("%s: part name %s is not a listed test", key, n))
		}
	}
	for n := range rows {
		if !listedSet[n] {
			out = append(out, fmt.Sprintf("%s: the row of %s names no listed test", key, n))
		}
	}
	type kv struct {
		n string
		v int
	}
	var heavy []kv
	for n, v := range rows {
		heavy = append(heavy, kv{n, v})
	}
	sort.Slice(heavy, func(i, j int) bool {
		return heavy[i].v > heavy[j].v || (heavy[i].v == heavy[j].v && heavy[i].n < heavy[j].n)
	})
	if len(heavy) >= 2 {
		partOf := map[string]int{}
		for p, ns := range cut {
			for _, n := range ns {
				partOf[n] = p
			}
		}
		if partOf[heavy[0].n] == partOf[heavy[1].n] {
			out = append(out, fmt.Sprintf("%s: the two heaviest tests %s (%d s) and %s (%d s) share part %d", key, heavy[0].n, heavy[0].v, heavy[1].n, heavy[1].v, partOf[heavy[0].n]))
		}
	}
	sort.Strings(out)
	return out
}

func TestTheRaceSplitTablesAreSortedWholeAndNeverSolo(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	tab := readRaceSplitTables(t, root)
	if len(tab.parts) == 0 {
		t.Fatal("ci/go_race_splits.tsv has no row: the cells would hold nothing")
	}
	for f, path := range map[string]string{
		"go_race_splits.tsv":        filepath.Join(root, "ci", "go_race_splits.tsv"),
		"go_race_split_weights.tsv": filepath.Join(root, "ci", "go_race_split_weights.tsv"),
	} {
		var keys []string
		for _, r := range tsvRows(t, path) {
			keys = append(keys, strings.Join(r[:len(r)-1], "\t"))
		}
		if !sort.StringsAreSorted(keys) {
			t.Errorf("ci/%s is not sorted (LC_ALL=C)", f)
		}
		for i := 1; i < len(keys); i++ {
			if keys[i] == keys[i-1] {
				t.Errorf("ci/%s lists %q twice", f, keys[i])
			}
		}
	}
	elsewhere := map[string]string{}
	for f, path := range map[string]string{
		"go_race_solo.tsv":           filepath.Join(root, "ci", "go_race_solo.tsv"),
		"race_excluded_packages.tsv": filepath.Join(root, "ci", "race_excluded_packages.tsv"),
	} {
		for _, r := range tsvRows(t, path) {
			elsewhere[r[0]] = f
		}
	}
	for key, parts := range tab.parts {
		if f, ok := elsewhere[key]; ok {
			t.Errorf("race split package %s is also in ci/%s", key, f)
		}
		if len(tab.rows[key]) < parts {
			t.Errorf("race split package %s has %d row(s) in ci/go_race_split_weights.tsv, fewer than its %d parts", key, len(tab.rows[key]), parts)
		}
	}
	for key := range tab.rows {
		if _, ok := tab.parts[key]; !ok {
			t.Errorf("ci/go_race_split_weights.tsv has rows of %s, which is no race split package", key)
		}
	}
}

func TestEveryRaceSplitTestRunsInExactlyOnePart(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	tab := readRaceSplitTables(t, root)
	weights := filepath.Join(root, "ci", "go_race_split_weights.tsv")
	for key, parts := range tab.parts {
		listed := raceListedTests(t, root, key)
		if len(listed) < parts {
			t.Fatalf("%s: %d listed test(s) for %d parts", key, len(listed), parts)
		}
		cut := raceSplitCut(t, root, key, parts, listed, weights)
		if d := raceSplitDefects(key, parts, listed, cut, tab.rows[key], anchoredPartPattern); len(d) > 0 {
			t.Errorf("%d defect(s):\n%s", len(d), strings.Join(d, "\n"))
		}
		for p := 1; p <= parts; p++ {
			sum := 0
			for _, n := range cut[p] {
				sum += tab.rows[key][n]
			}
			t.Logf("%s part %d/%d: %d test(s), %d s of rows", key, p, parts, len(cut[p]), sum)
		}
	}
}

func TestTheRaceSplitGuardPlantedDefectsAreCaught(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	tab := readRaceSplitTables(t, root)
	weights := filepath.Join(root, "ci", "go_race_split_weights.tsv")
	key := "internal/cigates/testshard" // gitleaks:allow
	parts, ok := tab.parts[key]
	if !ok {
		t.Fatalf("%s is no race split package: the plants have nothing to plant into", key)
	}
	listed := raceListedTests(t, root, key)
	copyCut := func() map[int][]string {
		c := map[int][]string{}
		for p, ns := range raceSplitCut(t, root, key, parts, listed, weights) {
			c[p] = append([]string{}, ns...)
		}
		return c
	}
	expect := func(name, want string, d []string) {
		t.Helper()
		for _, x := range d {
			if strings.Contains(x, want) {
				return
			}
		}
		t.Errorf("plant %q: no defect names %q (got %v)", name, want, d)
	}
	// a test in two parts
	c := copyCut()
	c[2] = append(c[2], c[1][0])
	expect("a test in two parts", "runs in 2 parts", raceSplitDefects(key, parts, listed, c, tab.rows[key], anchoredPartPattern))
	// a test in no part
	c = copyCut()
	dropped := c[1][0]
	c[1] = c[1][1:]
	expect("a test in no part", dropped+" runs in no part", raceSplitDefects(key, parts, listed, c, tab.rows[key], anchoredPartPattern))
	// a new test with no row: red here, and the cut puts it in the last part (never dropped)
	const planted = "TestZZPlantedNewTestWithNoRow"
	withNew := append(append([]string{}, listed...), planted)
	c = raceSplitCut(t, root, key, parts, withNew, weights)
	last := false
	for _, n := range c[parts] {
		if n == planted {
			last = true
		}
	}
	if !last {
		t.Errorf("a listed test with no row must go to the last part %d, got %v", parts, c)
	}
	expect("a new test with no row", planted+" has no row", raceSplitDefects(key, parts, withNew, c, tab.rows[key], anchoredPartPattern))
	// a name that is a prefix of another: anchored, each lands once; unanchored, the shorter name's pattern also matches the longer
	prefixListed := []string{"TestAPrefix", "TestAPrefixLonger", "TestB"}
	prefixRows := map[string]int{"TestAPrefix": 3, "TestAPrefixLonger": 2, "TestB": 1}
	prefixCut := map[int][]string{1: {"TestAPrefix"}, 2: {"TestAPrefixLonger", "TestB"}}
	if d := raceSplitDefects("prefix", 2, prefixListed, prefixCut, prefixRows, anchoredPartPattern); len(d) > 0 {
		t.Errorf("anchored -run patterns: a name that is a prefix of another must land once, got %v", d)
	}
	unanchored := func(ns []string) string { return "^(" + strings.Join(ns, "|") + ")" }
	expect("an unanchored -run pattern", "patterns of 2 parts match TestAPrefixLonger", raceSplitDefects("prefix", 2, prefixListed, prefixCut, prefixRows, unanchored))
	// the two heaviest tests in one part
	heavyRows := map[string]int{"TestAPrefix": 9, "TestAPrefixLonger": 8, "TestB": 1}
	expect("the two heaviest tests share a part", "share part 2", raceSplitDefects("prefix", 2, prefixListed,
		map[int][]string{1: {"TestB"}, 2: {"TestAPrefix", "TestAPrefixLonger"}}, heavyRows, anchoredPartPattern))
}

// The race leg's part call, read from ci/check_go.sh: the -race listing, the cut with the last part as restpart, the anchored
// -run of the part's names, the part run after the shard's packages and before the solo call (solo stays last), and the
// unweighted guard over the part's events.
func TestTheRaceLegRunsEachPartAloneByItsAnchoredNames(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	for _, want := range []string{
		`names="$(test_split_listed_names "${module_dir}" "${pkg}" "${parts}" -race)"`,
		`-v cutkey="${key}" -v cutparts="${parts}" -v cutpart="${part}" -v restpart="${parts}"`,
		`-v splits="${GO_RACE_SPLITS}" -v tests="${GO_RACE_SPLIT_WEIGHTS}" -v rserial="${GO_RACE_SERIAL_WEIGHTING}" -v rcap="$(race_leg_weight_cap "${count}")" -f "${ROOT}/ci/go_race_shard.awk"`,
		`-run "^($(printf '%s\n' "${names}" | paste -sd'|' -))\$" "${pkg}"`,
		`-f "${GO_RACE_UNWEIGHTED_SLOW_AWK:-${ROOT}/ci/go_race_unweighted_slow.awk}" "${partjson}"`,
	} {
		if strings.Count(src, want) != 1 {
			t.Errorf("ci/check_go.sh must hold exactly once: %s", want)
		}
	}
	body, ok := shellFunc(src, "check_race_shard")
	if !ok {
		t.Fatal("ci/check_go.sh: no check_race_shard()")
	}
	main, part, solo := strings.Index(body, `go_test_leg "${racelog}" "${legjson}" -race -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${pkgs[@]}"`),
		strings.Index(body, `for pkg in "${split_items[@]}"; do`), strings.Index(body, `solo, alone: %s`)
	if main < 0 || part < 0 || solo < 0 || !(main < part && part < solo) {
		t.Errorf("check_race_shard must run the split parts after the shard's packages and before the solo call, which stays last (main at %d, parts at %d, solo at %d)", main, part, solo)
	}
}

func shellFunc(src, name string) (string, bool) {
	i := strings.Index(src, "\n"+name+"() {\n")
	if i < 0 {
		return "", false
	}
	j := strings.Index(src[i+1:], "\n}\n")
	if j < 0 {
		return "", false
	}
	return src[i+1 : i+1+j], true
}

// A top-level test of 30 s or more with no row fails its race part, as the providersync slice's guard does.
func TestASlowTestWithNoRowFailsItsRacePart(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	dir := t.TempDir()
	weights := filepath.Join(dir, "weights.tsv")
	if err := os.WriteFile(weights, []byte("TestMeasured\t40\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ev := func(test string, secs float64) string {
		return fmt.Sprintf(`{"Action":"run","Package":"p","Test":%q}`+"\n"+`{"Action":"pass","Package":"p","Test":%q,"Elapsed":%g}`+"\n", test, test, secs)
	}
	for _, c := range []struct {
		name, events string
		want         int
	}{
		{"a measured test of 40 s", ev("TestMeasured", 40), 0},
		{"an unmeasured test of 29 s", ev("TestNew", 29), 0},
		{"an unmeasured test of 31 s", ev("TestNew", 31), 1},
	} {
		f := filepath.Join(dir, strings.ReplaceAll(c.name, " ", "_")+".json")
		if err := os.WriteFile(f, []byte(c.events), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("awk", "-v", "weights="+weights, "-v", "thresh=30", "-v", "label=race split", "-f",
			filepath.Join(root, "ci", "go_race_unweighted_slow.awk"), f)
		err := cmd.Run()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if code != c.want {
			t.Errorf("%s: the guard exited %d, want %d", c.name, code, c.want)
		}
	}
}

// writeRaceSplitLists writes the stand-in's -race listing of every race split package (FAKE_GO_RACE_SPLIT_LISTS).
func writeRaceSplitLists(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	root := repoRoot(t)
	for key := range readRaceSplitTables(t, root).parts {
		names := raceListedTests(t, root, key)
		path := filepath.Join(dir, strings.ReplaceAll(key, "/", "__")+".list")
		if err := os.WriteFile(path, []byte(strings.Join(names, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
