package racescope

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func shippedRows(t *testing.T, root string) []Row {
	t.Helper()
	rows, err := Parse(filepath.Join(root, "ci", "race_excluded_packages.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("ci/race_excluded_packages.tsv lists no package")
	}
	return rows
}

// Every package the -race legs skip is proven free of concurrency in its own files and in its in-module import closure.
func TestEveryRaceExcludedPackageIsProvenFreeOfConcurrency(t *testing.T) {
	root := repoRoot(t)
	problems, err := Check(root, shippedRows(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) > 0 {
		t.Fatalf("%d problem(s):\n%s", len(problems), strings.Join(problems, "\n"))
	}
}

func planFor(t *testing.T, root, plan string, shard int) map[string]bool {
	t.Helper()
	cmd := exec.Command("bash", "ci/check_go.sh", plan, string(rune('0'+shard)), "4")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ci/check_go.sh %s %d 4: %v", plan, shard, err)
	}
	keys := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if f := strings.Split(line, "\t"); len(f) == 3 {
			keys[f[2]] = true
		}
	}
	return keys
}

// The race legs run no listed package and the test legs run every one of them once: the retired receipt is the race run only.
func TestRaceLegsSkipTheListedPackagesAndTestLegsStillRunThem(t *testing.T) {
	root := repoRoot(t)
	rows := shippedRows(t, root)
	race, test := map[string]int{}, map[string]int{}
	for shard := 1; shard <= 4; shard++ {
		for k := range planFor(t, root, "race-shard-plan", shard) {
			race[k]++
		}
		for k := range planFor(t, root, "test-shard-plan", shard) {
			test[k]++
		}
	}
	for _, r := range rows {
		if race[r.Package] != 0 {
			t.Errorf("%s is listed in ci/race_excluded_packages.tsv and still runs in %d race leg(s)", r.Package, race[r.Package])
		}
		if test[r.Package] != 1 {
			t.Errorf("%s runs in %d test leg(s), want exactly 1", r.Package, test[r.Package])
		}
	}
	if len(race) == 0 {
		t.Fatal("the race legs plan no package at all")
	}
}

// A package that is excluded from -race is not also a solo package: the solo row would be dead.
func TestNoRaceExcludedPackageIsAlsoSolo(t *testing.T) {
	root := repoRoot(t)
	solo, err := os.ReadFile(filepath.Join(root, "ci", "go_race_solo.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range shippedRows(t, root) {
		for _, line := range strings.Split(string(solo), "\n") {
			if strings.HasPrefix(line, r.Package+"\t") {
				t.Errorf("%s is excluded from -race and still has a row in ci/go_race_solo.tsv", r.Package)
			}
		}
	}
}

func TestRowsAreSortedUniqueAndWellFormed(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "rows.tsv")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, body := range map[string]string{
		"unsorted":  "b\tnone\t-\tr\na\tnone\t-\tr\n",
		"duplicate": "a\tnone\t-\tr\na\tnone\t-\tr\n",
		"class":     "a\tmaybe\t-\tr\n",
		"columns":   "a\tnone\t-\n",
	} {
		if _, err := Parse(write(body)); err == nil {
			t.Errorf("%s list was accepted", name)
		}
	}
	if _, err := Parse(write("# c\na\tnone\t-\tr\nb\tcovered-by:c\tTestX\tr\n")); err != nil {
		t.Errorf("a well-formed list was refused: %v", err)
	}
}

// module writes a throwaway module and returns its root.
func module(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	files["go.mod"] = "module example.test/m\n\ngo 1.22\n"
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const witnessTest = "package c\n\nimport \"testing\"\n\nfunc TestH(t *testing.T) { _ = CheckTree([]string{\"x\"}, func(string) int { return 0 }) }\n"

const cleanPkg = "package a\n\nfunc F() int { return 1 }\n"
const cleanTest = "package a\n\nimport \"testing\"\n\nfunc TestF(t *testing.T) { _ = F() }\n"

func problemsOf(t *testing.T, files map[string]string, rows ...Row) string {
	t.Helper()
	p, err := Check(module(t, files), rows)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(p)
	return strings.Join(p, "\n")
}

var rowA = Row{"a", "none", "-", "pure"}

func TestAPureSynchronousPackageIsAccepted(t *testing.T) {
	if got := problemsOf(t, map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest}, rowA); got != "" {
		t.Fatalf("a clean package was refused:\n%s", got)
	}
}

// Planted faults: each one turns the guard red.
func TestPlantedConcurrencyInAnExcludedPackageIsRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		files map[string]string
		want  string
	}{
		"goroutine in a package file": {map[string]string{"a/a.go": "package a\n\nfunc F() int { go func() {}(); return 1 }\n", "a/a_test.go": cleanTest}, "go statement"},
		"goroutine in a test file":    {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": "package a\n\nimport \"testing\"\n\nfunc TestF(t *testing.T) { go func() {}() }\n"}, "go statement"},
		"t.Parallel":                  {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": "package a\n\nimport \"testing\"\n\nfunc TestF(t *testing.T) { t.Parallel() }\n"}, "use of .Parallel"},
		"sync import":                 {map[string]string{"a/a.go": "package a\n\nimport \"sync\"\n\nvar mu sync.Mutex\n\nfunc F() int { return 1 }\n", "a/a_test.go": cleanTest}, "imports sync"},
		"channel":                     {map[string]string{"a/a.go": "package a\n\nvar c chan int\n\nfunc F() int { return 1 }\n", "a/a_test.go": cleanTest}, "channel type"},
		"os/exec":                     {map[string]string{"a/a.go": "package a\n\nimport \"os/exec\"\n\nvar _ = exec.Command\n\nfunc F() int { return 1 }\n", "a/a_test.go": cleanTest}, "imports os/exec"},
		"race-only file":              {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "a/r_test.go": "//go:build race\n\npackage a\n"}, "build constraint names race"},
		"timer goroutine":             {map[string]string{"a/a.go": "package a\n\nimport \"time\"\n\nfunc F() int { time.AfterFunc(0, func() {}); return 1 }\n", "a/a_test.go": cleanTest}, "use of .AfterFunc"},
	} {
		if got := problemsOf(t, tc.files, rowA); !strings.Contains(got, tc.want) || !strings.Contains(got, "a: own file") {
			t.Errorf("%s: want a problem naming %q, got:\n%s", name, tc.want, got)
		}
	}
}

func TestAConcurrentInModuleImportIsRefusedDirectAndTransitive(t *testing.T) {
	direct := map[string]string{
		"a/a.go": "package a\n\nimport \"example.test/m/b\"\n\nfunc F() int { return b.G() }\n", "a/a_test.go": cleanTest,
		"b/b.go": "package b\n\nfunc G() int { go func() {}(); return 1 }\n",
	}
	if got := problemsOf(t, direct, rowA); !strings.Contains(got, "a: imports b: b.go:3: go statement") {
		t.Errorf("a direct concurrent import was accepted:\n%s", got)
	}
	transitive := map[string]string{
		"a/a.go": "package a\n\nimport \"example.test/m/b\"\n\nfunc F() int { return b.G() }\n", "a/a_test.go": cleanTest,
		"b/b.go": "package b\n\nimport \"example.test/m/c\"\n\nfunc G() int { return c.H() }\n",
		"c/c.go": "package c\n\nimport \"sync\"\n\nvar mu sync.Mutex\n\nfunc H() int { return 1 }\n",
	}
	if got := problemsOf(t, transitive, rowA); !strings.Contains(got, "a: imports c: c.go:3: imports sync") {
		t.Errorf("a transitive concurrent import was accepted:\n%s", got)
	}
	viaTest := map[string]string{
		"a/a.go": cleanPkg, "a/a_test.go": "package a\n\nimport (\n\t\"testing\"\n\n\t\"example.test/m/b\"\n)\n\nfunc TestF(t *testing.T) { _ = b.G() }\n",
		"b/b.go": "package b\n\nfunc G() int { go func() {}(); return 1 }\n",
	}
	if got := problemsOf(t, viaTest, rowA); !strings.Contains(got, "a: imports b") {
		t.Errorf("a concurrent import made by a test file was accepted:\n%s", got)
	}
}

func coveredModule(witnessFile string) map[string]string {
	return map[string]string{
		"a/a.go": "package a\n\nimport \"example.test/m/c\"\n\nfunc F() int { return c.H() }\n", "a/a_test.go": cleanTest,
		"c/c.go":           "package c\n\nimport \"sync\"\n\nvar mu sync.Mutex\n\nfunc H() int { return 1 }\n",
		"c/" + witnessFile: witnessTest,
	}
}

func TestACoveredByRowAllowsOnlyTheNamedPackageAndOnlyWithItsWitness(t *testing.T) {
	covered := Row{"a", "covered-by:c", "TestH", "the pool lives in c"}
	if got := problemsOf(t, coveredModule("c_test.go"), covered); got != "" {
		t.Fatalf("a covered-by row with its witness was refused:\n%s", got)
	}
	if got := problemsOf(t, coveredModule("c_test.go"), Row{"a", "covered-by:c", "TestGone", "x"}); !strings.Contains(got, "witness test TestGone not found") {
		t.Errorf("a missing witness was accepted:\n%s", got)
	}
	notRace := coveredModule("c_test.go")
	notRace["c/c_test.go"] = "//go:build !race\n\n" + notRace["c/c_test.go"]
	if got := problemsOf(t, notRace, covered); !strings.Contains(got, "compiled out of -race") {
		t.Errorf("a witness compiled out of -race was accepted:\n%s", got)
	}
	if got := problemsOf(t, coveredModule("c_test.go"), covered, Row{"c", "none", "-", "x"}); !strings.Contains(got, "itself excluded") {
		t.Errorf("a covered-by package that is itself excluded was accepted:\n%s", got)
	}
	// The named package is the only exception: the covered package's own concurrency does not excuse another dependency.
	other := coveredModule("c_test.go")
	other["a/a.go"] = "package a\n\nimport (\n\t\"example.test/m/b\"\n\t\"example.test/m/c\"\n)\n\nfunc F() int { return c.H() + b.G() }\n"
	other["b/b.go"] = "package b\n\nfunc G() int { go func() {}(); return 1 }\n"
	if got := problemsOf(t, other, covered); !strings.Contains(got, "a: imports b") || strings.Contains(got, "imports c") {
		t.Errorf("covered-by excused the wrong package:\n%s", got)
	}
	// The excluded package itself gets no exception: a goroutine in its own file stays red.
	own := coveredModule("c_test.go")
	own["a/a_test.go"] = "package a\n\nimport \"testing\"\n\nfunc TestF(t *testing.T) { go func() {}() }\n"
	if got := problemsOf(t, own, covered); !strings.Contains(got, "a: own file: a_test.go:5: go statement") {
		t.Errorf("a goroutine in a covered-by package's own file was accepted:\n%s", got)
	}
}

func TestRowsThatNameNothingRunnableAreRefused(t *testing.T) {
	files := map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "n/n.go": "package n\n"}
	for want, row := range map[string]Row{
		"no such package":                   {"gone", "none", "-", "x"},
		"has no test":                       {"n", "none", "-", "x"},
		"cannot be excluded":                {"internal/providersync", "none", "-", "x"},
		"covered-by package does not exist": {"a", "covered-by:gone", "TestX", "x"},
		"class none takes witness -":        {"a", "none", "TestX", "x"},
	} {
		if got := problemsOf(t, files, row); !strings.Contains(got, want) {
			t.Errorf("want %q, got:\n%s", want, got)
		}
	}
}

func callbackModule(callback string) map[string]string {
	return map[string]string{
		"a/a.go":      "package a\n\nimport \"example.test/m/c\"\n\nfunc F() int {\n\tvar out []string\n\t_ = out\n\treturn c.CheckTree([]string{\"x\"}, " + callback + ")\n}\n",
		"a/a_test.go": cleanTest,
		"c/c.go":      "package c\n\nimport \"sync\"\n\nvar mu sync.Mutex\n\nfunc CheckTree(p []string, read func(string) int) int { return read(p[0]) }\n",
		"c/c_test.go": witnessTest,
	}
}

// The callback of the worker pool must write no captured variable (CHAOS-11413): two workers writing it would be a race that
// only the excluded package's own -race run could see.
func TestACoveredByCallbackThatWritesACapturedVariableIsRefused(t *testing.T) {
	covered := Row{"a", "covered-by:c", "TestH", "pool in c"}
	ok := []string{
		`func(path string) int { n := len(path); n++; return n }`,
		`func(path string) int { var buf []string; buf = append(buf, path); return len(buf) }`,
		`func(path string) int { m := map[string]int{}; m[path] = 1; return m[path] }`,
	}
	for _, cb := range ok {
		if got := problemsOf(t, callbackModule(cb), covered); got != "" {
			t.Errorf("callback %s was refused:\n%s", cb, got)
		}
	}
	bad := map[string]string{
		"append to a captured slice": `func(path string) int { out = append(out, path); return 0 }`,
		"captured counter":           `func(path string) int { out = nil; return 0 }`,
		"captured map or field":      `func(path string) int { out[0] = path; return 0 }`,
		"increment":                  `func(path string) int { out2++; return 0 }`,
		"a variable, not a function": `cb`,
	}
	for name, cb := range bad {
		files := callbackModule(cb)
		files["a/a.go"] = strings.Replace(files["a/a.go"], "\treturn c.CheckTree", "\tvar out2 int\n\tcb := func(string) int { return 0 }\n\t_ = out2\n\treturn c.CheckTree", 1)
		if name == "increment" {
			files["a/a.go"] = strings.Replace(files["a/a.go"], "var out2 int\n", "var out2 int\n", 1)
		}
		if got := problemsOf(t, files, covered); !strings.Contains(got, "callback") {
			t.Errorf("%s: no callback problem, got:\n%s", name, got)
		}
	}
}

// r1 P1: a dot import hides the package qualifier, so a timer goroutine (or any banned call) is a bare identifier.
func TestADotImportInAnExcludedPackageIsRefused(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"dot-imported time in a package file": {"a/a.go": "package a\n\nimport . \"time\"\n\nfunc F() int { AfterFunc(0, func() {}); return 1 }\n", "a/a_test.go": cleanTest},
		"dot-imported time in a test file":    {"a/a.go": cleanPkg, "a/a_test.go": "package a\n\nimport (\n\t. \"time\"\n\t\"testing\"\n)\n\nfunc TestF(t *testing.T) { NewTicker(1) }\n"},
		"dot-imported in a dependency":        {"a/a.go": "package a\n\nimport \"example.test/m/b\"\n\nfunc F() int { return b.G() }\n", "a/a_test.go": cleanTest, "b/b.go": "package b\n\nimport . \"time\"\n\nfunc G() int { AfterFunc(0, func() {}); return 1 }\n"},
	} {
		if got := problemsOf(t, files, rowA); !strings.Contains(got, "dot import") {
			t.Errorf("%s: want a dot import problem, got:\n%s", name, got)
		}
	}
}

// r1 P1: a callback may call only builtins and the listed standard-library packages, so a helper cannot write the captured variable
// for it (a pointer to a captured slice passed to a helper), and a CheckTree reference that is not a direct call cannot hide it.
func TestACoveredByCallbackThatCallsAHelperOrAliasesTheCalleeIsRefused(t *testing.T) {
	covered := Row{"a", "covered-by:c", "TestH", "pool in c"}
	helper := func(cb string) map[string]string {
		files := callbackModule(cb)
		files["a/a.go"] += "\nfunc appendTo(p *[]string, s string) { *p = append(*p, s) }\n\nfunc pure(s string) int { return len(s) }\n"
		return files
	}
	for name, cb := range map[string]string{
		"helper given a pointer to a captured slice":  `func(path string) int { appendTo(&out, path); return 0 }`,
		"helper without a captured argument":          `func(path string) int { return pure(path) }`,
		"method call on a captured value":             `func(path string) int { out2.add(path); return 0 }`,
		"field write":                                 `func(path string) int { st.n = 1; return 0 }`,
		"range assign to a captured variable":         `func(path string) int { for out2 = range path {}; return 0 }`,
		"package-level function that writes a global": `writesGlobal`,
	} {
		files := helper(cb)
		files["a/a.go"] = strings.Replace(files["a/a.go"], "\\treturn c.CheckTree", "\\tvar out2 int\\n\\tst := struct{ n int }{}\\n\\t_, _ = out2, st\\n\\treturn c.CheckTree", 1)
		files["a/a.go"] += "\nvar global []string\n\nfunc writesGlobal(p string) int { global = append(global, p); return 0 }\n"
		files["a/a.go"] = strings.Replace(files["a/a.go"], "var out []string", "var out []string\n\tvar out2 string", 1)
		if got := problemsOf(t, files, covered); !strings.Contains(got, "callback") {
			t.Errorf("%s: no callback problem, got:\n%s", name, got)
		}
	}
	alias := callbackModule(`func(path string) int { return len(path) }`)
	alias["a/a.go"] = strings.Replace(alias["a/a.go"], "return c.CheckTree(", "check := c.CheckTree\n\treturn check(", 1)
	if got := problemsOf(t, alias, covered); !strings.Contains(got, "CheckTree is used other than as a direct call") {
		t.Errorf("an aliased CheckTree was accepted:\n%s", got)
	}
	pure := callbackModule(`func(path string) int { return len(strings.TrimSpace(path)) }`)
	pure["a/a.go"] = strings.Replace(pure["a/a.go"], "import \"example.test/m/c\"", "import (\n\t\"strings\"\n\n\t\"example.test/m/c\"\n)", 1)
	if got := problemsOf(t, pure, covered); got != "" {
		t.Errorf("a callback that calls only the standard library was refused:\n%s", got)
	}
}

// Threat-model ruling (acp, r2b): a package is excludable only if the scan can model all of it. Each case below is an ordinary-code
// escape the reviewer reproduced with a real data race.
func TestWhatTheScanCannotModelIsNotExcludable(t *testing.T) {
	cgo := "package a\n\n/*\nvoid f(void) {}\n*/\nimport \"C\"\n\nfunc F() int { go func() { C.f() }(); return 1 }\n"
	for name, tc := range map[string]struct {
		files map[string]string
		want  string
	}{
		"cgo file in the package":                            {map[string]string{"a/a.go": cgo, "a/a_test.go": cleanTest}, "cgo"},
		"cgo file in a dependency":                           {map[string]string{"a/a.go": "package a\n\nimport \"example.test/m/b\"\n\nfunc F() int { return b.G() }\n", "a/a_test.go": cleanTest, "b/b.go": strings.Replace(cgo, "package a", "package b", 1)}, "cgo"},
		"export directive":                                   {map[string]string{"a/a.go": "package a\n\n//export F\nfunc F() int { return 1 }\n", "a/a_test.go": cleanTest}, "//export"},
		"reflect in a test file":                             {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": "package a\n\nimport (\n\t\"reflect\"\n\t\"testing\"\n)\n\nfunc TestF(t *testing.T) { reflect.ValueOf(t).MethodByName(\"Parallel\").Call(nil) }\n"}, "imports reflect"},
		"reflect in an external test":                        {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "a/x_test.go": "package a_test\n\nimport (\n\t\"reflect\"\n\t\"testing\"\n)\n\nfunc TestX(t *testing.T) { _ = reflect.TypeOf(t) }\n"}, "imports reflect"},
		"closure passed to strings.Map":                      {map[string]string{"a/a.go": "package a\n\nimport \"strings\"\n\nvar shared int\n\nfunc F() int { return len(strings.Map(func(r rune) rune { shared++; return r }, \"x\")) }\n", "a/a_test.go": cleanTest}, "function value"},
		"func name passed to a call":                         {map[string]string{"a/a.go": "package a\n\nimport \"strings\"\n\nfunc less(r rune) bool { return r < 0 }\n\nfunc F() int { _ = strings.FieldsFunc(\"x\", less); return 1 }\n", "a/a_test.go": cleanTest}, "function value"},
		"method value passed to a call":                      {map[string]string{"a/a.go": "package a\n\nimport \"strings\"\n\ntype T struct{}\n\nfunc (T) less(r rune) bool { return r < 0 }\n\nfunc F() int { var x T; _ = strings.FieldsFunc(\"x\", x.less); return 1 }\n", "a/a_test.go": cleanTest}, "function value"},
		"external test package imports a concurrent package": {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "a/x_test.go": "package a_test\n\nimport (\n\t\"testing\"\n\n\t\"example.test/m/b\"\n)\n\nfunc TestX(t *testing.T) { _ = b.G() }\n", "b/b.go": "package b\n\nfunc G() int { go func() {}(); return 1 }\n"}, "a: imports b"},
	} {
		if got := problemsOf(t, tc.files, rowA); !strings.Contains(got, tc.want) {
			t.Errorf("%s: want a problem naming %q, got:\n%s", name, tc.want, got)
		}
	}
}

// A callback may not pass a function value to a standard-library call either (strings.Map with a closure wrote shared state).
func TestACoveredByCallbackThatPassesAFunctionValueIsRefused(t *testing.T) {
	covered := Row{"a", "covered-by:c", "TestH", "pool in c"}
	files := callbackModule(`func(path string) int { return len(strings.Map(transform, path)) }`)
	files["a/a.go"] = strings.Replace(files["a/a.go"], "import \"example.test/m/c\"", "import (\n\t\"strings\"\n\n\t\"example.test/m/c\"\n)", 1) + "\nfunc transform(r rune) rune { return r }\n"
	if got := problemsOf(t, files, covered); !strings.Contains(got, "function value") {
		t.Errorf("a function value passed to strings.Map was accepted:\n%s", got)
	}
}

// The witness must still call CheckTree: a no-op body keeps its name and proves nothing.
func TestAWitnessThatNoLongerDrivesCheckTreeIsRefused(t *testing.T) {
	covered := Row{"a", "covered-by:c", "TestH", "pool in c"}
	files := coveredModule("c_test.go")
	files["c/c_test.go"] = "package c\n\nimport \"testing\"\n\nfunc TestH(t *testing.T) {}\n"
	if got := problemsOf(t, files, covered); !strings.Contains(got, "does not call CheckTree") {
		t.Errorf("a no-op witness was accepted:\n%s", got)
	}
}

// The same, when the registration sits in the covered-by package itself (as goldenscan/tree.go does): that package is exempt from the
// package-wide scan, so only the callback check can see the function value.
func TestAFunctionValueInACallbackInsideTheCoveredPackageIsRefused(t *testing.T) {
	covered := Row{"a", "covered-by:c", "TestH", "pool in c"}
	files := coveredModule("c_test.go")
	files["c/use.go"] = "package c\n\nimport \"strings\"\n\nfunc transform(r rune) rune { return r }\n\nfunc Use(paths []string) int {\n\treturn CheckTree(paths, func(path string) int { return len(strings.Map(transform, path)) })\n}\n"
	if got := problemsOf(t, files, covered); !strings.Contains(got, "callback passes a function value") {
		t.Errorf("a function value passed inside the callback was accepted:\n%s", got)
	}
	files["c/use.go"] = "package c\n\nimport \"strings\"\n\nfunc Use(paths []string) int {\n\treturn CheckTree(paths, func(path string) int { return len(strings.Map(func(r rune) rune { return r }, path)) })\n}\n"
	if got := problemsOf(t, files, covered); !strings.Contains(got, "callback passes a function value") {
		t.Errorf("a function literal passed inside the callback was accepted:\n%s", got)
	}
}

// acp ruling (CHAOS-11413): the exact set of callees that may take a function value, pinned. A new entry stays red until this list
// names it, and until its line in scan.go carries a comment naming the stdlib contract that makes it synchronous.
func TestThePureCalleeAllowlistIsPinnedAndEachEntryHasItsReason(t *testing.T) {
	want := []string{"(*testing.T).Run", "path/filepath.Walk", "path/filepath.WalkDir", "slices.SortFunc", "slices.SortStableFunc", "sort.Slice", "sort.SliceStable"}
	var got []string
	for k := range pureCallees {
		got = append(got, k)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("pureCallees is %v, the pinned set is %v: pin the new entry here and give its line a comment naming the contract", got, want)
	}
	src, err := os.ReadFile("scan.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range want {
		found := false
		for _, line := range strings.Split(string(src), "\n") {
			if strings.Contains(line, `"`+k+`":`) {
				found = true
				if i := strings.Index(line, "//"); i < 0 || len(strings.TrimSpace(line[i+2:])) < 20 {
					t.Errorf("pureCallees entry %s has no reason comment: %q", k, line)
				}
			}
		}
		if !found {
			t.Errorf("pureCallees entry %s is not on a line of its own in scan.go", k)
		}
	}
}

// What a pure callee runs is scanned as package source: a goroutine inside a WalkDir callback, or t.Parallel inside a t.Run body.
func TestPlantedConcurrencyInsideAPureCalleesFunctionIsRefused(t *testing.T) {
	for name, tc := range map[string]struct{ src, want string }{
		"goroutine inside a WalkDir callback": {"package a\n\nimport \"path/filepath\"\n\nfunc F() int {\n\t_ = filepath.WalkDir(\".\", func(p string, d int, err error) error { go func() {}(); return nil })\n\treturn 1\n}\n", "go statement"},
		"t.Parallel inside a t.Run body":      {"package a\n\nimport \"testing\"\n\nfunc F(t *testing.T) { t.Run(\"x\", func(t *testing.T) { t.Parallel() }) }\n", "use of .Parallel"},
	} {
		if got := problemsOf(t, map[string]string{"a/a.go": tc.src, "a/a_test.go": cleanTest}, rowA); !strings.Contains(got, tc.want) {
			t.Errorf("%s: want %q, got:\n%s", name, tc.want, got)
		}
	}
}

// A pure callee may take only a function literal or a same-package function: its body is scanned. A value from another package, a
// field, a variable or an interface method is a body the scan never sees.
func TestAPureCalleeGivenAFunctionTheScanCannotSeeIsRefused(t *testing.T) {
	head := "package a\n\nimport \"sort\"\n\nfunc local(i, j int) bool { return i < j }\n\ntype S struct{ less func(i, j int) bool }\n\n"
	for name, call := range map[string]string{
		"field of a struct":        `var s S; sort.Slice([]int{1}, s.less)`,
		"local variable":           `f := local; sort.Slice([]int{1}, f)`,
		"value of another package": `sort.Slice([]int{1}, other.Less)`,
		"result of a call":         `sort.Slice([]int{1}, pick())`,
		"interface method":         `var i interface{ Less(a, b int) bool }; sort.Slice([]int{1}, i.Less)`,
	} {
		files := map[string]string{"a/a.go": head + "func pick() func(i, j int) bool { return local }\n\nfunc F() int { " + call + "; return 1 }\n", "a/a_test.go": cleanTest}
		if got := problemsOf(t, files, rowA); !strings.Contains(got, "body the scan cannot see") {
			t.Errorf("%s: want a refusal, got:\n%s", name, got)
		}
	}
	for name, call := range map[string]string{
		"function literal":      `sort.Slice([]int{1}, func(i, j int) bool { return i < j })`,
		"same-package function": `sort.Slice([]int{1}, local)`,
	} {
		files := map[string]string{"a/a.go": head + "func F() int { " + call + "; return 1 }\n", "a/a_test.go": cleanTest}
		if got := problemsOf(t, files, rowA); got != "" {
			t.Errorf("%s was refused:\n%s", name, got)
		}
	}
}

// Fail closed on what the scan cannot read: linkname, non-Go sources, plugins.
func TestLinknameNonGoSourcesAndPluginsAreRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		files map[string]string
		want  string
	}{
		"go:linkname":          {map[string]string{"a/a.go": "package a\n\nimport _ \"unsafe\"\n\n//go:linkname f runtime.f\nfunc f()\n\nfunc F() int { return 1 }\n", "a/a_test.go": cleanTest}, "go:linkname"},
		"assembly file":        {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "a/a_amd64.s": "TEXT ·f(SB),0,$0-0\n\tRET\n", "a/a_arm64.s": "TEXT ·f(SB),0,$0-0\n\tRET\n"}, "non-Go source"},
		"C source file":        {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "a/c.c": "void f(void) {}\n"}, "non-Go source"},
		"header file":          {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "a/c.h": "void f(void);\n"}, "non-Go source"},
		"syso object":          {map[string]string{"a/a.go": cleanPkg, "a/a_test.go": cleanTest, "a/o.syso": "x"}, "non-Go source"},
		"plugin import":        {map[string]string{"a/a.go": "package a\n\nimport \"plugin\"\n\nvar _ = plugin.Open\n\nfunc F() int { return 1 }\n", "a/a_test.go": cleanTest}, "imports plugin"},
		"non-Go in dependency": {map[string]string{"a/a.go": "package a\n\nimport \"example.test/m/b\"\n\nfunc F() int { return b.G() }\n", "a/a_test.go": cleanTest, "b/b.go": "package b\n\nfunc G() int { return 1 }\n", "b/b.s": "// asm\n"}, "non-Go source"},
	} {
		if got := problemsOf(t, tc.files, rowA); !strings.Contains(got, tc.want) {
			t.Errorf("%s: want %q, got:\n%s", name, tc.want, got)
		}
	}
}

// acp final verify (CHAOS-11413): a function value is judged by its TYPE, not by how it is spelled. A package-level func variable, a
// local variable, a struct field or a call result handed to a callee that runs it elsewhere (singleflight.DoChan) raced under -race.
func TestAFunctionValueOfAnySpellingPassedToACalleeIsRefused(t *testing.T) {
	head := "package a\n\ntype G struct{}\n\nfunc (G) DoChan(key string, fn func() (any, error)) {}\n\ntype J struct{ fn func() (any, error) }\n\nvar work = func() (any, error) { return nil, nil }\n\nfunc pick() func() (any, error) { return work }\n\n"
	for name, body := range map[string]string{
		"package-level func variable": `var g G; g.DoChan("k", work)`,
		"struct field":                `var g G; j := J{fn: work}; g.DoChan("k", j.fn)`,
		"local variable":              `var g G; w := work; g.DoChan("k", w)`,
		"call result":                 `var g G; g.DoChan("k", pick())`,
		"conversion":                  `var g G; g.DoChan("k", (func() (any, error))(work))`,
		"parenthesised":               `var g G; g.DoChan("k", (work))`,
	} {
		files := map[string]string{"a/a.go": head + "func F() int { " + body + "; return 1 }\n", "a/a_test.go": cleanTest}
		if got := problemsOf(t, files, rowA); !strings.Contains(got, "function value") {
			t.Errorf("%s: want a function value problem, got:\n%s", name, got)
		}
	}
	if got := problemsOf(t, map[string]string{"a/a.go": head + "func F() int { var g G; g.DoChan(\"k\", nil); return len(\"k\") }\n", "a/a_test.go": cleanTest}, rowA); got != "" {
		t.Errorf("a call that passes no function value was refused:\n%s", got)
	}
}

// t.Run is pinned by the type of its receiver (*testing.T), not by the receiver's name; a method named Run on another type is not pure.
func TestThePinnedTRunIsResolvedByType(t *testing.T) {
	okTest := "package a\n\nimport \"testing\"\n\nfunc TestF(tt *testing.T) { tt.Run(\"x\", func(u *testing.T) {}) }\n"
	if got := problemsOf(t, map[string]string{"a/a.go": cleanPkg, "a/a_test.go": okTest}, rowA); got != "" {
		t.Errorf("tt.Run on a *testing.T was refused:\n%s", got)
	}
	fake := "package a\n\nimport \"testing\"\n\ntype runner struct{}\n\nfunc (runner) Run(name string, fn func()) {}\n\nfunc TestF(t *testing.T) { var r runner; t.Run(\"x\", func(u *testing.T) {}); r.Run(\"x\", func() {}) }\n"
	if got := problemsOf(t, map[string]string{"a/a.go": cleanPkg, "a/a_test.go": fake}, rowA); !strings.Contains(got, "function value") {
		t.Errorf("a method named Run on another type was treated as the pinned t.Run:\n%s", got)
	}
}

// Code outside the standard library and this module (singleflight, otel, grpc) runs user code on goroutines of its own.
func TestAnOutOfModuleImportInTheClosureIsRefused(t *testing.T) {
	own := map[string]string{"a/a.go": "package a\n\nimport \"golang.org/x/sync/singleflight\"\n\nvar g singleflight.Group\n\nfunc F() int { return 1 }\n", "a/a_test.go": cleanTest}
	if got := problemsOf(t, own, rowA); !strings.Contains(got, "out-of-module") {
		t.Errorf("an out-of-module import in the package was accepted:\n%s", got)
	}
	dep := map[string]string{"a/a.go": "package a\n\nimport \"example.test/m/b\"\n\nfunc F() int { return b.G() }\n", "a/a_test.go": cleanTest, "b/b.go": "package b\n\nimport _ \"golang.org/x/sync/singleflight\"\n\nfunc G() int { return 1 }\n"}
	if got := problemsOf(t, dep, rowA); !strings.Contains(got, "out-of-module") {
		t.Errorf("an out-of-module import in a dependency was accepted:\n%s", got)
	}
	testImport := map[string]string{"a/a.go": cleanPkg, "a/a_test.go": "package a\n\nimport (\n\t\"testing\"\n\n\t_ \"golang.org/x/sync/singleflight\"\n)\n\nfunc TestF(t *testing.T) {}\n"}
	if got := problemsOf(t, testImport, rowA); !strings.Contains(got, "out-of-module") {
		t.Errorf("an out-of-module import in a test file was accepted:\n%s", got)
	}
}

// acp delta verify of DELTA5 (CHAOS-11413): the stdlib is a deny-by-default ALLOWlist. Each plant below passed the r2b scanner with no
// problem while go test -race reported a DATA RACE (the verifier's reproductions, same sources).
func TestStdlibPackagesOffTheAllowlistAreRefused(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"context: a custom parent makes propagateCancel call the user's Err on a goroutine": {
			"a/a.go":      "package a\n\nimport \"context\"\n\ntype Ctx struct {\n\tcontext.Context\n\tN *int\n}\n\nfunc (c Ctx) Value(any) any { return nil }\n\nfunc (c Ctx) Err() error { *c.N++; return c.Context.Err() }\n\nfunc Child(parent context.Context, n *int) context.Context {\n\tc, _ := context.WithCancel(Ctx{parent, n})\n\treturn c\n}\n",
			"a/a_test.go": "package a\n\nimport (\n\t\"context\"\n\t\"testing\"\n\t\"time\"\n)\n\nfunc TestRace(t *testing.T) {\n\tn := 0\n\tparent, cancel := context.WithCancel(context.Background())\n\t_ = Child(parent, &n)\n\tcancel()\n\tfor i := 0; i < 1000; i++ {\n\t\tn++\n\t\ttime.Sleep(time.Microsecond)\n\t}\n}\n",
		},
		"net/rpc: ServeCodec starts a goroutine per request that calls the user's method": {
			"a/a.go":      "package a\n\nimport (\n\t\"bytes\"\n\t\"net/rpc\"\n\t\"net/rpc/jsonrpc\"\n\t\"time\"\n)\n\ntype Svc struct{ N int }\n\nfunc (s *Svc) Inc(arg int, reply *int) error { s.N += arg; time.Sleep(time.Millisecond); *reply = s.N; return nil }\n\ntype conn struct {\n\tr *bytes.Reader\n\tw bytes.Buffer\n}\n\nfunc (c *conn) Read(p []byte) (int, error)  { return c.r.Read(p) }\nfunc (c *conn) Write(p []byte) (int, error) { return len(p), nil }\nfunc (c *conn) Close() error                { return nil }\n\nfunc Serve(s *Svc) {\n\tsrv := rpc.NewServer()\n\t_ = srv.Register(s)\n\treq := `{\"method\":\"Svc.Inc\",\"params\":[1],\"id\":1}{\"method\":\"Svc.Inc\",\"params\":[1],\"id\":2}{\"method\":\"Svc.Inc\",\"params\":[1],\"id\":3}`\n\tsrv.ServeCodec(jsonrpc.NewServerCodec(&conn{r: bytes.NewReader([]byte(req))}))\n}\n",
			"a/a_test.go": "package a\n\nimport \"testing\"\n\nfunc TestRace(t *testing.T) { Serve(&Svc{}) }\n",
		},
		"reflect in a non-test helper calls t.Parallel by name": {
			"a/a.go":      "package a\n\nimport (\n\t\"reflect\"\n\t\"testing\"\n)\n\nfunc Mark(t *testing.T) { reflect.ValueOf(t).MethodByName(\"Para\" + \"llel\").Call(nil) }\n",
			"a/a_test.go": "package a\n\nimport (\n\t\"testing\"\n\t\"time\"\n)\n\nvar shared int\n\nfunc TestA(t *testing.T) { Mark(t); for i := 0; i < 1000; i++ { shared++; time.Sleep(time.Microsecond) } }\nfunc TestB(t *testing.T) { Mark(t); for i := 0; i < 1000; i++ { shared++; time.Sleep(time.Microsecond) } }\n",
		},
		"runtime.SetFinalizer runs a function held in an interface on the finalizer goroutine": {
			"a/a.go":      "package a\n\nimport \"runtime\"\n\nvar Hits int\n\ntype T struct{ b [64]byte }\n\nvar fin any = func(*T) { Hits++ }\n\nfunc Make() {\n\tx := &T{}\n\truntime.SetFinalizer(x, fin)\n}\n",
			"a/a_test.go": "package a\n\nimport (\n\t\"runtime\"\n\t\"testing\"\n\t\"time\"\n)\n\nfunc TestRace(t *testing.T) {\n\tMake()\n\truntime.GC()\n\tfor i := 0; i < 2000; i++ {\n\t\tHits++\n\t\ttime.Sleep(time.Microsecond)\n\t}\n}\n",
		},
	} {
		if got := problemsOf(t, files, rowA); !strings.Contains(got, "not on the stdlib allowlist") {
			t.Errorf("%s: want a stdlib allowlist problem, got:\n%s", name, got)
		}
	}
}

// The allowlist is a table the test reads: the exact set is pinned, and each entry says why it starts no goroutine and runs no user
// code on another one.
func TestTheStdlibAllowlistIsPinnedAndEachEntryHasItsReason(t *testing.T) {
	want := []string{"bytes", "crypto/sha256", "encoding/hex", "encoding/json", "errors", "fmt", "io", "io/fs", "log/slog", "math", "net/netip", "os",
		"path", "path/filepath", "regexp", "slices", "sort", "strconv", "strings", "testing", "time", "unicode", "unicode/utf8"}
	var got []string
	for k, reason := range stdlibAllowed {
		got = append(got, k)
		if len(strings.TrimSpace(reason)) < 30 {
			t.Errorf("stdlibAllowed[%q] has no reason: %q", k, reason)
		}
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("stdlibAllowed is %v, the pinned set is %v: pin the new entry here and give it a reason naming why it starts no goroutine", got, want)
	}
	for _, banned := range []string{"context", "runtime", "reflect", "sync", "sync/atomic", "net", "net/http", "net/rpc", "os/exec", "os/signal", "unsafe", "plugin"} {
		if _, ok := stdlibAllowed[banned]; ok {
			t.Errorf("%s must not be on the allowlist", banned)
		}
	}
}

// A function converted to an interface type, however the conversion is spelled, runs where the scan cannot follow it.
func TestAFuncConvertedToAnInterfaceIsRefused(t *testing.T) {
	head := "package a\n\ntype S struct{ v any }\n\nfunc fn() {}\n\nfunc take(v any) {}\n\n"
	for name, body := range map[string]string{
		"var with an interface type":   "var fin any = func() {}\n\t_ = fin",
		"var of a named func":          "var fin any = fn\n\t_ = fin",
		"argument to an interface":     "take(func() {})",
		"explicit conversion":          "_ = any(fn)",
		"assignment":                   "var v any\n\tv = fn\n\t_ = v",
		"composite literal field":      "_ = S{v: fn}",
		"composite literal element":    "_ = []any{fn}",
		"map literal element":          "_ = map[string]any{\"k\": fn}",
		"append to an interface slice": "var s []any\n\ts = append(s, fn)\n\t_ = s",
	} {
		files := map[string]string{"a/a.go": head + "func F() int {\n\t" + body + "\n\treturn 1\n}\n", "a/a_test.go": cleanTest}
		if got := problemsOf(t, files, rowA); !strings.Contains(got, "converted to an interface") {
			t.Errorf("%s: want a conversion problem, got:\n%s", name, got)
		}
	}
	ret := map[string]string{"a/a.go": "package a\n\nfunc fn() {}\n\nfunc G() any { return fn }\n\nfunc F() int { _ = G(); return 1 }\n", "a/a_test.go": cleanTest}
	if got := problemsOf(t, ret, rowA); !strings.Contains(got, "converted to an interface") {
		t.Errorf("a function returned as an interface was accepted:\n%s", got)
	}
	ok := map[string]string{"a/a.go": "package a\n\nfunc F() int { var x any = 1; _ = x; return 1 }\n", "a/a_test.go": cleanTest}
	if got := problemsOf(t, ok, rowA); got != "" {
		t.Errorf("a non-function converted to an interface was refused:\n%s", got)
	}
}
