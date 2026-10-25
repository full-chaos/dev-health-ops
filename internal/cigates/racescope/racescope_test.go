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
		"c/" + witnessFile: "package c\n\nimport \"testing\"\n\nfunc TestH(t *testing.T) { _ = H() }\n",
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
