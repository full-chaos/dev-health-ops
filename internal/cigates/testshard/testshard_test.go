package testshard_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10355: the go-quality test leg runs as three weighted shards (`check_go.sh ci-leg test K 3`). These guards drive the
// real ci/check_go.sh, ci/go_race_shard.awk and ci/go_test_weights.tsv; the leg-count pin is the Go port of the workflow
// matrix check of tests/tooling/test_go_quality_legs.py.

const shardCount = 3

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

func run(t *testing.T, env []string, name string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(), env...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

func checkGo(t *testing.T, env []string, args ...string) (string, string, error) {
	t.Helper()
	return run(t, env, "bash", append([]string{"ci/check_go.sh"}, args...)...)
}

func goList(t *testing.T) (string, []string) {
	t.Helper()
	env := []string{"GOWORK=off", "GOFLAGS=-mod=readonly"}
	mod, errs, err := run(t, env, "go", "list", "-m")
	if err != nil {
		t.Fatalf("go list -m: %v\n%s", err, errs)
	}
	out, errs, err := run(t, env, "go", "list", "./...")
	if err != nil {
		t.Fatalf("go list ./...: %v\n%s", err, errs)
	}
	return strings.TrimSpace(mod), strings.Fields(out)
}

func keyOf(mod, pkg string) string {
	if pkg == mod {
		return "."
	}
	return strings.TrimPrefix(pkg, mod+"/")
}

type weightRow struct {
	pkg     string
	seconds int
}

func weightRows(t *testing.T) []weightRow {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_test_weights.tsv"))
	if err != nil {
		t.Fatalf("ci/go_test_weights.tsv: %v", err)
	}
	var rows []weightRow
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 2 {
			t.Fatalf("weights row is not <package><TAB><seconds>: %q", line)
		}
		n, err := strconv.Atoi(f[1])
		if err != nil || n <= 0 {
			t.Fatalf("weights row %q has no positive integer seconds", f[0])
		}
		rows = append(rows, weightRow{f[0], n})
	}
	return rows
}

// shardPlan is the root-module part of a shard (the plan also holds the packages of nested modules).
func shardPlan(t *testing.T, shard, count int) []string {
	t.Helper()
	out, errs, err := checkGo(t, nil, "test-shard-plan", fmt.Sprint(shard), fmt.Sprint(count))
	if err != nil {
		t.Fatalf("test-shard-plan %d %d: %v\n%s", shard, count, err, errs)
	}
	var pkgs []string
	for _, l := range strings.Split(out, "\n") {
		if f := strings.Split(l, "\t"); len(f) == 3 && f[0] == "." {
			pkgs = append(pkgs, f[1])
		}
	}
	return pkgs
}

func TestTheTestWeightsTableIsClean(t *testing.T) {
	t.Parallel()
	mod, pkgs := goList(t)
	known := map[string]bool{}
	for _, p := range pkgs {
		known[keyOf(mod, p)] = true
	}
	rows := weightRows(t)
	if len(rows) == 0 {
		t.Fatal("ci/go_test_weights.tsv has no row: every shard would be balanced by package count alone")
	}
	seen := map[string]bool{}
	var keys []string
	for _, r := range rows {
		if seen[r.pkg] {
			t.Errorf("package %s is weighed twice", r.pkg)
		}
		seen[r.pkg] = true
		keys = append(keys, r.pkg)
		if !known[r.pkg] {
			t.Errorf("stale row: %s is not a package of the module", r.pkg)
		}
	}
	if !sort.StringsAreSorted(keys) {
		t.Error("ci/go_test_weights.tsv is not sorted (LC_ALL=C)")
	}
}

func TestEveryShardIsNonEmptyAndTheShardsPartitionTheModule(t *testing.T) {
	t.Parallel()
	mod, pkgs := goList(t)
	weights := map[string]int{}
	for _, r := range weightRows(t) {
		weights[r.pkg] = r.seconds
	}
	var all []string
	var loads []int
	heaviest := 1
	for k := 1; k <= shardCount; k++ {
		plan := shardPlan(t, k, shardCount)
		if len(plan) == 0 {
			t.Fatalf("test shard %d/%d is empty", k, shardCount)
		}
		load := 0
		for _, p := range plan {
			w := 1
			if v, ok := weights[keyOf(mod, p)]; ok {
				w = v
			}
			if w > heaviest {
				heaviest = w
			}
			load += w
		}
		loads = append(loads, load)
		all = append(all, plan...)
	}
	sort.Strings(all)
	want := append([]string{}, pkgs...)
	sort.Strings(want)
	if strings.Join(all, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the %d test shards are not a partition of go list ./...: %d planned, %d listed", shardCount, len(all), len(want))
	}
	total := 0
	for _, l := range loads {
		total += l
	}
	for k, l := range loads {
		if l > total/shardCount+heaviest {
			t.Errorf("test shard %d weighs %d, over the longest-processing-time bound %d (loads %v)", k+1, l, total/shardCount+heaviest, loads)
		}
	}
}

func TestAShardThatSelectsNothingFailsLoudly(t *testing.T) {
	t.Parallel()
	_, errs, err := checkGo(t, nil, "test-shard-plan", "5000", "5001")
	if err == nil {
		t.Fatal("a test shard that selects no package must fail")
	}
	if !strings.Contains(errs, "selected zero") {
		t.Fatalf("the failure does not say the shard selected zero packages: %s", errs)
	}
}

func TestBadShardArgumentsFail(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"0", "3"}, "outside 1..3"},
		{[]string{"4", "3"}, "outside 1..3"},
		{[]string{"x", "3"}, "must be a positive integer"},
		{[]string{"1", ""}, "must be a positive integer"},
	} {
		_, errs, err := checkGo(t, nil, append([]string{"test-shard-plan"}, c.args...)...)
		if err == nil || !strings.Contains(errs, c.want) {
			t.Errorf("test-shard-plan %q: want a failure naming %q, got err=%v stderr=%s", c.args, c.want, err, errs)
		}
	}
}

// The leg-count pin: the matrix entries that run the test leg are exactly `test K N` for K = 1..N with N their count,
// and the ci-leg verb takes that shape (and refuses the bare `test` and an out-of-range shard).
func TestWorkflowTestLegsMatchTheCiLegArguments(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "go-quality.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		Jobs map[string]struct {
			Strategy struct {
				Matrix struct {
					Include []map[string]any `yaml:"include"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	var args, names []string
	for _, e := range wf.Jobs["go-quality-leg"].Strategy.Matrix.Include {
		a, _ := e["args"].(string)
		n, _ := e["name"].(string)
		if a == "test" || strings.HasPrefix(a, "test ") {
			args = append(args, a)
			names = append(names, n)
		}
	}
	n := len(args)
	if n < 2 {
		t.Fatalf("the go-quality matrix runs %d test leg(s) (%v): the test leg must be split into shards", n, args)
	}
	sort.Strings(args)
	var want, wantNames []string
	for k := 1; k <= n; k++ {
		want = append(want, fmt.Sprintf("test %d %d", k, n))
		wantNames = append(wantNames, fmt.Sprintf("test-%d-of-%d", k, n))
	}
	sort.Strings(want)
	sort.Strings(wantNames)
	sort.Strings(names)
	if strings.Join(args, "|") != strings.Join(want, "|") {
		t.Errorf("test legs %v are not test K N for K = 1..%d: a gap or a stale COUNT leaves packages unrun or run twice", args, n)
	}
	if strings.Join(names, "|") != strings.Join(wantNames, "|") {
		t.Errorf("test leg names %v, want %v", names, wantNames)
	}
	if n != shardCount {
		t.Errorf("the workflow runs %d test legs, this guard pins %d", n, shardCount)
	}
	for _, c := range []struct{ args, want string }{
		{"test", "must be a positive integer"},
		{fmt.Sprintf("test %d %d", n+1, n), fmt.Sprintf("outside 1..%d", n)},
	} {
		_, errs, err := checkGo(t, nil, append([]string{"ci-leg"}, strings.Fields(c.args)...)...)
		if err == nil || !strings.Contains(errs, c.want) {
			t.Errorf("ci-leg %s: want a failure naming %q, got err=%v stderr=%s", c.args, c.want, err, errs)
		}
	}
}

func writeLog(t *testing.T, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "shard.log")
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// Planted row: a fake shard over 85% of the 30 min cap (1530 s) trips the wall-time guard and names its three heaviest
// packages by measured time.
func TestTheWallTimeGuardTripsOnAFakeShardOverTheBound(t *testing.T) {
	t.Parallel()
	log := writeLog(t, strings.Join([]string{
		"ok  \tgithub.com/x/a\t10.500s",
		"ok  \tgithub.com/x/b\t900.000s",
		"ok  \tgithub.com/x/c\t450.250s",
		"ok  \tgithub.com/x/d\t300s",
		"ok  \tgithub.com/x/e\t(cached)",
		"",
	}, "\n"))
	_, errs, err := checkGo(t, nil, "test-shard-wall-guard", "2", "3", "1531", log)
	if err == nil {
		t.Fatal("a shard at 1531 s of a 1800 s cap must fail")
	}
	for _, want := range []string{"test shard 2/3", "85%", "github.com/x/b", "github.com/x/c", "github.com/x/d", "measured"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the failure lacks %q: %s", want, errs)
		}
	}
	if strings.Contains(errs, "github.com/x/a") {
		t.Errorf("the failure names a package outside the three heaviest: %s", errs)
	}
	out, errs, err := checkGo(t, nil, "test-shard-wall-guard", "2", "3", "1530", log)
	if err != nil {
		t.Fatalf("a shard at exactly the bound must pass: %v\n%s", err, errs)
	}
	if !strings.Contains(out, "test shard 2/3: wall 1530s") {
		t.Errorf("the guard does not log the shard wall time: %s", out)
	}
	_, _, err = checkGo(t, []string{"GO_TEST_SHARD_CAP_SECONDS=100"}, "test-shard-wall-guard", "1", "3", "86", log)
	if err == nil {
		t.Error("the cap is not read from GO_TEST_SHARD_CAP_SECONDS")
	}
}

func TestTheWallTimeGuardNamesTheHeaviestByWeightWhenNothingIsMeasured(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	weights := map[string]int{}
	for _, r := range weightRows(t) {
		weights[r.pkg] = r.seconds
	}
	plan := shardPlan(t, 1, shardCount)
	sort.SliceStable(plan, func(i, j int) bool {
		wi, wj := weights[keyOf(mod, plan[i])], weights[keyOf(mod, plan[j])]
		if wi != wj {
			return wi > wj
		}
		return plan[i] < plan[j]
	})
	_, errs, err := checkGo(t, nil, "test-shard-wall-guard", "1", "3", "1600", writeLog(t, ""))
	if err == nil {
		t.Fatal("a shard over the bound must fail even when no package time was measured")
	}
	if !strings.Contains(errs, "by weight") {
		t.Errorf("the failure does not say the names come from the weights: %s", errs)
	}
	for _, p := range plan[:3] {
		if !strings.Contains(errs, p) {
			t.Errorf("the failure lacks the heavy package %s: %s", p, errs)
		}
	}
}

// The race-excluded census (ci/race_excluded_tests.tsv) is split over the test shards: every row runs in exactly one shard.
func TestTheRaceExcludedCensusIsSplitOverTheShardsExactlyOnce(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "race_excluded_tests.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) != "" && !strings.HasPrefix(l, "#") {
			want = append(want, l)
		}
	}
	var got []string
	for k := 1; k <= shardCount; k++ {
		out, errs, err := checkGo(t, nil, "race-excluded-shard-plan", fmt.Sprint(k), fmt.Sprint(shardCount))
		if err != nil {
			t.Fatalf("race-excluded-shard-plan %d %d: %v\n%s", k, shardCount, err, errs)
		}
		var rows []string
		for _, l := range strings.Split(out, "\n") {
			if strings.Count(l, "\t") == 1 {
				rows = append(rows, l)
			}
		}
		if len(rows) == 0 {
			t.Errorf("test shard %d/%d runs no race-excluded row: the census is not split", k, shardCount)
		}
		got = append(got, rows...)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the shards run %d race-excluded rows, the census lists %d: a row is lost or run twice", len(got), len(want))
	}
}

// A missing or empty race-excluded census fails the test leg: the failure of the row loader must not be swallowed.
func TestAMissingOrEmptyRaceExcludedCensusFailsTheTestLeg(t *testing.T) {
	t.Parallel()
	empty := filepath.Join(t.TempDir(), "empty.tsv")
	if err := os.WriteFile(empty, []byte("# no rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, list := range map[string]string{"empty": empty, "missing": filepath.Join(t.TempDir(), "absent.tsv")} {
		r := runCheckGo(t, []string{"GO_RACE_EXCLUDED_TESTS=" + list}, false, "ci-leg", "test", "350", "350")
		if r.Err == nil {
			t.Errorf("%s census: the test leg passed: %s", name, r.Stdout)
			continue
		}
		if !strings.Contains(r.Stderr, "race_excluded_tests.tsv") {
			t.Errorf("%s census: the failure does not name the census: %s", name, r.Stderr)
		}
	}
	r := runCheckGo(t, nil, false, "ci-leg", "test", "350", "350")
	if r.Err != nil {
		t.Errorf("the real census must pass the same leg: %v\n%s", r.Err, r.Stderr)
	}
}
