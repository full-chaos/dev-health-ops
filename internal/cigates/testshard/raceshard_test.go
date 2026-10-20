package testshard_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Go port of sections 2 (race shards) and 3 (leg logs) of tests/tooling/test_go_quality_legs.py. They drive the real
// ci/check_go.sh, ci/go_race_shard.awk and ci/go_race_weights.tsv. Every guard has an analysis function that takes plain
// data, so a ...Planted... companion can feed the same function a defect and show it fails.

const raceshardDefaultWeight = 2

func raceshardPath(t *testing.T, rel string) string {
	t.Helper()
	return filepath.Join(repoRoot(t), rel)
}

// raceshardParseWeights parses a weights table text (package<TAB>seconds rows) into rows in file order.
func raceshardParseWeights(text string) (order []string, weights map[string]int) {
	weights = map[string]int{}
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		n := 0
		if len(f) > 1 {
			n, _ = strconv.Atoi(f[1])
		}
		order = append(order, f[0])
		weights[f[0]] = n
	}
	return order, weights
}

func raceshardRealWeights(t *testing.T) map[string]int {
	t.Helper()
	b, err := os.ReadFile(raceshardPath(t, "ci/go_race_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	_, w := raceshardParseWeights(string(b))
	return w
}

// raceshardAwk runs the shard awk (awkFile) over pkgs for shard k of count.
func raceshardAwk(t *testing.T, awkFile, mod string, pkgs []string, k, count int, weightsFile string) []string {
	t.Helper()
	cmd := exec.Command("awk", "-v", "mod="+mod, "-v", fmt.Sprintf("shard=%d", k), "-v", fmt.Sprintf("count=%d", count),
		"-v", "weights="+weightsFile, "-f", awkFile)
	cmd.Stdin = strings.NewReader(strings.Join(pkgs, "\n") + "\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("awk shard %d/%d: %v", k, count, err)
	}
	return strings.Fields(string(out))
}

func raceshardSlices(t *testing.T, mod string, pkgs []string, count int) [][]string {
	t.Helper()
	var s [][]string
	for k := 1; k <= count; k++ {
		s = append(s, raceshardAwk(t, raceshardPath(t, "ci/go_race_shard.awk"), mod, pkgs, k, count, raceshardPath(t, "ci/go_race_weights.tsv")))
	}
	return s
}

func raceshardLoads(mod string, weights map[string]int, slices [][]string) (loads []int, heaviest int) {
	w := func(p string) int {
		if v, ok := weights[keyOf(mod, p)]; ok {
			return v
		}
		return raceshardDefaultWeight
	}
	for _, s := range slices {
		l := 0
		for _, p := range s {
			l += w(p)
			if w(p) > heaviest {
				heaviest = w(p)
			}
		}
		loads = append(loads, l)
	}
	return loads, heaviest
}

// raceshardPartitionDefects is the check of the partition-and-balance guard: disjoint, covering, within the LPT bound and,
// for 2 and 3 ways, within 10% of the mean. allPkgs gives the heaviest package for the bound.
func raceshardPartitionDefects(mod string, allPkgs []string, weights map[string]int, slices [][]string) []string {
	var defects []string
	count := len(slices)
	var flat []string
	for _, s := range slices {
		flat = append(flat, s...)
	}
	seen := map[string]bool{}
	for _, p := range flat {
		if seen[p] {
			defects = append(defects, fmt.Sprintf("package %s is in two slices (count=%d)", p, count))
			break
		}
		seen[p] = true
	}
	a, b := append([]string{}, flat...), append([]string{}, allPkgs...)
	sort.Strings(a)
	sort.Strings(b)
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		defects = append(defects, fmt.Sprintf("the slices do not cover the list (count=%d)", count))
	}
	loads, _ := raceshardLoads(mod, weights, slices)
	_, heaviest := raceshardLoads(mod, weights, [][]string{allPkgs})
	total, maxLoad := 0, 0
	for _, l := range loads {
		total += l
		if l > maxLoad {
			maxLoad = l
		}
	}
	if float64(maxLoad) > float64(total)/float64(count)+float64(heaviest) {
		defects = append(defects, fmt.Sprintf("a slice exceeds the LPT bound (loads %v)", loads))
	}
	if (count == 2 || count == 3) && float64(maxLoad) > 1.1*float64(total)/float64(count) {
		defects = append(defects, fmt.Sprintf("%d-way race slices unbalanced: %v", count, loads))
	}
	return defects
}

var (
	raceshardListOnce sync.Once
	raceshardMod      string
	raceshardPkgs     []string
)

func raceshardGoList(t *testing.T) (string, []string) {
	t.Helper()
	raceshardListOnce.Do(func() { raceshardMod, raceshardPkgs = goList(t) })
	if len(raceshardPkgs) == 0 {
		t.Fatal("go list produced no packages")
	}
	return raceshardMod, raceshardPkgs
}

func TestRaceShardsPartitionThePackageListAndBalance(t *testing.T) {
	mod, pkgs := raceshardGoList(t)
	if len(pkgs) < 100 {
		t.Fatalf("only %d packages", len(pkgs))
	}
	weights := raceshardRealWeights(t)
	for _, count := range []int{2, 3, 5} {
		slices := raceshardSlices(t, mod, pkgs, count)
		for _, d := range raceshardPartitionDefects(mod, pkgs, weights, slices) {
			t.Error(d)
		}
	}
}

func TestRaceShardsPartitionPlantedDefectsAreCaught(t *testing.T) {
	mod, pkgs := raceshardGoList(t)
	weights := raceshardRealWeights(t)
	good := raceshardSlices(t, mod, pkgs, 3)
	if d := raceshardPartitionDefects(mod, pkgs, weights, good); len(d) != 0 {
		t.Fatalf("the unmutated slices must be clean, got %v", d)
	}
	clone := func() [][]string {
		var c [][]string
		for _, s := range good {
			c = append(c, append([]string{}, s...))
		}
		return c
	}
	dup := clone()
	dup[1] = append(dup[1], dup[0][0])
	drop := clone()
	drop[2] = drop[2][1:]
	skew := clone()
	skew[0] = append(skew[0], skew[1]...)
	skew[1] = nil
	for name, tc := range map[string]struct {
		slices [][]string
		needle string
	}{
		"duplicate": {dup, "in two slices"},
		"dropped":   {drop, "do not cover"},
		"skewed":    {skew, "unbalanced"},
	} {
		got := strings.Join(raceshardPartitionDefects(mod, pkgs, weights, tc.slices), "; ")
		if !strings.Contains(got, tc.needle) {
			t.Errorf("%s: planted defect not caught (needle %q, got %q)", name, tc.needle, got)
		}
	}
}

// raceshardTableDefects is the check of the weights-table guard: sorted rows, no duplicates, no stale package.
func raceshardTableDefects(text string, known map[string]bool) []string {
	rows, _ := raceshardParseWeights(text)
	var defects []string
	if !sort.StringsAreSorted(rows) {
		defects = append(defects, "ci/go_race_weights.tsv is not sorted (LC_ALL=C)")
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r] {
			defects = append(defects, "a package is weighed twice: "+r)
		}
		seen[r] = true
		if !known[r] {
			defects = append(defects, "weights name a package that no longer exists: "+r)
		}
	}
	return defects
}

func raceshardKnown(t *testing.T) map[string]bool {
	t.Helper()
	mod, pkgs := raceshardGoList(t)
	known := map[string]bool{}
	for _, p := range pkgs {
		known[keyOf(mod, p)] = true
	}
	return known
}

func TestTheRaceWeightsTableIsClean(t *testing.T) {
	b, err := os.ReadFile(raceshardPath(t, "ci/go_race_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range raceshardTableDefects(string(b), raceshardKnown(t)) {
		t.Error(d)
	}
}

func TestTheRaceWeightsTablePlantedDefectsAreCaught(t *testing.T) {
	b, err := os.ReadFile(raceshardPath(t, "ci/go_race_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	known := raceshardKnown(t)
	text := string(b)
	if d := raceshardTableDefects(text, known); len(d) != 0 {
		t.Fatalf("the real table must be clean, got %v", d)
	}
	var data []string
	for _, l := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			data = append(data, l)
		}
	}
	swapped := append([]string{}, data...)
	swapped[0], swapped[1] = swapped[1], swapped[0]
	for name, tc := range map[string]struct {
		text   string
		needle string
	}{
		"unsorted":  {strings.Join(swapped, "\n") + "\n", "not sorted"},
		"duplicate": {text + data[0] + "\n", "weighed twice"},
		"stale":     {text + "zz/no/such/package\t3\n", "no longer exists"},
	} {
		got := strings.Join(raceshardTableDefects(tc.text, known), "; ")
		if !strings.Contains(got, tc.needle) {
			t.Errorf("%s: planted defect not caught (needle %q, got %q)", name, tc.needle, got)
		}
	}
}

// raceshardHoldsDefect runs the awk named by awkFile over 23 synthetic packages under hostile weights and returns a defect
// text when the slices of any count are not an exact partition.
func raceshardHoldsDefect(t *testing.T, awkFile string) string {
	t.Helper()
	var pkgs []string
	for i := 0; i < 23; i++ {
		pkgs = append(pkgs, fmt.Sprintf("m/p%d", i))
	}
	weird := filepath.Join(t.TempDir(), "w.tsv")
	if err := os.WriteFile(weird, []byte("p3\t100000\np4\t0\np5\t-7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 2, 4, 7} {
		var got []string
		for k := 1; k <= count; k++ {
			got = append(got, raceshardAwk(t, awkFile, "m", pkgs, k, count, weird)...)
		}
		sort.Strings(got)
		want := append([]string{}, pkgs...)
		sort.Strings(want)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			return fmt.Sprintf("count=%d: slices are not an exact partition (%d of %d)", count, len(got), len(want))
		}
	}
	return ""
}

func TestTheShardPartitionHoldsWhateverTheWeightsSay(t *testing.T) {
	if d := raceshardHoldsDefect(t, raceshardPath(t, "ci/go_race_shard.awk")); d != "" {
		t.Error(d)
	}
}

func TestTheShardPartitionPlantedAwkMutantsAreCaught(t *testing.T) {
	b, err := os.ReadFile(raceshardPath(t, "ci/go_race_shard.awk"))
	if err != nil {
		t.Fatal(err)
	}
	const keep = "if (best == shard) keep[k] = 1"
	if !strings.Contains(string(b), keep) {
		t.Fatalf("awk no longer contains %q: update the planted mutants", keep)
	}
	for name, repl := range map[string]string{
		"drops a package":      "if (best == shard && k % 5 != 0) keep[k] = 1",
		"duplicates a package": "if (best == shard || k == 1) keep[k] = 1",
	} {
		mutant := filepath.Join(t.TempDir(), "mutant.awk")
		if err := os.WriteFile(mutant, []byte(strings.Replace(string(b), keep, repl, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
		if raceshardHoldsDefect(t, mutant) == "" {
			t.Errorf("mutant that %s was not caught", name)
		}
	}
}

var (
	raceshardLegMu    sync.Mutex
	raceshardLegCache = map[int]*legRun{}
)

// raceshardLeg runs `ci-leg race K 2` once per process (the stand-in go records the calls).
func raceshardLeg(t *testing.T, k int) legRun {
	t.Helper()
	raceshardLegMu.Lock()
	defer raceshardLegMu.Unlock()
	if r, ok := raceshardLegCache[k]; ok {
		return *r
	}
	r := runCheckGo(t, nil, false, "ci-leg", "race", fmt.Sprint(k), "2")
	raceshardLegCache[k] = &r
	return r
}

// raceshardVerbDefects is the check of the race-verb guard: every call is a -race call, each of the legs ran one
// providersync test shard, and the root module's packages seen across the legs are exactly the go list minus providersync.
func raceshardVerbDefects(mod string, pkgs []string, legs [][]string) []string {
	var defects []string
	var seen []string
	providerCalls := 0
	inRoot := map[string]bool{}
	for _, p := range pkgs {
		inRoot[p] = true
	}
	for i, calls := range legs {
		if len(calls) == 0 {
			defects = append(defects, fmt.Sprintf("shard %d/%d ran no go test", i+1, len(legs)))
		}
		for _, c := range calls {
			if !strings.Contains(" "+c+" ", " -race ") {
				defects = append(defects, "a go test call without -race: "+c)
			}
			for _, w := range strings.Fields(c) {
				if strings.HasPrefix(w, mod) {
					seen = append(seen, w)
				}
			}
			if strings.Contains(c, "-run") && strings.HasSuffix(c, "./internal/providersync") {
				providerCalls++
			}
		}
	}
	if providerCalls != len(legs) {
		defects = append(defects, "each race leg runs one providersync test shard")
	}
	var root []string
	for _, p := range seen {
		if inRoot[p] {
			root = append(root, p)
		}
	}
	var expected []string
	for _, p := range pkgs {
		if p != mod+"/internal/providersync" {
			expected = append(expected, p)
		}
	}
	sort.Strings(root)
	sort.Strings(expected)
	if strings.Join(root, "\n") != strings.Join(expected, "\n") {
		defects = append(defects, fmt.Sprintf("the legs' packages are not exactly the list minus providersync (%d seen, %d expected)", len(root), len(expected)))
	}
	return defects
}

func TestRaceVerbSlicesPartitionTheRealPackages(t *testing.T) {
	mod, pkgs := raceshardGoList(t)
	var legs [][]string
	for k := 1; k <= 2; k++ {
		r := raceshardLeg(t, k)
		if r.Err != nil {
			t.Fatalf("race %d/2: %v\n%s%s", k, r.Err, tail(r.Stderr), tail(r.Stdout))
		}
		legs = append(legs, r.Calls)
	}
	for _, d := range raceshardVerbDefects(mod, pkgs, legs) {
		t.Error(d)
	}
}

func tail(s string) string {
	if len(s) > 1500 {
		return s[len(s)-1500:]
	}
	return s
}

func TestRaceVerbSlicesPlantedDefectsAreCaught(t *testing.T) {
	mod, pkgs := raceshardGoList(t)
	var legs [][]string
	for k := 1; k <= 2; k++ {
		r := raceshardLeg(t, k)
		if r.Err != nil {
			t.Fatalf("race %d/2: %v", k, r.Err)
		}
		legs = append(legs, r.Calls)
	}
	if d := raceshardVerbDefects(mod, pkgs, legs); len(d) != 0 {
		t.Fatalf("the real legs must be clean, got %v", d)
	}
	// Planted: an empty leg, a call without -race, and a package listed nowhere.
	if d := raceshardVerbDefects(mod, pkgs, [][]string{legs[0], nil}); len(d) == 0 {
		t.Error("an empty leg was not caught")
	}
	noRace := [][]string{{}, {}}
	for i, l := range legs {
		for _, c := range l {
			noRace[i] = append(noRace[i], strings.Replace(c, " -race ", " ", 1))
		}
	}
	if d := raceshardVerbDefects(mod, pkgs, noRace); len(d) == 0 {
		t.Error("calls without -race were not caught")
	}
	dropped := [][]string{{}, {}}
	for i, l := range legs {
		for _, c := range l {
			dropped[i] = append(dropped[i], strings.ReplaceAll(c, " "+pkgs[0], ""))
		}
	}
	if d := raceshardVerbDefects(mod, pkgs, dropped); len(d) == 0 {
		t.Error("a dropped package was not caught")
	}
	// Planted in the real script: a weights file that lacks a row fails the leg before any go test runs.
	b, err := os.ReadFile(raceshardPath(t, "ci/go_race_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(l, "internal/httpguard\t") {
			kept = append(kept, l)
		}
	}
	if len(kept) == len(strings.Split(string(b), "\n")) {
		t.Skip("internal/httpguard has no weights row to remove")
	}
	planted := filepath.Join(t.TempDir(), "w.tsv")
	if err := os.WriteFile(planted, []byte(strings.Join(kept, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	r := runCheckGo(t, []string{"GO_RACE_WEIGHTS=" + planted}, false, "ci-leg", "race", "1", "2")
	if r.Err == nil || !strings.Contains(r.Stderr, "NO row") {
		t.Errorf("a missing weights row must fail the leg (err=%v): %s", r.Err, tail(r.Stderr))
	}
	if len(r.Calls) != 0 {
		t.Errorf("the leg ran go test before failing: %v", r.Calls)
	}
}

func TestARaceSliceThatSelectsNothingFailsLoudly(t *testing.T) {
	r := runCheckGo(t, nil, false, "ci-leg", "race", "5000", "5001")
	if r.Err == nil {
		t.Fatal("a race slice that selects nothing must fail")
	}
	if !strings.Contains(r.Stderr, "select zero") && !strings.Contains(r.Stderr, "selected zero") {
		t.Errorf("stderr does not say the slice selects zero: %s", tail(r.Stderr))
	}
	if len(r.Calls) != 0 {
		t.Errorf("a zero-package leg must not run go test: %v", r.Calls)
	}
}

func TestARaceSliceThatSelectsNothingPlantedControlSelectsSomething(t *testing.T) {
	// Planted contrast: the awk gives shard 5000 of 5001 no package, so it is exactly the input the guard exists for;
	// a real shard of the same awk is non-empty, so the guard is not an unconditional failure.
	mod, pkgs := raceshardGoList(t)
	awk, w := raceshardPath(t, "ci/go_race_shard.awk"), raceshardPath(t, "ci/go_race_weights.tsv")
	if got := raceshardAwk(t, awk, mod, pkgs, 5000, 5001, w); len(got) != 0 {
		t.Errorf("shard 5000/5001 should select nothing, got %d", len(got))
	}
	if got := raceshardAwk(t, awk, mod, pkgs, 1, 2, w); len(got) == 0 {
		t.Error("shard 1/2 selected nothing")
	}
}

func TestBadLegArgumentsFail(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		needle string
	}{
		{[]string{"ci-leg"}, "accepts LEG"},
		{[]string{"ci-leg", "nope"}, "must be one of"},
		{[]string{"ci-leg", "race", "0", "2"}, "outside 1..2"},
		{[]string{"ci-leg", "race", "3", "2"}, "outside 1..2"},
		{[]string{"ci-leg", "race", "x", "2"}, "must be a positive integer"},
		{[]string{"ci-leg", "race"}, "must be a positive integer"},
		{[]string{"ci-leg", "test"}, "must be a positive integer"},
		{[]string{"ci-leg", "test", "4", "3"}, "outside 1..3"},
	} {
		r := runCheckGo(t, nil, false, tc.args...)
		if r.Err == nil {
			t.Errorf("%v: must fail", tc.args)
		}
		if !strings.Contains(r.Stderr, tc.needle) {
			t.Errorf("%v: stderr lacks %q: %s", tc.args, tc.needle, tail(r.Stderr))
		}
		if len(r.Calls) != 0 {
			t.Errorf("%v: ran go test: %v", tc.args, r.Calls)
		}
	}
}

func TestBadLegArgumentsPlantedValidArgumentsAreNotRefused(t *testing.T) {
	// Planted contrast: a valid in-range argument pair must not hit any argument message.
	r := runCheckGo(t, nil, false, "ci-leg", "race", "2", "2")
	for _, needle := range []string{"outside 1..", "must be a positive integer", "must be one of", "accepts LEG"} {
		if strings.Contains(r.Stderr, needle) {
			t.Errorf("valid arguments got %q: %s", needle, tail(r.Stderr))
		}
	}
	if len(r.Calls) == 0 {
		t.Error("valid arguments ran no go test")
	}
}

var (
	raceshardStart = regexp.MustCompile(`==> stage race 1/2 start \d\d:\d\d:\d\dZ`)
	raceshardDone  = regexp.MustCompile(`<== stage race 1/2 done in \d+s at \d\d:\d\d:\d\dZ`)
)

// raceshardStampDefects is the check of the leg-log guard on a passing leg's stdout.
func raceshardStampDefects(stdout string) []string {
	var d []string
	if !raceshardStart.MatchString(stdout) {
		d = append(d, "no UTC start stamp")
	}
	if !raceshardDone.MatchString(stdout) {
		d = append(d, "no UTC done stamp")
	}
	if !strings.Contains(stdout, "ci-leg race: OK") {
		d = append(d, "no ci-leg OK line")
	}
	return d
}

func TestEveryStagePrintsStartAndDoneStamps(t *testing.T) {
	r := raceshardLeg(t, 1)
	if r.Err != nil {
		t.Fatalf("race 1/2: %v\n%s", r.Err, tail(r.Stderr))
	}
	for _, d := range raceshardStampDefects(r.Stdout) {
		t.Error(d)
	}
}

func TestEveryStagePlantedUnstampedLogsAreCaught(t *testing.T) {
	r := raceshardLeg(t, 1)
	if r.Err != nil {
		t.Fatalf("race 1/2: %v", r.Err)
	}
	if d := raceshardStampDefects(r.Stdout); len(d) != 0 {
		t.Fatalf("the real log must be clean, got %v", d)
	}
	noStart := strings.ReplaceAll(r.Stdout, "==> stage", "stage")
	noDone := strings.ReplaceAll(r.Stdout, "<== stage", "stage")
	noOK := strings.ReplaceAll(r.Stdout, "ci-leg race: OK", "")
	badStamp := regexp.MustCompile(`start \d\d:\d\d:\d\dZ`).ReplaceAllString(r.Stdout, "start")
	for name, text := range map[string]string{"no start": noStart, "no done": noDone, "no OK": noOK, "no stamp time": badStamp} {
		if len(raceshardStampDefects(text)) == 0 {
			t.Errorf("planted log %q was not caught", name)
		}
	}
}

// raceshardFailedLegDefects is the check of the failing-stage guard.
func raceshardFailedLegDefects(r legRun) []string {
	var d []string
	if len(r.Calls) == 0 {
		d = append(d, "the stand-in go test was never reached")
	}
	if r.Err == nil {
		d = append(d, "the leg passed although its stage failed")
	}
	if !strings.Contains(r.Stdout, "==> stage race 1/2 start") {
		d = append(d, "no start stamp")
	}
	if strings.Contains(r.Stdout, "<== stage race 1/2 done") {
		d = append(d, "a failing stage printed done")
	}
	if strings.Contains(r.Stdout, "ci-leg race: OK") {
		d = append(d, "a failing leg printed OK")
	}
	return d
}

func TestAFailingStagePrintsNoDoneAndFailsTheLeg(t *testing.T) {
	r := runCheckGo(t, nil, true, "ci-leg", "race", "1", "2")
	for _, d := range raceshardFailedLegDefects(r) {
		t.Error(d)
	}
	// One failing race call of the leg (the package run) fails it as well.
	r = runCheckGo(t, []string{"FAKE_GO_FAIL_ONLY=packages"}, false, "ci-leg", "race", "1", "2")
	for _, d := range raceshardFailedLegDefects(r) {
		t.Error("FAKE_GO_FAIL_ONLY=packages: " + d)
	}
}

func TestAFailingStagePlantedPassingLegIsCaught(t *testing.T) {
	// Planted: feed the failing-stage check a passing leg's result; it must report defects.
	r := raceshardLeg(t, 1)
	if r.Err != nil {
		t.Fatalf("race 1/2: %v", r.Err)
	}
	if len(raceshardFailedLegDefects(r)) == 0 {
		t.Error("a passing leg was accepted as a failed one")
	}
	// Planted: a failed leg whose log still carries done and OK lines.
	failed := runCheckGo(t, nil, true, "ci-leg", "race", "1", "2")
	failed.Stdout += "<== stage race 1/2 done in 0s at 00:00:00Z\nci-leg race: OK\n"
	got := strings.Join(raceshardFailedLegDefects(failed), "; ")
	if !strings.Contains(got, "printed done") || !strings.Contains(got, "printed OK") {
		t.Errorf("planted done/OK lines not caught: %q", got)
	}
}
