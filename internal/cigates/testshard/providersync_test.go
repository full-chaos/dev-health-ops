package testshard_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-8166: internal/providersync runs in every race leg, one weight-balanced test-name shard each. Go port of section 7 of
// tests/tooling/test_go_quality_legs.py. Each guard has a `...Planted...` companion that feeds it a planted defect.

const providersyncAwkName = "go_providersync_race_shard.awk"

func providersyncPaths(t *testing.T) (weights, awk string) {
	t.Helper()
	root := repoRoot(t)
	return filepath.Join(root, "ci", "go_providersync_race_weights.tsv"), filepath.Join(root, "ci", "go_providersync_race_shard.awk")
}

func providersyncWrite(t *testing.T, name, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// providersyncNames: the Test* names of the real providersync race listing (produced once).
func providersyncNames(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(providersyncRaceListing(t))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "Test") {
			names = append(names, l)
		}
	}
	if len(names) == 0 {
		t.Fatal("go test -list reported no providersync test")
	}
	return names
}

func providersyncShard(t *testing.T, awk, weights string, names []string, shard, count int) ([]string, error) {
	t.Helper()
	cmd := exec.Command("awk", "-v", "shard="+strconv.Itoa(shard), "-v", "count="+strconv.Itoa(count), "-v", "weights="+weights, "-f", awk)
	cmd.Stdin = strings.NewReader(strings.Join(names, "\n") + "\n")
	out, err := cmd.Output()
	return strings.Fields(string(out)), err
}

func providersyncSorted(s []string) []string {
	c := append([]string(nil), s...)
	sort.Strings(c)
	return c
}

// providersyncPartitionProblem returns "" when the shards for counts 2..4 are an exact, non-empty partition of names.
func providersyncPartitionProblem(t *testing.T, awk, weights string, names []string) string {
	t.Helper()
	for count := 2; count <= 4; count++ {
		var got []string
		for shard := 1; shard <= count; shard++ {
			part, err := providersyncShard(t, awk, weights, names, shard, count)
			if err != nil {
				return fmt.Sprintf("shard %d/%d: awk failed: %v", shard, count, err)
			}
			if len(part) == 0 {
				return fmt.Sprintf("providersync race shard %d/%d is empty", shard, count)
			}
			got = append(got, part...)
		}
		if strings.Join(providersyncSorted(got), "\n") != strings.Join(providersyncSorted(names), "\n") {
			return fmt.Sprintf("count %d: not an exact partition", count)
		}
	}
	return ""
}

func providersyncParseWeights(t *testing.T, path string) (keys []string, weights map[string]int) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	weights = map[string]int{}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := strings.Split(l, "\t")
		n, err := strconv.Atoi(f[1])
		if err != nil {
			t.Fatalf("bad weight row %q", l)
		}
		keys = append(keys, f[0])
		weights[f[0]] = n
	}
	return keys, weights
}

// providersyncWeightsProblem returns "" when the weights are sorted, unique, name real tests and balance 3 shards.
func providersyncWeightsProblem(t *testing.T, awk, weightsPath string, names []string) string {
	t.Helper()
	keys, weights := providersyncParseWeights(t, weightsPath)
	if !sort.StringsAreSorted(keys) {
		return "weight keys are not sorted"
	}
	seen := map[string]bool{}
	real := map[string]bool{}
	for _, n := range names {
		real[n] = true
	}
	maxW := 0
	for _, k := range keys {
		if seen[k] {
			return "duplicate weight key " + k
		}
		seen[k] = true
		if !real[k] {
			return "a weight names a missing test: " + k
		}
		if weights[k] > maxW {
			maxW = weights[k]
		}
	}
	var totals []int
	sum, max := 0, 0
	for shard := 1; shard <= 3; shard++ {
		part, err := providersyncShard(t, awk, weightsPath, names, shard, 3)
		if err != nil {
			return "awk failed: " + err.Error()
		}
		tot := 0
		for _, n := range part {
			w, ok := weights[n]
			if !ok {
				w = 20
			}
			tot += w
		}
		totals = append(totals, tot)
		sum += tot
		if tot > max {
			max = tot
		}
	}
	// No shard above the heaviest single test plus a fair share of the rest.
	if float64(max) > float64(maxW)+0.25*float64(sum) {
		return fmt.Sprintf("shards do not balance: %v", totals)
	}
	return ""
}

func TestProvidersyncEveryTestIsInExactlyOneRaceShard(t *testing.T) {
	weights, awk := providersyncPaths(t)
	if p := providersyncPartitionProblem(t, awk, weights, providersyncNames(t)); p != "" {
		t.Fatal(p)
	}
}

func TestProvidersyncPlantedPartitionGuardFailsOnLostDuplicatedOrEmptyShard(t *testing.T) {
	weights, _ := providersyncPaths(t)
	names := providersyncNames(t)
	mutants := map[string]string{
		"loses a test":      "$0 ~ /^Test/ && NR > 1 && (++n - 1) % count + 1 == shard { print }\n",
		"duplicates a test": "NR == 1 { first = $0 }\n$0 ~ /^Test/ { n++; if ((n - 1) % count + 1 == shard) print }\nEND { if (shard == 1 && count > 1) print first }\n",
		"empty last shard":  "$0 ~ /^Test/ && shard < count { print }\n",
	}
	for name, body := range mutants {
		if providersyncPartitionProblem(t, providersyncWrite(t, "m.awk", body), weights, names) == "" {
			t.Errorf("partition guard accepted an awk that %s", name)
		}
	}
}

func TestProvidersyncRaceWeightsNameRealTestsAndBalance(t *testing.T) {
	weights, awk := providersyncPaths(t)
	if p := providersyncWeightsProblem(t, awk, weights, providersyncNames(t)); p != "" {
		t.Fatal(p)
	}
}

func TestProvidersyncPlantedWeightsGuardFailsOnUnsortedMissingDuplicateAndUnbalanced(t *testing.T) {
	weights, awk := providersyncPaths(t)
	names := providersyncNames(t)
	keys, _ := providersyncParseWeights(t, weights)
	if len(keys) < 2 {
		t.Fatal("real weights table has fewer than two rows")
	}
	row := func(k string, w int) string { return k + "\t" + strconv.Itoa(w) + "\n" }
	cases := map[string]string{
		"unsorted":  row(keys[1], 1000) + row(keys[0], 1000),
		"missing":   row(keys[0], 1000) + row("TestNoSuchProvidersyncTest", 5),
		"duplicate": row(keys[0], 1000) + row(keys[0], 1000),
	}
	for name, body := range cases {
		if providersyncWeightsProblem(t, awk, providersyncWrite(t, "w.tsv", body), names) == "" {
			t.Errorf("weights guard accepted a %s table", name)
		}
	}
	// A shard awk that puts every test in shard 1 is a lopsided split the balance bound must refuse.
	lopsided := providersyncWrite(t, "lop.awk", "$0 ~ /^Test/ && shard == 1 { print }\n")
	if providersyncWeightsProblem(t, lopsided, weights, names) == "" {
		t.Error("weights guard accepted an unbalanced split")
	}
}

func providersyncCalls(calls []string) []string {
	var r []string
	for _, c := range calls {
		if strings.HasSuffix(c, "./internal/providersync") {
			r = append(r, c)
		}
	}
	return r
}

func providersyncRunLeg(t *testing.T, env ...string) legRun {
	t.Helper()
	return runCheckGo(t, env, false, "ci-leg", "race", "1", "3")
}

func TestProvidersyncRaceLegWhoseShardsDoNotPartitionFailsBeforeAnyTestRuns(t *testing.T) {
	empty := providersyncWrite(t, "empty.tsv", "# no rows\n")
	r := providersyncRunLeg(t, "GO_PROVIDERSYNC_RACE_WEIGHTS="+empty)
	if r.Err == nil {
		t.Fatalf("leg passed: %s", r.Stdout)
	}
	if !strings.Contains(r.Stderr, providersyncAwkName+" failed") {
		t.Errorf("missing awk failure message: %s", r.Stderr)
	}
	if len(providersyncCalls(r.Calls)) != 0 {
		t.Errorf("a providersync test ran: %v", r.Calls)
	}
}

func TestProvidersyncPlantedRaceLegWithRealWeightsPassesTheShardCheck(t *testing.T) {
	r := providersyncRunLeg(t)
	if r.Err != nil {
		t.Fatalf("clean leg failed: %v\n%s%s", r.Err, r.Stderr, r.Stdout)
	}
	if strings.Contains(r.Stderr, providersyncAwkName+" failed") || len(providersyncCalls(r.Calls)) != 1 {
		t.Errorf("clean leg shows the planted failure or no slice run: %s %v", r.Stderr, r.Calls)
	}
}

// providersyncRunNames: the test names of the -run regex of a recorded providersync call.
func providersyncRunNames(t *testing.T, call string) []string {
	t.Helper()
	w := strings.Fields(call)
	for i, x := range w {
		if x == "-run" && i+1 < len(w) {
			re := w[i+1]
			if !strings.HasPrefix(re, "^(") || !strings.HasSuffix(re, ")$") {
				t.Fatalf("unexpected -run regex %q", re)
			}
			return strings.Split(re[2:len(re)-2], "|")
		}
	}
	t.Fatalf("no -run in %q", call)
	return nil
}

func providersyncAssertListingCall(t *testing.T, r legRun) {
	t.Helper()
	var calls []string
	for _, c := range r.ListCalls {
		if strings.HasSuffix(c, "./internal/providersync") {
			calls = append(calls, c)
		}
	}
	if len(calls) == 0 {
		t.Fatal("the providersync listing was never requested")
	}
	for _, c := range calls {
		if !strings.Contains(" "+c+" ", " -race ") {
			t.Errorf("listing is not the race build's: %s", c)
		}
		if !strings.Contains(" "+c+" ", " -list .* ") {
			t.Errorf("listing is not of every name: %s", c)
		}
	}
}

func TestProvidersyncEachRaceLegRunsExactlyItsOwnSlice(t *testing.T) {
	weights, awk := providersyncPaths(t)
	names := providersyncNames(t)
	var union []string
	for shard := 1; shard <= 3; shard++ {
		r := runCheckGo(t, nil, false, "ci-leg", "race", strconv.Itoa(shard), "3")
		if r.Err != nil {
			t.Fatalf("leg %d: %v\n%s%s", shard, r.Err, r.Stderr, r.Stdout)
		}
		calls := providersyncCalls(r.Calls)
		if len(calls) != 1 {
			t.Fatalf("leg %d: want one providersync call, got %v", shard, calls)
		}
		ran := providersyncRunNames(t, calls[0])
		want, err := providersyncShard(t, awk, weights, names, shard, 3)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(providersyncSorted(ran), "\n") != strings.Join(providersyncSorted(want), "\n") {
			t.Errorf("leg %d does not run its own slice", shard)
		}
		union = append(union, ran...)
		providersyncAssertListingCall(t, r)
	}
	if strings.Join(providersyncSorted(union), "\n") != strings.Join(providersyncSorted(names), "\n") {
		t.Error("the legs' slices are not the listed set")
	}
}

func TestProvidersyncPlantedLegSliceDiffersFromTheExpectedSliceUnderAnotherAwk(t *testing.T) {
	// A valid partition with another assignment: leg 1 runs a different slice, so the own-slice comparison must see it.
	weights, awk := providersyncPaths(t)
	names := providersyncNames(t)
	rr := providersyncWrite(t, "rr.awk", "$0 ~ /^Test/ { n++; if ((n - 1) % count + 1 == shard) print }\n")
	r := providersyncRunLeg(t, "GO_PROVIDERSYNC_RACE_SHARD_AWK="+rr)
	if r.Err != nil {
		t.Fatalf("round-robin partition leg failed: %v\n%s", r.Err, r.Stderr)
	}
	calls := providersyncCalls(r.Calls)
	if len(calls) != 1 {
		t.Fatalf("want one providersync call: %v", calls)
	}
	ran := providersyncRunNames(t, calls[0])
	want, _ := providersyncShard(t, awk, weights, names, 1, 3)
	if strings.Join(providersyncSorted(ran), "\n") == strings.Join(providersyncSorted(want), "\n") {
		t.Error("planted awk produced the same slice; the own-slice guard could not fail")
	}
}

func providersyncMutatedAwk(t *testing.T, from, to string) string {
	t.Helper()
	_, awk := providersyncPaths(t)
	b, err := os.ReadFile(awk)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), from) {
		t.Fatalf("real awk does not contain %q", from)
	}
	return providersyncWrite(t, "mut.awk", strings.Replace(string(b), from, to, 1))
}

func TestProvidersyncShardAwkThatLosesATestFailsTheLegAtThePartitionCheck(t *testing.T) {
	// Every shard stays non-empty, so only the exact-partition comparison can catch the lost test.
	broken := providersyncMutatedAwk(t, "$0 ~ /^Test/ {", "$0 ~ /^Test/ && NR > 1 {")
	r := providersyncRunLeg(t, "GO_PROVIDERSYNC_RACE_SHARD_AWK="+broken)
	if r.Err == nil {
		t.Fatalf("leg passed: %s", r.Stdout)
	}
	if !strings.Contains(r.Stderr, "not an exact partition") {
		t.Errorf("missing partition message: %s", r.Stderr)
	}
	if len(providersyncCalls(r.Calls)) != 0 {
		t.Errorf("a providersync test ran: %v", r.Calls)
	}
}

func TestProvidersyncPlantedLostTestAwkIsTheOnlyDifferenceFromTheRealAwk(t *testing.T) {
	// Same mutation text with no change: the leg passes, so the failure above comes from the lost test alone.
	same := providersyncMutatedAwk(t, "$0 ~ /^Test/ {", "$0 ~ /^Test/ {")
	r := providersyncRunLeg(t, "GO_PROVIDERSYNC_RACE_SHARD_AWK="+same)
	if r.Err != nil || strings.Contains(r.Stderr, "not an exact partition") {
		t.Fatalf("unmutated copy failed: %v\n%s", r.Err, r.Stderr)
	}
}

func TestProvidersyncFailingSliceFailsTheLegAndStopsIt(t *testing.T) {
	// The stand-in fails ONLY the providersync slice, so `|| true` on that call would be visible.
	r := providersyncRunLeg(t, "FAKE_GO_FAIL_ONLY=providersync")
	if len(providersyncCalls(r.Calls)) == 0 {
		t.Fatal("the providersync slice was never run")
	}
	if r.Err == nil {
		t.Fatalf("leg passed: %s", r.Stdout)
	}
	if strings.Contains(r.Stdout, "ci-leg race: OK") || strings.Contains(r.Stdout, "<== stage race 1/3 done") {
		t.Errorf("leg reported success: %s", r.Stdout)
	}
	if len(r.Calls) != len(providersyncCalls(r.Calls)) {
		t.Errorf("the leg went on to the package run after a failed slice: %v", r.Calls)
	}
}

func TestProvidersyncFailingPackageRaceRunFailsTheLegAfterAGreenSlice(t *testing.T) {
	r := providersyncRunLeg(t, "FAKE_GO_FAIL_ONLY=packages")
	if len(providersyncCalls(r.Calls)) == 0 {
		t.Fatal("the providersync slice was never run")
	}
	if len(r.Calls) == len(providersyncCalls(r.Calls)) {
		t.Fatal("the package run was never reached")
	}
	if r.Err == nil {
		t.Fatalf("leg passed: %s", r.Stdout)
	}
	if strings.Contains(r.Stdout, "<== stage race 1/3 done") || strings.Contains(r.Stdout, "ci-leg race: OK") {
		t.Errorf("leg reported success: %s", r.Stdout)
	}
}

func TestProvidersyncPlantedFailureGuardsSeeAGreenLegWhenNothingFails(t *testing.T) {
	// Without FAKE_GO_FAIL_ONLY the same leg ends OK, so the two failure tests above assert a real difference.
	r := providersyncRunLeg(t)
	if r.Err != nil {
		t.Fatalf("clean leg failed: %v\n%s", r.Err, r.Stderr)
	}
	if !strings.Contains(r.Stdout, "ci-leg race: OK") || !strings.Contains(r.Stdout, "<== stage race 1/3 done") {
		t.Errorf("clean leg lacks the OK/done lines: %s", r.Stdout)
	}
	if len(r.Calls) == len(providersyncCalls(r.Calls)) {
		t.Error("clean leg did not reach the package run")
	}
}

func TestProvidersyncNonTestNameInTheRaceListingFailsBeforeAnyTestRuns(t *testing.T) {
	r := providersyncRunLeg(t, "FAKE_GO_LIST_EXTRA=FuzzSomething")
	if r.Err == nil {
		t.Fatalf("leg passed: %s", r.Stdout)
	}
	if !strings.Contains(r.Stderr, "FuzzSomething") || !strings.Contains(r.Stderr, "no race shard would run") {
		t.Errorf("missing non-Test message: %s", r.Stderr)
	}
	providersyncAssertListingCall(t, r)
	if len(providersyncCalls(r.Calls)) != 0 {
		t.Errorf("a providersync test ran: %v", r.Calls)
	}
}

func TestProvidersyncPlantedListingWithoutExtraNameIsAccepted(t *testing.T) {
	r := providersyncRunLeg(t)
	if r.Err != nil || strings.Contains(r.Stderr, "no race shard would run") {
		t.Fatalf("clean listing rejected: %v\n%s", r.Err, r.Stderr)
	}
	providersyncAssertListingCall(t, r)
}

func providersyncSwapAwk(t *testing.T) string {
	return providersyncWrite(t, "swap.awk",
		"NR == 1 { first = $0 }\n"+
			"$0 ~ /^Test/ { n++; if ((n - 1) % count + 1 == shard) {"+
			" if (shard == 2 && !done) { done = 1; print first } else print } }\n")
}

func TestProvidersyncShardAwkThatSwapsATestFailsAtTheSetCompare(t *testing.T) {
	// Shard 2 runs the first test instead of its own first one: every shard non-empty, count unchanged, so only the
	// sorted-set comparison can catch "one test twice, one test lost".
	r := providersyncRunLeg(t, "GO_PROVIDERSYNC_RACE_SHARD_AWK="+providersyncSwapAwk(t))
	if r.Err == nil {
		t.Fatalf("leg passed: %s", r.Stdout)
	}
	if !strings.Contains(r.Stderr, "not an exact partition") {
		t.Errorf("missing partition message: %s", r.Stderr)
	}
	if len(providersyncCalls(r.Calls)) != 0 {
		t.Errorf("a providersync test ran: %v", r.Calls)
	}
}

func TestProvidersyncPlantedSwapAwkKeepsEveryShardNonEmptyAndTheCount(t *testing.T) {
	// Proves the swap mutant evades the cheap checks (non-empty shards, same total), so the set compare is the only catch.
	names := providersyncNames(t)
	weights, _ := providersyncPaths(t)
	swap := providersyncSwapAwk(t)
	total := 0
	for shard := 1; shard <= 3; shard++ {
		part, err := providersyncShard(t, swap, weights, names, shard, 3)
		if err != nil || len(part) == 0 {
			t.Fatalf("shard %d empty or failed: %v", shard, err)
		}
		total += len(part)
	}
	if total != len(names) {
		t.Errorf("swap mutant changes the count: %d vs %d", total, len(names))
	}
	if providersyncPartitionProblem(t, swap, weights, names) == "" {
		t.Error("the partition guard did not catch the swap mutant")
	}
}

func providersyncBalanceSizes(t *testing.T, awk string) []int {
	t.Helper()
	// One heavy listed test and six unlisted ones over three shards.
	weights := providersyncWrite(t, "w.tsv", "TestHeavy\t1000\n")
	names := []string{"TestHeavy"}
	for _, c := range "ABCDEF" {
		names = append(names, "Test"+string(c))
	}
	var sizes []int
	for shard := 1; shard <= 3; shard++ {
		part, err := providersyncShard(t, awk, weights, names, shard, 3)
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(part))
		if shard == 1 && awk != "" && (len(part) != 1 || part[0] != "TestHeavy") {
			sizes = append(sizes, -1) // heavy test not alone: flagged by the caller
		}
	}
	return sizes
}

func TestProvidersyncShardAwkBalancesByWeightAndWeighsAnUnlistedTest(t *testing.T) {
	// By weight the heavy test sits alone and the six unlisted tests (default weight > 0) split 3/3.
	_, awk := providersyncPaths(t)
	got := providersyncBalanceSizes(t, awk)
	if fmt.Sprint(got) != "[1 3 3]" {
		t.Fatalf("sizes %v, want [1 3 3]", got)
	}
}

func TestProvidersyncPlantedAwkIgnoringWeightsOrWeighingUnlistedZeroIsCaught(t *testing.T) {
	rr := providersyncWrite(t, "rr.awk", "$0 ~ /^Test/ { n++; if ((n - 1) % count + 1 == shard) print }\n")
	if got := fmt.Sprint(providersyncBalanceSizes(t, rr)); got == "[1 3 3]" {
		t.Errorf("an awk ignoring weights was not caught: %s", got)
	}
	zero := providersyncMutatedAwk(t, "defw = 20", "defw = 0")
	if got := fmt.Sprint(providersyncBalanceSizes(t, zero)); got == "[1 3 3]" {
		t.Errorf("an awk weighing unlisted tests 0 was not caught: %s", got)
	}
}
