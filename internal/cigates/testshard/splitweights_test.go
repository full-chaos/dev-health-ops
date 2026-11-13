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

// CHAOS-13668: a go-quality test leg cuts a split package's test names by ci/go_test_split_weights.tsv (ci/go_race_shard.awk
// cut(), names mode): the measured names longest first onto the lightest part, any other name by djb2(name) mod parts. So adding
// or removing an unmeasured test moves no other test, and the plan derives each part's weight from the rows, with no hand-kept
// part row that could describe a part that no longer exists. A row naming a test `go test -list` no longer reports fails.

func djb2(name string) uint32 {
	h := uint32(5381)
	for i := 0; i < len(name); i++ {
		h = h*33 + uint32(name[i])
	}
	return h
}

// cutMirror is the Go mirror of cut(): part (1-based) per listed name.
func cutMirror(rows map[string]int, parts int, names []string) map[string]int {
	measured := make([]string, 0, len(rows))
	for n := range rows {
		measured = append(measured, n)
	}
	sort.Slice(measured, func(i, j int) bool {
		if rows[measured[i]] != rows[measured[j]] {
			return rows[measured[i]] > rows[measured[j]]
		}
		return measured[i] < measured[j]
	})
	load := make([]int, parts+1)
	at := map[string]int{}
	for _, n := range measured {
		best := 1
		for p := 2; p <= parts; p++ {
			if load[p] < load[best] {
				best = p
			}
		}
		at[n] = best
		load[best] += rows[n]
	}
	out := map[string]int{}
	for _, n := range names {
		if p, ok := at[n]; ok {
			out[n] = p
		} else {
			out[n] = int(djb2(n)%uint32(parts)) + 1
		}
	}
	return out
}

// derivedPartWeights: "<key>#<part>" -> the weight the chain plan gives that part: its rows plus ceil(max(W - all rows, 0) /
// parts), at least 1 (W = the package row, else 1).
func derivedPartWeights(tests map[string]map[string]int, splits, weights map[string]int) map[string]int {
	out := map[string]int{}
	for key, rows := range tests {
		parts := splits[key]
		if parts < 2 {
			continue
		}
		load := make([]int, parts+1)
		total := 0
		for n, p := range cutMirror(rows, parts, keysOf(rows)) {
			load[p] += rows[n]
			total += rows[n]
		}
		w, ok := weights[key]
		if !ok {
			w = 1
		}
		left := max(w-total, 0)
		for p := 1; p <= parts; p++ {
			out[fmt.Sprintf("%s#%d", key, p)] = max(load[p]+(left+parts-1)/parts, 1)
		}
	}
	return out
}

func keysOf(m map[string]int) []string {
	var k []string
	for n := range m {
		k = append(k, n)
	}
	sort.Strings(k)
	return k
}

func withPartWeights(weights, parts map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range weights {
		out[k] = v
	}
	for k, v := range parts {
		out[k] = v
	}
	return out
}

func writeTestRows(t *testing.T, dir string, tests map[string]map[string]int) string {
	t.Helper()
	var lines []string
	for key, rows := range tests {
		for n, s := range rows {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%d", key, n, s))
		}
	}
	sort.Strings(lines)
	p := filepath.Join(dir, "split_weights.tsv")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// splitWeightRows reads ci/go_test_split_weights.tsv: package -> test -> seconds, failing on a malformed table.
func splitWeightRows(t *testing.T) map[string]map[string]int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_test_split_weights.tsv"))
	if err != nil {
		t.Fatalf("ci/go_test_split_weights.tsv: %v", err)
	}
	rows := map[string]map[string]int{}
	var keys []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 3 {
			t.Fatalf("split weights row is not <package><TAB><test><TAB><seconds>: %q", line)
		}
		n, err := strconv.Atoi(f[2])
		if err != nil || n < 1 || strconv.Itoa(n) != f[2] {
			t.Fatalf("split weights row %q has no positive integer seconds", line)
		}
		if !strings.HasPrefix(f[1], "Test") && !strings.HasPrefix(f[1], "Example") && !strings.HasPrefix(f[1], "Fuzz") {
			t.Fatalf("split weights row %q names no test go test can run", line)
		}
		if _, dup := rows[f[0]][f[1]]; dup {
			t.Fatalf("split weights row %q repeats its test", line)
		}
		if rows[f[0]] == nil {
			rows[f[0]] = map[string]int{}
		}
		rows[f[0]][f[1]] = n
		keys = append(keys, f[0]+"\x00"+f[1])
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatal("ci/go_test_split_weights.tsv is not sorted by package, then test (LC_ALL=C)")
	}
	return rows
}

func TestTheSplitWeightsTableIsClean(t *testing.T) {
	t.Parallel()
	splits := splitRows(t)
	rows := splitWeightRows(t)
	for key := range rows {
		if _, ok := splits[key]; !ok {
			t.Errorf("ci/go_test_split_weights.tsv has rows of %s, which ci/go_test_splits.tsv does not split", key)
		}
	}
	for key, parts := range splits {
		if n := len(rows[key]); n < parts {
			t.Errorf("split package %s has %d test row(s), fewer than its %d parts: a part could be empty", key, n, parts)
		}
	}
}

// awkCut runs the names mode of AWK (every name with its part); the error carries stderr.
func awkCut(t *testing.T, awk, key string, parts int, tests map[string]map[string]int, names []string) (map[string]int, error) {
	t.Helper()
	cmd := exec.Command("awk", "-v", "cutkey="+key, "-v", fmt.Sprintf("cutparts=%d", parts), "-v", "tests="+writeTestRows(t, t.TempDir(), tests), "-f", awk)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	cmd.Stdin = strings.NewReader(strings.Join(names, "\n") + "\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err, out)
	}
	got := map[string]int{}
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		n, p, ok := strings.Cut(l, "\t")
		pi, err := strconv.Atoi(p)
		if !ok || err != nil {
			return nil, fmt.Errorf("malformed names-mode line %q", l)
		}
		got[n] = pi
	}
	return got, nil
}

// planAwk runs the chain=1 plan of AWK over PKGS for shard 1 of COUNT (stderr in the error).
func planAwk(t *testing.T, awk string, weights, splits map[string]int, tests map[string]map[string]int, pkgs []string, count int) error {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, rows map[string]int) string {
		var b strings.Builder
		for _, k := range keysOf(rows) {
			fmt.Fprintf(&b, "%s\t%d\n", k, rows[k])
		}
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	var in strings.Builder
	for _, p := range pkgs {
		in.WriteString(fixtureMod + "/" + p + "\n")
	}
	cmd := exec.Command("awk", "-v", "mod="+fixtureMod, "-v", "shard=1", "-v", fmt.Sprintf("count=%d", count), "-v", "weights="+write("w.tsv", weights),
		"-v", "splits="+write("s.tsv", splits), "-v", "tests="+writeTestRows(t, dir, tests), "-v", "serial=none", "-v", "chain=1", "-v", "defw=1", "-f", awk)
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v: %s", err, out)
	}
	return nil
}

var (
	r1Rows  = map[string]map[string]int{"s": {"TestHeavy": 100, "TestMid": 50}}
	r1Names = []string{"TestB", "TestD", "TestF", "TestH", "TestHeavy", "TestMid", "TestP", "TestR"}
	// Four equal rows on three parts: the name tie and the lightest-part tie both decide a part.
	tieRows  = map[string]map[string]int{"s": {"TestA": 10, "TestB": 10, "TestC": 10, "TestD": 10}}
	tieNames = []string{"ExampleZ", "FuzzY", "TestA", "TestB", "TestC", "TestD", "TestE", "TestF", "TestG"}
)

// R1 (red on main: round robin over the sorted names moves every later name): adding or removing an unmeasured test leaves every
// other test in its part.
func cellAddingOneTestMovesNoOtherTest(t *testing.T, awk string) string {
	before, err := awkCut(t, awk, "s", 2, r1Rows, r1Names)
	if err != nil {
		return "names mode failed: " + err.Error()
	}
	for _, v := range []struct {
		what  string
		names []string
	}{
		{"add TestA (sorts first)", append([]string{"TestA"}, r1Names...)},
		{"add TestE (sorts in the middle)", append(append([]string{}, r1Names...), "TestE")},
		{"add TestZ (sorts last)", append(append([]string{}, r1Names...), "TestZ")},
		{"remove TestD", []string{"TestB", "TestF", "TestH", "TestHeavy", "TestMid", "TestP", "TestR"}},
	} {
		sort.Strings(v.names)
		after, err := awkCut(t, awk, "s", 2, r1Rows, v.names)
		if err != nil {
			return v.what + ": names mode failed: " + err.Error()
		}
		for n, p := range after {
			if q, ok := before[n]; ok && q != p {
				return fmt.Sprintf("%s: %s moved from part %d to part %d", v.what, n, q, p)
			}
		}
	}
	return ""
}

// R2: a row naming a test go test -list no longer reports fails the cut.
func cellAStaleRowFails(t *testing.T, awk string) string {
	_, err := awkCut(t, awk, "s", 2, map[string]map[string]int{"s": {"TestGone": 30, "TestHeavy": 100}}, []string{"TestB", "TestHeavy"})
	if err == nil || !strings.Contains(err.Error(), "TestGone") || !strings.Contains(err.Error(), "no longer reports") {
		return fmt.Sprintf("a row naming an unlisted test passed the cut: %v", err)
	}
	return ""
}

// R3: a hand "<key>#<part>" row in the weights fails the chain plan.
func cellAHandPartRowFails(t *testing.T, awk string) string {
	err := planAwk(t, awk, map[string]int{"s": 100, "s#1": 60}, map[string]int{"s": 2}, r1Rows, []string{"s"}, 2)
	if err == nil || !strings.Contains(err.Error(), "part row s#1") {
		return fmt.Sprintf("a hand part row passed the chain plan: %v", err)
	}
	return ""
}

// R4: fewer rows than parts fails the cut and the plan (a part could be empty).
func cellFewerRowsThanPartsFails(t *testing.T, awk string) string {
	one := map[string]map[string]int{"s": {"TestHeavy": 100}}
	if _, err := awkCut(t, awk, "s", 2, one, r1Names); err == nil || !strings.Contains(err.Error(), "fewer than its 2 parts") {
		return fmt.Sprintf("names mode with 1 row for 2 parts: %v", err)
	}
	if err := planAwk(t, awk, map[string]int{"s": 100}, map[string]int{"s": 2}, one, []string{"s"}, 2); err == nil || !strings.Contains(err.Error(), "fewer than its 2 parts") {
		return fmt.Sprintf("the plan with 1 row for 2 parts: %v", err)
	}
	return ""
}

// The cut equals its Go mirror on fixtures where the name tie, the part tie and the hash each decide a part.
func cellTheCutMatchesItsMirror(t *testing.T, awk string) string {
	for _, c := range []struct {
		rows  map[string]map[string]int
		parts int
		names []string
	}{{tieRows, 3, tieNames}, {r1Rows, 2, r1Names}} {
		got, err := awkCut(t, awk, "s", c.parts, c.rows, c.names)
		if err != nil {
			return "names mode failed: " + err.Error()
		}
		want := cutMirror(c.rows["s"], c.parts, c.names)
		for _, n := range c.names {
			if got[n] != want[n] {
				return fmt.Sprintf("%s: awk part %d, mirror part %d", n, got[n], want[n])
			}
		}
	}
	return ""
}

func TestAddingOneUnmeasuredTestMovesNoOtherTest(t *testing.T) {
	t.Parallel()
	if d := cellAddingOneTestMovesNoOtherTest(t, realShardAwk(t)); d != "" {
		t.Error(d)
	}
}

func TestASplitWeightRowNamingNoListedTestFails(t *testing.T) {
	t.Parallel()
	if d := cellAStaleRowFails(t, realShardAwk(t)); d != "" {
		t.Error(d)
	}
}

func TestAHandPartRowIsRefused(t *testing.T) {
	t.Parallel()
	if d := cellAHandPartRowFails(t, realShardAwk(t)); d != "" {
		t.Error(d)
	}
}

func TestFewerSplitWeightRowsThanPartsFail(t *testing.T) {
	t.Parallel()
	if d := cellFewerRowsThanPartsFails(t, realShardAwk(t)); d != "" {
		t.Error(d)
	}
}

func TestTheSplitCutMatchesItsGoMirror(t *testing.T) {
	t.Parallel()
	if d := cellTheCutMatchesItsMirror(t, realShardAwk(t)); d != "" {
		t.Error(d)
	}
}

// One planted mutant per rule of the cut; each cell holds with the real awk and fails with its mutant.
func TestTheSplitCutPlantedMutantsAreCaught(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile(realShardAwk(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		rule, from, to string
		cell           func(*testing.T, string) string
	}{
		{"an unmeasured name goes by its own hash, not by its place in the list (round robin)",
			"      p = partOf(cutkey, np, lname[i])\n", "      p = (i - 1) % np + 1\n", cellAddingOneTestMovesNoOtherTest},
		{"a stale row fails the cut", "      if (!(tname[cutkey, i] in listed)) die(", "      if (0) die(", cellAStaleRowFails},
		{"a hand part row fails the chain plan", "      if (chain && index(f[1], \"#\")) die(", "      if (0) die(", cellAHandPartRowFails},
		{"fewer rows than parts fails the cut",
			"    if (tn[cutkey] + 0 < np) die(", "    if (0) die(", cellFewerRowsThanPartsFails},
		{"fewer rows than parts fails the plan",
			"      if (tn[key] + 0 < np) die(", "      if (0) die(", cellFewerRowsThanPartsFails},
		{"measured names tie by name, ascending", "trow[key, o[j]] == trow[key, v] && o[j] > v", "trow[key, o[j]] == trow[key, v] && o[j] < v", cellTheCutMatchesItsMirror},
		{"a measured name goes to the lightest part, the lowest index on a tie",
			"if (psum[key, p] < psum[key, best]) best = p", "if (psum[key, p] <= psum[key, best]) best = p", cellTheCutMatchesItsMirror},
		{"the hash is djb2", "(h * 33 + ord[", "(h * 31 + ord[", cellTheCutMatchesItsMirror},
	} {
		if n := strings.Count(string(src), c.from); n != 1 {
			t.Fatalf("%s: ci/go_race_shard.awk holds %q %d times, not once: update this planted mutant", c.rule, c.from, n)
		}
		if d := c.cell(t, realShardAwk(t)); d != "" {
			t.Errorf("%s: the real awk fails its cell: %s", c.rule, d)
		}
		mutant := filepath.Join(t.TempDir(), "mutant.awk")
		if err := os.WriteFile(mutant, []byte(strings.Replace(string(src), c.from, c.to, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
		if d := c.cell(t, mutant); d == "" {
			t.Errorf("%s: the mutant passed its cell", c.rule)
		}
	}
}

// The wiring: check_go.sh hands the real tables to the cut and the plan, so a stale row fails a real leg's split list and a hand
// part row fails a real leg's plan.
func TestCheckGoFailsAStaleSplitWeightRowAndAHandPartRow(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	_, parts := shardPlanParts(t, 1, shardCount)
	if len(parts) == 0 {
		t.Fatalf("test shard 1/%d holds no split part", shardCount)
	}
	key := keyOf(mod, strings.SplitN(parts[0], "#", 2)[0])
	src, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_test_split_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(t.TempDir(), "split_weights.tsv")
	if err := os.WriteFile(stale, append(src, []byte(key+"\tTestNoSuchTestCHAOS13668\t5\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	_, errs, err := checkGo(t, []string{"GO_TEST_SPLIT_WEIGHTS=" + stale}, "test-shard-split-list", "1", fmt.Sprint(shardCount))
	if err == nil || !strings.Contains(errs, "TestNoSuchTestCHAOS13668") || !strings.Contains(errs, "no longer reports") {
		t.Errorf("a split weights row naming no test of %s passed leg 1's split list: %v\n%s", key, err, errs)
	}
	w, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_test_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	hand := filepath.Join(t.TempDir(), "weights.tsv")
	if err := os.WriteFile(hand, append(w, []byte(key+"#1\t60\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	_, errs, err = checkGo(t, []string{"GO_TEST_WEIGHTS=" + hand}, "test-shard-plan", "1", fmt.Sprint(shardCount))
	if err == nil || !strings.Contains(errs, "part row "+key+"#1") {
		t.Errorf("a hand part row passed leg 1's plan: %v\n%s", err, errs)
	}
}

// The race legs and the auth legs never read ci/go_test_split_weights.tsv: their plans are byte for byte the same with a broken
// table in its place (the auth split packages keep their round robin cut).
func TestTheRaceAndAuthLegPlansDoNotReadTheSplitWeights(t *testing.T) {
	t.Parallel()
	broken := filepath.Join(t.TempDir(), "split_weights.tsv")
	if err := os.WriteFile(broken, []byte("not a row\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		env  []string
		args []string
	}{
		{nil, []string{"race-shard-plan", "1", "6"}},
		{[]string{"AUTH_LEG_LIST=1"}, []string{"auth", "race-leg", "1", "3"}},
		{[]string{"AUTH_LEG_LIST=1"}, []string{"auth", "race-integration-leg", "1", "4"}},
	} {
		out, errs, err := checkGo(t, c.env, c.args...)
		if err != nil {
			t.Fatalf("%v: %v\n%s", c.args, err, errs)
		}
		bout, berrs, err := checkGo(t, append(append([]string{}, c.env...), "GO_TEST_SPLIT_WEIGHTS="+broken), c.args...)
		if err != nil || bout != out || berrs != errs {
			t.Errorf("%v reads ci/go_test_split_weights.tsv: a broken table changes its plan (err %v)", c.args, err)
		}
	}
}
