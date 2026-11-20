package testshard_test

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13664: check_test_shard runs a test shard's split-package jobs one after another (a chain) beside one go test of its
// other packages (the rest), then the serial package alone, so a leg's wall is about max(chain, rest / 2) + serial + 15 s
// (+6..+34 s over the 50 test legs of 5 main runs at 10 legs). The test-leg plan (ci/go_race_shard.awk chain=1) places the
// chain and the serial package first, then the rest under the highest modelled wall. A part weighs what its test rows give it
// (CHAOS-13668, ci/go_test_split_weights.tsv); the model reads those derived weights as "<key>#<part>" entries of its weights map.

// plannedWallCap: the highest modelled wall a test shard may plan. CHAOS-13800: 600 at 13 legs, as whole runs measure up to 1.6x
// slower than their medians (600 x 1.5 = 900, check_go.sh's cap). CHAOS-14341: the rows are main-push medians in both directions
// (a go.sum change run measured like the others), so the model is a main-push leg's wall: max 523 at 13 legs. The floor is one test:
// chartgates TestWorkerSecretKeysGuardsRealTree (row 440) alone in its part, 455 modelled.
const plannedWallCap = 600

type planItem struct {
	key         string
	part, parts int
}

type chainLoad struct {
	chain, rest, serial, heaviestChain int
}

func (l chainLoad) wall() float64 {
	return math.Max(float64(l.chain), float64(l.rest)/2) + float64(l.serial) + 15
}

// itemWeight is what the plan weighs an item by: a part's derived weight ("<key>#<part>" in weights, derivedPartWeights), else
// ceil(package weight / parts); no row weighs 1.
func itemWeight(it planItem, weights map[string]int) int {
	w, ok := weights[it.key]
	if !ok {
		w = 1
	}
	if it.parts < 2 {
		return w
	}
	if pw, ok := weights[fmt.Sprintf("%s#%d", it.key, it.part)]; ok {
		return pw
	}
	return (w + it.parts - 1) / it.parts
}

func chainLoadOf(items []planItem, weights map[string]int, serial string) chainLoad {
	var l chainLoad
	for _, it := range items {
		w := itemWeight(it, weights)
		switch {
		case it.key == serial:
			l.serial += w
		case it.parts > 1:
			l.chain += w
		default:
			l.rest += w
		}
		if (it.key == serial || it.parts > 1) && w > l.heaviestChain {
			l.heaviestChain = w
		}
	}
	return l
}

// chainPlanDefects: each shard's chain + serial load is within the longest-processing-time bound of the chain pass (the mean
// plus the heaviest chain item), and each shard's modelled wall is at most limit.
func chainPlanDefects(shards []chainLoad, limit float64) []string {
	total, heaviest := 0, 0
	for _, l := range shards {
		total += l.chain + l.serial
		if l.heaviestChain > heaviest {
			heaviest = l.heaviestChain
		}
	}
	var d []string
	for i, l := range shards {
		if l.chain+l.serial > total/len(shards)+heaviest {
			d = append(d, fmt.Sprintf("test shard %d/%d: chain + serial %d s is over the longest-processing-time bound %d s", i+1, len(shards), l.chain+l.serial, total/len(shards)+heaviest))
		}
		if l.wall() > limit {
			d = append(d, fmt.Sprintf("test shard %d/%d: modelled wall max(chain %d, rest %d / 2) + serial %d + 15 = %.0f s, over %.0f s", i+1, len(shards), l.chain, l.rest, l.serial, l.wall(), limit))
		}
	}
	return d
}

func serialHeavyKey(t *testing.T, mod string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^SERIAL_HEAVY_TEST_PACKAGE="([^"]+)"`).FindSubmatch(b)
	if m == nil {
		t.Fatal("ci/check_go.sh sets no SERIAL_HEAVY_TEST_PACKAGE")
	}
	return keyOf(mod, string(m[1]))
}

type chainFixture struct {
	count   int
	serial  string
	weights map[string]int
	splits  map[string]int
	tests   map[string]map[string]int // package -> test -> seconds (ci/go_test_split_weights.tsv)
	pkgs    []string
	limit   float64
}

const fixtureMod = "example.com/m"

// fixturePlan runs AWK over the fixture with check_go.sh's test-plan arguments: chain=1, or the sum-only form main ran before
// CHAOS-13664 (serialx=2, no chain).
func fixturePlan(t *testing.T, awk string, f chainFixture, chain bool) []chainLoad {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, rows map[string]int) string {
		var keys []string
		for k := range rows {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&b, "%s\t%d\n", k, rows[k])
		}
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	weights, splits, pins := write("weights.tsv", f.weights), write("splits.tsv", f.splits), write("pins.tsv", nil)
	tests := ""
	if f.tests != nil {
		tests = writeTestRows(t, dir, f.tests)
	}
	var in strings.Builder
	for _, p := range f.pkgs {
		in.WriteString(fixtureMod + "/" + p + "\n")
	}
	var loads []chainLoad
	for k := 1; k <= f.count; k++ {
		args := []string{"-v", "mod=" + fixtureMod, "-v", fmt.Sprintf("shard=%d", k), "-v", fmt.Sprintf("count=%d", f.count), "-v", "weights=" + weights,
			"-v", "pin=" + pins, "-v", "splits=" + splits, "-v", "serial=" + f.serial, "-v", "defw=1"}
		if tests != "" {
			args = append(args, "-v", "tests="+tests)
		}
		if chain {
			args = append(args, "-v", "chain=1")
		} else {
			args = append(args, "-v", "serialx=2")
		}
		cmd := exec.Command("awk", append(args, "-f", awk)...)
		cmd.Stdin = strings.NewReader(in.String())
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("awk shard %d/%d: %v", k, f.count, err)
		}
		var items []planItem
		for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if l == "" {
				continue
			}
			fl := strings.Split(l, "\t")
			it := planItem{key: strings.TrimPrefix(fl[0], fixtureMod+"/")}
			if len(fl) == 3 {
				it.part, _ = strconv.Atoi(fl[1])
				it.parts, _ = strconv.Atoi(fl[2])
			}
			items = append(items, it)
		}
		loads = append(loads, chainLoadOf(items, withPartWeights(f.weights, derivedPartWeights(f.tests, f.splits, f.weights)), f.serial))
	}
	return loads
}

func realShardAwk(t *testing.T) string {
	return filepath.Join(repoRoot(t), "ci", "go_race_shard.awk")
}

// The red cell: two parts of one split package and one heavy package on two shards. The sum-only split puts the two parts
// together (the heavy package takes a shard alone), so they run as one 200 s chain; the chain-aware plan puts one part on each.
var redCellFixture = chainFixture{count: 2, serial: "none", weights: map[string]int{"a": 200, "s": 200}, splits: map[string]int{"s": 2}, pkgs: []string{"a", "s"}, limit: 160}

func TestTheSumOnlySplitChainsTwoPartsOnOneShardAndFailsTheWallModel(t *testing.T) {
	t.Parallel()
	if d := chainPlanDefects(fixturePlan(t, realShardAwk(t), redCellFixture, false), redCellFixture.limit); len(d) == 0 {
		t.Fatal("the sum-only split of the red-cell fixture passed the wall model: the guard cannot see a chained shard")
	}
}

func TestTheChainAwarePlanPutsTheChainedPartsApart(t *testing.T) {
	t.Parallel()
	for _, d := range chainPlanDefects(fixturePlan(t, realShardAwk(t), redCellFixture, true), redCellFixture.limit) {
		t.Error(d)
	}
}

// One planted mutant per rule of the chain-aware plan; each fixture passes with the real awk and fails with its mutant.
func TestTheChainPlanPlantedMutantsAreCaught(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile(realShardAwk(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		rule, from, to string
		f              chainFixture
	}{
		{
			rule: "a part weighs the test rows cut to it",
			from: "      weight[n] = psum[key, pp] + int(((left > 0 ? left : 0) + np - 1) / np)\n", to: "      weight[n] = int((weight[n] + np - 1) / np)\n",
			f: chainFixture{count: 2, serial: "none", weights: map[string]int{"s": 200, "t": 200}, splits: map[string]int{"s": 2, "t": 2},
				tests: map[string]map[string]int{"s": {"TestA": 190, "TestB": 10}, "t": {"TestC": 190, "TestD": 10}}, pkgs: []string{"s", "t"}, limit: 250},
		},
		{
			rule: "a part takes its share of what the rows leave of the package weight",
			from: "      left = weight[n] - mtot[key]\n", to: "      left = 0\n",
			f: chainFixture{count: 2, serial: "g", weights: map[string]int{"g": 200, "s": 400}, splits: map[string]int{"s": 2},
				tests: map[string]map[string]int{"s": {"TestA": 150, "TestB": 1}}, pkgs: []string{"g", "s"}, limit: 360},
		},
		{
			rule: "the chain pass counts the serial load",
			from: "if (cl[s] + sl[s] < cl[best] + sl[best]) best = s", to: "if (cl[s] < cl[best]) best = s",
			f: chainFixture{count: 2, serial: "g", weights: map[string]int{"g": 200, "s": 200}, splits: map[string]int{"s": 2}, pkgs: []string{"g", "s"}, limit: 250},
		},
		{
			rule: "the rest goes under the highest modelled wall",
			from: "if (wall2(s, weight[k]) <= top && (best == 0 || rl[s] < rl[best])) best = s", to: "if (best == 0 || rl[s] < rl[best]) best = s",
			f: chainFixture{count: 2, serial: "g", weights: map[string]int{"g": 280, "s": 291, "r1": 100, "r2": 40}, splits: map[string]int{"s": 2},
				tests: map[string]map[string]int{"s": {"TestA": 290, "TestB": 1}}, pkgs: []string{"g", "r1", "r2", "s"}, limit: 310},
		},
	} {
		if n := strings.Count(string(src), c.from); n != 1 {
			t.Fatalf("%s: ci/go_race_shard.awk holds %q %d times, not once: update this planted mutant", c.rule, c.from, n)
		}
		for _, d := range chainPlanDefects(fixturePlan(t, realShardAwk(t), c.f, true), c.f.limit) {
			t.Errorf("%s: the real awk fails its fixture: %s", c.rule, d)
		}
		mutant := filepath.Join(t.TempDir(), "mutant.awk")
		if err := os.WriteFile(mutant, []byte(strings.Replace(string(src), c.from, c.to, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
		if d := chainPlanDefects(fixturePlan(t, mutant, c.f, true), c.f.limit); len(d) == 0 {
			t.Errorf("%s: the mutant passed the wall model", c.rule)
		}
	}
}

// solo packages run after the shard's other packages, which the chain model does not cover: chain=1 refuses a solo file.
func TestTheChainAwarePlanRefusesASoloFile(t *testing.T) {
	t.Parallel()
	solo := filepath.Join(t.TempDir(), "solo.tsv")
	if err := os.WriteFile(solo, []byte("a\treason\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("awk", "-v", "mod="+fixtureMod, "-v", "shard=1", "-v", "count=2", "-v", "chain=1", "-v", "solo="+solo, "-f", realShardAwk(t))
	cmd.Stdin = strings.NewReader(fixtureMod + "/a\n")
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 || !strings.Contains(string(out), "chain=1 takes no solo file") {
		t.Fatalf("chain=1 with a solo file: want exit 2 and the refusal, got %v: %s", err, out)
	}
}

// CHAOS-14334: the split packages of ci/go_test_apart.tsv shell out to ci/check_go.sh and run slower beside each other: test shard
// 6/13 held parts of all four (919-940 s on slow runners, over the 900 s cap). The test plan (ci/go_race_shard.awk tapart=) keeps
// each test shard to parts of at most one of them. The same plan with no apart rows is the red: it puts two or more on one shard.

func testApartRows(t *testing.T) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_test_apart.tsv"))
	if err != nil {
		t.Fatalf("ci/go_test_apart.tsv: %v", err)
	}
	rows := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "#") {
			continue
		}
		k, reason, ok := strings.Cut(l, "\t")
		if !ok || strings.TrimSpace(reason) == "" {
			t.Fatalf("ci/go_test_apart.tsv: row is not <package><TAB><reason>: %q", l)
		}
		rows[k] = true
	}
	if len(rows) < 2 {
		t.Fatalf("ci/go_test_apart.tsv holds %d row(s): nothing to keep apart", len(rows))
	}
	return rows
}

// testApartLegs plans every test leg with env; per leg, the listed packages it holds parts of, sorted.
func testApartLegs(t *testing.T, env []string, listed map[string]bool) [][]string {
	t.Helper()
	_, parts := allLegs(t, env, "test-shard-plan", shardCount)
	legs := make([][]string, shardCount)
	for k := 1; k <= shardCount; k++ {
		seen := map[string]bool{}
		for _, l := range strings.Split(parts[k], "\n") {
			f := strings.Split(l, "\t")
			if len(f) == 5 && f[0] == "." && listed[f[2]] && !seen[f[2]] {
				seen[f[2]] = true
				legs[k-1] = append(legs[k-1], f[2])
			}
		}
		sort.Strings(legs[k-1])
	}
	return legs
}

func TestNoTestShardHoldsPartsOfTwoApartPackages(t *testing.T) {
	t.Parallel()
	listed, splits := testApartRows(t), splitRows(t)
	planned := map[string]bool{}
	for k, leg := range testApartLegs(t, nil, listed) {
		if len(leg) > 1 {
			t.Errorf("test shard %d/%d holds parts of %s (ci/go_test_apart.tsv)", k+1, shardCount, strings.Join(leg, ", "))
		}
		for _, p := range leg {
			planned[p] = true
		}
		t.Logf("test shard %d/%d: %v", k+1, shardCount, leg)
	}
	for p := range listed {
		if splits[p] < 2 {
			t.Errorf("ci/go_test_apart.tsv row %s: no split package of ci/go_test_splits.tsv", p)
		}
		if !planned[p] {
			t.Errorf("ci/go_test_apart.tsv row %s: no test shard holds a part of it", p)
		}
	}
}

func TestTheTestPlanWithoutApartRowsPutsApartPackagesOnOneShard(t *testing.T) {
	t.Parallel()
	listed := testApartRows(t)
	none := filepath.Join(t.TempDir(), "apart.tsv")
	if err := os.WriteFile(none, []byte("# no rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, leg := range testApartLegs(t, []string{"GO_TEST_APART=" + none}, listed) {
		if len(leg) > 1 {
			return
		}
	}
	t.Errorf("the %d-leg test plan without apart rows keeps the ci/go_test_apart.tsv packages apart by itself: the red is gone", shardCount)
}

// testApartAwk runs ci/go_race_shard.awk (chain=1, tapart) for shard of count over packages m/<name> split 2 ways, weight 200 each.
func testApartAwk(t *testing.T, shard, count int, pkgs []string, apart string, chain bool) (string, string, error) {
	t.Helper()
	dir := t.TempDir()
	var w, s, in strings.Builder
	for _, p := range pkgs {
		fmt.Fprintf(&w, "%s\t200\n", p)
		fmt.Fprintf(&s, "%s\t2\n", p)
		in.WriteString("m/" + p + "\n")
	}
	write := func(name, text string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cmd := exec.Command("awk", "-v", "mod=m", "-v", "shard="+strconv.Itoa(shard), "-v", "count="+strconv.Itoa(count),
		"-v", "weights="+write("w.tsv", w.String()), "-v", "splits="+write("s.tsv", s.String()), "-v", "serial=none", "-v", "defw=1",
		"-v", "tapart="+write("a.tsv", apart), "-v", "chain="+map[bool]string{true: "1", false: ""}[chain], "-f", realShardAwk(t))
	cmd.Stdin = strings.NewReader(in.String())
	var out, errs strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errs
	err := cmd.Run()
	return out.String(), errs.String(), err
}

// Three packages of two 100 s parts on three shards: the plain chain plan puts a part of a and a part of b on shard 1; with a and
// b listed each shard holds parts of one of them; a row of no split package of the run is inert. On one shard the rule cannot hold,
// and the plan says so. Bad rows are refused.
func TestTheApartRuleKeepsListedPartsOffEachOthersShards(t *testing.T) {
	t.Parallel()
	pkgs := []string{"a", "b", "c"}
	holds := func(apart string) []string {
		var d []string
		for k := 1; k <= 3; k++ {
			out, errs, err := testApartAwk(t, k, 3, pkgs, apart, true)
			if err != nil || errs != "" {
				t.Fatalf("shard %d/3: %v\n%s", k, err, errs)
			}
			if strings.Contains(out, "m/a\t") && strings.Contains(out, "m/b\t") {
				d = append(d, fmt.Sprintf("shard %d/3 holds parts of a and b", k))
			}
		}
		return d
	}
	if d := holds("# no rows\n"); len(d) == 0 {
		t.Error("the fixture's plain chain plan keeps a and b apart by itself: the red is gone")
	}
	for _, d := range holds("a\tshells out\nb\tshells out\nd\tno package of this run: inert\n") {
		t.Error(d)
	}
	_, errs, err := testApartAwk(t, 1, 1, pkgs, "a\tshells out\nb\tshells out\n", true)
	if err != nil || !strings.Contains(errs, "WARN test plan: m/b part 1/2 beside another apart package on leg 1/1") {
		t.Errorf("one shard, two listed packages: want the WARN line, got %v: %s", err, errs)
	}
	for _, c := range []struct {
		apart string
		chain bool
		want  string
	}{
		{"a\tshells out\na\tagain\n", true, "test apart row repeats a"},
		{"a\t \n", true, "bad test apart row"},
		{"a\tshells out\n", false, "tapart= is for chain=1 (the test legs) only"},
	} {
		_, errs, err := testApartAwk(t, 1, 3, pkgs, c.apart, c.chain)
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 || !strings.Contains(errs, c.want) {
			t.Errorf("apart rows %q (chain %v): want exit 2 and %q, got %v: %s", c.apart, c.chain, c.want, err, errs)
		}
	}
}
