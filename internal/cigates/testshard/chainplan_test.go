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

// plannedWallCap: the highest modelled wall a test shard may plan, under check_go.sh's 600 s warn line with a margin.
const plannedWallCap = 540

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
