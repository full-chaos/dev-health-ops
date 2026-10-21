//go:build !race

package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The venue-oracles job runs as a sharded matrix whose slices partition the registry: `ci/check_go.sh venue-oracles SHARD
// COUNT` runs the SHARD-th slice of the registry's `run` rows, the legs are the longest-processing-time assignment by the
// measured seconds in ci/venue_oracle_weights.d/ (ci/venue_oracle_shard.awk), and a leg that selects nothing fails loudly.

var oraclePackages = []pkgTests{
	{"internal/a", []string{"TestA1", "TestA2", "TestA3", "TestA4", "TestA5"}},
	{"internal/b", []string{"TestB1", "TestB2", "TestB3"}},
	{"internal/c", []string{"TestC1", "TestC2"}},
}

func stripe(rows [][2]string, start, step int) [][2]string {
	var out [][2]string
	for i := start; i < len(rows); i += step {
		out = append(out, rows[i])
	}
	return out
}

func TestOracleShardSlices(t *testing.T) {
	t.Parallel()
	t.Run("slices partition the registry run rows", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, oraclePackages)
		all := o.registryRunRows()
		if len(all) != 10 {
			t.Fatalf("%d rows", len(all))
		}
		var seen [][2]string
		for shard := 1; shard <= 4; shard++ {
			r := o.venueOracles(nil, strconv.Itoa(shard), "4")
			if r.res.code != 0 {
				t.Fatalf("shard %d: %s%s", shard, r.res.stderr, r.res.stdout)
			}
			if !rowsEqual(sortedRows(r.ran), stripe(all, shard-1, 4)) {
				t.Errorf("shard %d ran %v, want every 4th row from %d", shard, r.ran, shard-1)
			}
			if !strings.Contains(r.res.stdout, fmt.Sprintf("shard %d/4 runs", shard)) {
				t.Errorf("shard %d does not log its slice: %s", shard, r.res.stdout)
			}
			seen = append(seen, r.ran...)
		}
		if !rowsEqual(sortedRows(seen), all) {
			t.Errorf("the union of the shards %v is not the registry %v (or a test ran twice)", sortedRows(seen), all)
		}
	})
	t.Run("no arguments runs every row", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, oraclePackages)
		r := o.venueOracles(nil)
		if r.res.code != 0 || !rowsEqual(sortedRows(r.ran), o.registryRunRows()) {
			t.Fatalf("exit %d ran %v: %s", r.res.code, r.ran, r.res.stderr)
		}
	})
	t.Run("a single shard of one is the whole registry", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, oraclePackages)
		r := o.venueOracles(nil, "1", "1")
		if r.res.code != 0 || !rowsEqual(sortedRows(r.ran), o.registryRunRows()) {
			t.Fatalf("exit %d ran %v: %s", r.res.code, r.ran, r.res.stderr)
		}
	})
	t.Run("a slice with zero rows fails loudly", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestA1", "TestA2"}}})
		ok := o.venueOracles(nil, "2", "3")
		if ok.res.code != 0 || len(ok.ran) != 1 {
			t.Fatalf("exit %d ran %v: %s", ok.res.code, ok.ran, ok.res.stderr)
		}
		r := o.venueOracles(nil, "3", "3")
		if r.res.code == 0 || !strings.Contains(r.res.stderr, "selects zero of the 2 registered run rows") || len(r.ran) != 0 {
			t.Fatalf("a zero-row leg: exit %d ran %v: %s", r.res.code, r.ran, r.res.stderr)
		}
	})
	t.Run("bad arguments fail", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, oraclePackages)
		for _, c := range []struct {
			args   []string
			needle string
		}{
			{[]string{"0", "4"}, "outside 1..4"}, {[]string{"5", "4"}, "outside 1..4"},
			{[]string{"x", "4"}, "SHARD must be a positive integer"}, {[]string{"1", "y"}, "COUNT must be a positive integer"},
			{[]string{"1", "0"}, "outside 1..0"}, {[]string{"2"}, "accepts no arguments, or SHARD COUNT"},
		} {
			r := o.venueOracles(nil, c.args...)
			if r.res.code == 0 || !strings.Contains(r.res.stderr, c.needle) || len(r.ran) != 0 {
				t.Errorf("%v: exit %d ran %v: %s", c.args, r.res.code, r.ran, r.res.stderr)
			}
		}
	})
	t.Run("a package with only local-only rows fails in every leg", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestA1", "TestA2"}}, {"internal/z", []string{"TestZOnly"}}}, "TestZOnly")
		for shard := 1; shard <= 2; shard++ {
			r := o.venueOracles(nil, strconv.Itoa(shard), "2")
			if r.res.code == 0 || !strings.Contains(r.res.stderr, "internal/z is registered but no test there is run") {
				t.Errorf("shard %d: exit %d: %s", shard, r.res.code, r.res.stderr)
			}
		}
	})
}

func matrixShards(t *testing.T) []int {
	t.Helper()
	raw, _ := nested(oracleWorkflowJob(t, "venue-oracles.yml", "venue-oracles"), "strategy", "matrix", "shard").([]any)
	if len(raw) == 0 {
		t.Fatal("the matrix shard list is empty")
	}
	var out []int
	for _, x := range raw {
		n, _ := x.(int)
		out = append(out, n)
	}
	return out
}

const (
	oracleJobSetupSeconds = 200
	oracleDriftSeconds    = 200
)

type planRow struct {
	pkg, test   string
	leg, weight int
}

func oraclePlan(t *testing.T, o *otree, count int) ([]planRow, map[int][2]int) {
	t.Helper()
	r := o.verb("venue-oracle-plan", nil, strconv.Itoa(count))
	if r.res.code != 0 {
		t.Fatalf("plan: %s%s", r.res.stderr, r.res.stdout)
	}
	var rows []planRow
	legs := map[int][2]int{}
	for _, l := range strings.Split(r.res.stdout, "\n") {
		f := strings.Split(l, "\t")
		switch {
		case f[0] == "#leg" && len(f) >= 4:
			k, _ := strconv.Atoi(f[1])
			n, _ := strconv.Atoi(f[2])
			s, _ := strconv.Atoi(f[3])
			legs[k] = [2]int{n, s}
		case len(f) == 4:
			leg, _ := strconv.Atoi(f[2])
			w, _ := strconv.Atoi(f[3])
			rows = append(rows, planRow{f[0], f[1], leg, w})
		}
	}
	return rows, legs
}

func (o *otree) writeWeights(text string) {
	o.t.Helper()
	dir := filepath.Join(o.dir, "ci", "venue_oracle_weights.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		o.t.Fatal(err)
	}
	old, _ := filepath.Glob(filepath.Join(dir, "*.tsv"))
	for _, f := range old {
		if err := os.Remove(f); err != nil {
			o.t.Fatal(err)
		}
	}
	o.write("ci/venue_oracle_weights.d/scratch.tsv", text)
}

func TestOracleLegPlan(t *testing.T) {
	t.Parallel()
	t.Run("the planned heaviest leg fits the budget", func(t *testing.T) {
		t.Parallel()
		job := oracleWorkflowJob(t, "venue-oracles.yml", "venue-oracles")
		budget, _ := strconv.Atoi(fmt.Sprint(nested(job, "env", "VENUE_LEG_BUDGET_SECONDS")))
		cap := job["timeout-minutes"].(int) * 60
		if budget+oracleJobSetupSeconds+oracleDriftSeconds > cap {
			t.Fatalf("the budget %ds plus %ds setup and %ds drift does not fit the %ds job cap", budget, oracleJobSetupSeconds, oracleDriftSeconds, cap)
		}
		count := len(matrixShards(t))
		o := realOTree(t)
		rows, legs := oraclePlan(t, o, count)
		if len(legs) != count {
			t.Fatalf("%d legs planned, want %d", len(legs), count)
		}
		total, heaviest := 0, 0
		for _, l := range legs {
			total += l[0]
			if l[1] > heaviest {
				heaviest = l[1]
			}
		}
		if total != len(rows) || len(rows) != len(o.registryRunRows()) {
			t.Fatalf("the plan holds %d rows over %d legs, the registry has %d run rows", len(rows), total, len(o.registryRunRows()))
		}
		if heaviest > budget {
			t.Errorf("the heaviest of the %d planned venue legs is %ds of tests, over the %ds budget: regenerate ci/venue_oracle_weights.d/ or add a leg to the matrix and the verb's COUNT", count, heaviest, budget)
		}
	})
	t.Run("weights balance the legs and never drop a test", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, oraclePackages)
		o.writeWeights("# weights\ninternal/a\tTestA1\t500\ninternal/a\tTestA2\t20\ninternal/a\tTestA3\t20\ninternal/b\tTestB1\t300\ninternal/c\tTestC1\t10\ninternal/nowhere\tTestGone\t9999\n")
		all := o.registryRunRows()
		rows, legs := oraclePlan(t, o, 3)
		rows2, _ := oraclePlan(t, o, 3)
		if fmt.Sprint(rows) != fmt.Sprint(rows2) {
			t.Fatal("the plan is not deterministic")
		}
		var got [][2]string
		total, heavyRow := 0, 0
		for _, r := range rows {
			got = append(got, [2]string{r.pkg, r.test})
			total += r.weight
			if r.weight > heavyRow {
				heavyRow = r.weight
			}
		}
		if !rowsEqual(sortedRows(got), all) {
			t.Fatal("the plan dropped or added a test")
		}
		legTotal, legMax := 0, 0
		for _, l := range legs {
			legTotal += l[1]
			if l[1] > legMax {
				legMax = l[1]
			}
		}
		if legTotal != total || float64(legMax) > float64(total)/3+float64(heavyRow) {
			t.Errorf("legs hold %ds of %ds, heaviest %d, one row weighs up to %d", legTotal, total, legMax, heavyRow)
		}
		var seen [][2]string
		for leg := 1; leg <= 3; leg++ {
			r := o.venueOracles(nil, strconv.Itoa(leg), "3")
			if r.res.code != 0 {
				t.Fatalf("leg %d: %s%s", leg, r.res.stderr, r.res.stdout)
			}
			var want [][2]string
			for _, pr := range rows {
				if pr.leg == leg {
					want = append(want, [2]string{pr.pkg, pr.test})
				}
			}
			if !rowsEqual(sortedRows(r.ran), sortedRows(want)) {
				t.Errorf("leg %d ran %v, the plan says %v", leg, r.ran, want)
			}
			if !strings.Contains(r.res.stdout, fmt.Sprintf("predicts %ds of tests for this leg", legs[leg][1])) {
				t.Errorf("leg %d does not log its prediction: %s", leg, r.res.stdout)
			}
			seen = append(seen, r.ran...)
		}
		if !rowsEqual(sortedRows(seen), all) {
			t.Error("the legs do not run the whole registry")
		}
	})
	t.Run("the 500 second row gets a leg to itself", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestA1", "TestA2", "TestA3", "TestA4"}}})
		o.writeWeights("internal/a\tTestA1\t500\ninternal/a\tTestA2\t100\ninternal/a\tTestA3\t100\ninternal/a\tTestA4\t100\n")
		rows, legs := oraclePlan(t, o, 2)
		heavy := 0
		for _, r := range rows {
			if r.test == "TestA1" {
				heavy = r.leg
			}
		}
		var alone []string
		for _, r := range rows {
			if r.leg == heavy {
				alone = append(alone, r.test)
			}
		}
		sum := 0
		for _, l := range legs {
			sum += l[1]
		}
		if strings.Join(alone, ",") != "TestA1" || legs[heavy] != [2]int{1, 500} || sum != 800 {
			t.Errorf("rows %v legs %v", rows, legs)
		}
	})
	t.Run("the plan verb rejects a bad count", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, oraclePackages)
		for _, c := range []string{"0", "x"} {
			r := o.verb("venue-oracle-plan", nil, c)
			if r.res.code == 0 || !strings.Contains(r.res.stderr, "COUNT must be a positive integer") {
				t.Errorf("count %q: exit %d: %s", c, r.res.code, r.res.stderr)
			}
		}
	})
	t.Run("a leg logs its plan and warns over the budget", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestA1"}}})
		ok := o.venueOracles([]string{"VENUE_LEG_BUDGET_SECONDS=1800"}, "1", "1")
		if ok.res.code != 0 || !strings.Contains(ok.res.stdout, "venue-oracles: leg ") || !strings.Contains(ok.res.stdout, "budget 1800s") || strings.Contains(ok.res.stdout, "::warning") {
			t.Fatalf("exit %d: %s%s", ok.res.code, ok.res.stdout, ok.res.stderr)
		}
		slow := o.venueOracles([]string{"VENUE_LEG_BUDGET_SECONDS=1", "FAKE_GO_SLEEP=2"}, "1", "1")
		if slow.res.code != 0 || !strings.Contains(slow.res.stdout, "::warning title=venue-oracles leg over its test-time budget") {
			t.Fatalf("exit %d: %s%s", slow.res.code, slow.res.stdout, slow.res.stderr)
		}
	})
	t.Run("an unmeasured row is planned pessimistically", func(t *testing.T) {
		t.Parallel()
		files, _ := filepath.Glob(filepath.Join(rootOf(t), "ci", "venue_oracle_weights.d", "*.tsv"))
		sort.Strings(files)
		if len(files) == 0 {
			t.Fatal("no weight files")
		}
		for _, f := range files {
			for _, l := range strings.Split(readText(t, f), "\n") {
				if strings.TrimSpace(l) == "" || strings.HasPrefix(strings.TrimLeft(l, " \t"), "#") {
					continue
				}
				c := strings.Split(l, "\t")
				if len(c) < 3 || len(c) > 4 || (len(c) == 4 && c[3] != "unmeasured") {
					t.Errorf("%s: unknown weights columns in %q", filepath.Base(f), l)
					continue
				}
				if secs, err := strconv.Atoi(c[2]); err != nil {
					t.Errorf("%s: weight %q is not a whole number", filepath.Base(f), c[2])
				} else if len(c) == 4 && secs < 600 {
					t.Errorf("%s %s is unmeasured but planned at %ds: a new test must carry at least the provisional 600 s", c[0], c[1], secs)
				}
			}
		}
	})
}

// The real registry and the real script, only `go test` stood in for: the plan puts every run row in exactly one leg and no
// leg is empty, and the first and last legs run exactly their planned rows (every leg runs a `go list` of the whole module,
// so running them all costs minutes).
func TestOracleShardsRealRegistry(t *testing.T) {
	t.Parallel()
	shards := matrixShards(t)
	count := shards[len(shards)-1]
	o := realOTree(t)
	all := o.registryRunRows()
	if len(all) < 40 {
		t.Fatalf("only %d run rows in the registry", len(all))
	}
	rows, legs := oraclePlan(t, o, count)
	var planned [][2]string
	perLeg := map[int][][2]string{}
	for _, r := range rows {
		planned = append(planned, [2]string{r.pkg, r.test})
		perLeg[r.leg] = append(perLeg[r.leg], [2]string{r.pkg, r.test})
	}
	if !rowsEqual(sortedRows(planned), all) {
		t.Fatalf("the plan does not put every run row in exactly one leg: %d planned, %d registered", len(planned), len(all))
	}
	for leg := 1; leg <= count; leg++ {
		if len(perLeg[leg]) == 0 || legs[leg][0] != len(perLeg[leg]) {
			t.Fatalf("leg %d holds %d planned rows (plan header says %d)", leg, len(perLeg[leg]), legs[leg][0])
		}
	}
	for _, leg := range []int{1, count} {
		r := o.venueOracles(nil, strconv.Itoa(leg), strconv.Itoa(count))
		if r.res.code != 0 || !rowsEqual(sortedRows(r.ran), sortedRows(perLeg[leg])) {
			t.Fatalf("leg %d/%d: exit %d ran %d rows, plan has %d: %s", leg, count, r.res.code, len(r.ran), len(perLeg[leg]), tail(r.res.stderr+r.res.stdout, 1500))
		}
	}
}
