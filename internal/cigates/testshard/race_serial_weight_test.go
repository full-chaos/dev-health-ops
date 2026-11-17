package testshard_test

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13958: a race leg runs its packages in one parallel go test, then each race part and each solo package alone, so its
// wall is about max(largest package, package sum / 3.5) + its parts and solo packages. The race plan weighs a leg that way
// (ci/go_race_shard.awk rserial=1). This cell plans the shipped race leg count both ways on the committed tables and models each
// leg from the rows alone: the old sum weighting must put a leg over raceModelledLegLimit (the red), the serial weighting none.
// The rows are the largest hosted race times, so a modelled leg reads above a warm median: the plan of record (PLAN-13958-6)
// models 985 here as a 797 s warm wall with the measured overhead, against the 900 s median goal.
// 1000 = the rows model (the largest hosted times, not a wall); a red here means re-cut the race legs, not a flake.
const (
	raceModelledLegLimit = 1000.0
	raceBatchOverlap     = 3.5 // a many-package batch's measured overlap (GO_RACE_BATCH_OVERLAP, the plan's default)
)

func raceSoloKeys(t *testing.T) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_race_solo.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	solo := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Split(l, "\t"); l != "" && !strings.HasPrefix(l, "#") && f[0] != "" {
			solo[f[0]] = true
		}
	}
	return solo
}

// modelledRaceLegs plans every race leg of count with GO_RACE_SERIAL_WEIGHTING=weighting and returns each leg's modelled wall
// (rows, no setup or providersync slice: both are the same for every placement).
func modelledRaceLegs(t *testing.T, weighting string, count int, weights map[string]float64, solo map[string]bool) []float64 {
	t.Helper()
	var walls []float64
	for k := 1; k <= count; k++ {
		out, errs, err := checkGo(t, []string{"GO_RACE_SERIAL_WEIGHTING=" + weighting}, "race-shard-plan", strconv.Itoa(k), strconv.Itoa(count))
		if err != nil {
			t.Fatalf("race-shard-plan %d %d (weighting %s): %v\n%s", k, count, weighting, err, errs)
		}
		var largest, sum, serial float64
		for _, row := range strings.Split(out, "\n") {
			f := strings.Split(row, "\t")
			if len(f) < 3 || f[0] != "." || f[2] == "internal/providersync" {
				continue
			}
			w, ok := weights[f[2]]
			if !ok {
				w = 1
			}
			switch {
			case len(f) == 5:
				parts, perr := strconv.Atoi(f[4])
				if perr != nil || parts < 2 {
					t.Fatalf("race-shard-plan %d %d: part row %q has no part count", k, count, row)
				}
				serial += math.Ceil(w / float64(parts))
			case solo[f[2]]:
				serial += w
			default:
				largest = math.Max(largest, w)
				sum += w
			}
		}
		walls = append(walls, math.Max(largest, sum/raceBatchOverlap)+serial)
	}
	return walls
}

func TestTheRacePlanWeighsPartsAsSerialTimeAndKeepsEveryLegUnderTheLimit(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	weights := raceWeights(t)
	solo := raceSoloKeys(t)
	maxOf := func(v []float64) (m float64, at int) {
		for i, x := range v {
			if x > m {
				m, at = x, i+1
			}
		}
		return m, at
	}
	sumWeighted := modelledRaceLegs(t, "0", races, weights, solo)
	serial := modelledRaceLegs(t, "1", races, weights, solo)
	if m, at := maxOf(sumWeighted); m <= raceModelledLegLimit {
		t.Errorf("the sum weighting puts no race leg over %.0f (max %.0f, leg %d): the cell's red is gone, it would pass without the serial weighting", raceModelledLegLimit, m, at)
	}
	for k, w := range serial {
		if w > raceModelledLegLimit {
			t.Errorf("race leg %d/%d: modelled %.0f with the serial weighting, over %.0f", k+1, races, w, raceModelledLegLimit)
		}
	}
	ms, _ := maxOf(serial)
	mw, _ := maxOf(sumWeighted)
	t.Logf("race legs %d: modelled max %.0f (serial weighting) vs %.0f (sum weighting)", races, ms, mw)
}

// guardPredictions plans every race leg of count with env and predicts each leg as the CHAOS-12462 budget guard does
// (race_leg_budget_test.go: its own constants), from the rows and the plan output.
func guardPredictions(t *testing.T, env []string, count int, weights map[string]float64) (preds []float64, stderr string) {
	t.Helper()
	var errs strings.Builder
	for k := 1; k <= count; k++ {
		out, e, err := checkGo(t, env, "race-shard-plan", strconv.Itoa(k), strconv.Itoa(count))
		if err != nil {
			t.Fatalf("race-shard-plan %d %d %v: %v\n%s", k, count, env, err, e)
		}
		errs.WriteString(e)
		load, rows := providersyncSliceAtFiveS*5/float64(count), 0
		for _, row := range strings.Split(out, "\n") {
			f := strings.Split(row, "\t")
			if len(f) < 3 || f[0] != "." || f[2] == "internal/providersync" {
				continue
			}
			rows++
			w, ok := weights[f[2]]
			if !ok {
				w = 1
			}
			if len(f) == 5 {
				parts, perr := strconv.Atoi(f[4])
				if perr != nil || parts < 2 {
					t.Fatalf("race-shard-plan %d %d: part row %q has no part count", k, count, row)
				}
				w = math.Ceil(w / float64(parts))
			}
			load += w
		}
		if rows == 0 {
			t.Fatalf("race-shard-plan %d %d %v planned no package", k, count, env)
		}
		preds = append(preds, raceLegSetupSeconds+raceLegSecondsPerWeight*load)
	}
	return preds, errs.String()
}

// CHAOS-13958: the serial weighting alone piles package sums on the legs whose wall is one large package, past the budget guard;
// the race plan caps a leg's row weight (race_leg_weight_cap of ci/check_go.sh, from the guard's constants). The pile-up is the
// plan at a batch overlap of 3.5 with no apart rows: there, off = a leg over the guard's limit (the red), default = none. The
// shipped plan (overlap 1.5, ci/go_race_apart.tsv) stays under it either way and must not warn. A package no leg can take under
// the cap still lands, and the plan says so.
func TestTheRaceLegWeightCapKeepsTheBudgetGuardAndWarnsWhenNoLegFits(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	weights := raceWeights(t)
	limit := 0.75 * raceLegCapSeconds
	over := func(p []float64) (n int, worst float64) {
		for _, x := range p {
			if x > limit {
				n++
			}
			worst = math.Max(worst, x)
		}
		return n, worst
	}
	none := filepath.Join(t.TempDir(), "apart.tsv")
	if err := os.WriteFile(none, []byte("# no rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pileUp := []string{"GO_RACE_BATCH_OVERLAP=3.5", "GO_RACE_APART=" + none}
	off, _ := guardPredictions(t, append([]string{"GO_RACE_LEG_WEIGHT_CAP=0"}, pileUp...), races, weights)
	if n, worst := over(off); n == 0 {
		t.Errorf("with the cap off (overlap 3.5, no apart rows) no race leg is over %.0f s (max %.0f): the cell's red is gone, it would pass without the cap", limit, worst)
	}
	capped, _ := guardPredictions(t, pileUp, races, weights)
	if n, worst := over(capped); n > 0 {
		t.Errorf("with the default cap (overlap 3.5, no apart rows) %d race leg(s) are over %.0f s (max %.0f)", n, limit, worst)
	}
	def, warns := guardPredictions(t, nil, races, weights)
	if n, worst := over(def); n > 0 {
		t.Errorf("with the default cap %d race leg(s) are over %.0f s (max %.0f)", n, limit, worst)
	}
	if strings.Contains(warns, "WARN race plan:") {
		t.Errorf("the default cap leaves a package with no leg: %s", warns)
	}
	const smallCap = 600.0
	_, warns = guardPredictions(t, []string{"GO_RACE_LEG_WEIGHT_CAP=600"}, races, weights)
	named := false
	for _, l := range strings.Split(warns, "\n") {
		if !strings.HasPrefix(l, "WARN race plan: ") || !strings.Contains(l, " over the leg weight cap on leg ") {
			continue
		}
		key := strings.TrimPrefix(strings.Fields(strings.TrimPrefix(l, "WARN race plan: "))[0], "github.com/full-chaos/dev-health-ops/")
		if weights[key] > smallCap {
			named = true
		}
	}
	if !named {
		t.Errorf("with a %v cap no WARN line names a package heavier than the cap:\n%s", smallCap, warns)
	}
	_, offMax := over(off)
	_, defMax := over(def)
	t.Logf("race legs %d: guard max %.0f s with the cap, %.0f s without", races, defMax, offMax)
}

// CHAOS-13958: the race plan knobs take only their documented values (GO_RACE_SERIAL_WEIGHTING: unset, 0, 1;
// GO_RACE_LEG_WEIGHT_CAP: unset or a non-negative decimal, 0 = off; GO_RACE_BATCH_OVERLAP: unset or a positive decimal;
// GO_RACE_APART: unset or a file); any other value, empty included, fails before any plan.
// The leg weight cap is the budget guard's scope: the root module only; a nested module's packages are never counted or warned.
func TestTheRacePlanKnobsRefuseEveryOtherValueAndTheCapCountsOnlyTheRootModule(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, env string
		accept    bool
	}{
		{"serial unset", "", true},
		{"serial 0", "GO_RACE_SERIAL_WEIGHTING=0", true},
		{"serial 1", "GO_RACE_SERIAL_WEIGHTING=1", true},
		{"serial empty", "GO_RACE_SERIAL_WEIGHTING=", false},
		{"serial 00", "GO_RACE_SERIAL_WEIGHTING=00", false},
		{"serial 01", "GO_RACE_SERIAL_WEIGHTING=01", false},
		{"serial 2", "GO_RACE_SERIAL_WEIGHTING=2", false},
		{"serial word", "GO_RACE_SERIAL_WEIGHTING=true", false},
		{"serial space", "GO_RACE_SERIAL_WEIGHTING= ", false},
		{"cap unset", "", true},
		{"cap 0", "GO_RACE_LEG_WEIGHT_CAP=0", true},
		{"cap integer", "GO_RACE_LEG_WEIGHT_CAP=1900", true},
		{"cap decimal", "GO_RACE_LEG_WEIGHT_CAP=2523.5", true},
		{"cap empty", "GO_RACE_LEG_WEIGHT_CAP=", false},
		{"cap word", "GO_RACE_LEG_WEIGHT_CAP=abc", false},
		{"cap negative", "GO_RACE_LEG_WEIGHT_CAP=-5", false},
		{"cap space", "GO_RACE_LEG_WEIGHT_CAP= ", false},
		{"cap exponent", "GO_RACE_LEG_WEIGHT_CAP=1e3", false},
		{"overlap 1.5", "GO_RACE_BATCH_OVERLAP=1.5", true},
		{"overlap 3.5", "GO_RACE_BATCH_OVERLAP=3.5", true},
		{"overlap 0", "GO_RACE_BATCH_OVERLAP=0", false},
		{"overlap 0.0", "GO_RACE_BATCH_OVERLAP=0.0", false},
		{"overlap empty", "GO_RACE_BATCH_OVERLAP=", false},
		{"overlap word", "GO_RACE_BATCH_OVERLAP=abc", false},
		{"overlap negative", "GO_RACE_BATCH_OVERLAP=-1", false},
		{"apart missing", "GO_RACE_APART=/nonexistent/go_race_apart.tsv", false},
		{"apart empty", "GO_RACE_APART=", false},
		{"apart directory", "GO_RACE_APART=/", false},
	} {
		var env []string
		if c.env != "" {
			env = []string{c.env}
		}
		out, errs, err := checkGo(t, env, "race-shard-plan", "1", "13")
		planned := strings.Contains(out, "\t")
		switch {
		case c.accept && (err != nil || !planned):
			t.Errorf("%s (%q): want a plan, got err %v\n%s", c.name, c.env, err, errs)
		case !c.accept && (err == nil || planned || !strings.Contains(errs, "ERROR: ")):
			t.Errorf("%s (%q): want a refusal before any plan, got err %v, planned %v\n%s", c.name, c.env, err, planned, errs)
		}
	}
	// An apart file that exists but cannot be read would read as no rows: refused too (a root user reads it anyway).
	if os.Geteuid() != 0 {
		locked := filepath.Join(t.TempDir(), "apart.tsv")
		if err := os.WriteFile(locked, []byte("heavy\tinternal/cigates/docs\t770\n"), 0o000); err != nil {
			t.Fatal(err)
		}
		out, errs, err := checkGo(t, []string{"GO_RACE_APART=" + locked}, "race-shard-plan", "1", "13")
		if err == nil || strings.Contains(out, "\t") || !strings.Contains(errs, "ERROR: ") {
			t.Errorf("apart unreadable: want a refusal before any plan, got err %v\n%s", err, errs)
		}
	}
	// A cap of 1 fits nothing, so every root package warns; a nested module's packages (plan rows whose module is not ".")
	// must never.
	nested := map[string]bool{}
	var warns []string
	for k := 1; k <= 13; k++ {
		out, errs, err := checkGo(t, []string{"GO_RACE_LEG_WEIGHT_CAP=1"}, "race-shard-plan", strconv.Itoa(k), "13")
		if err != nil {
			t.Fatalf("race-shard-plan %d 13 (cap 1): %v\n%s", k, err, errs)
		}
		for _, row := range strings.Split(out, "\n") {
			if f := strings.Split(row, "\t"); len(f) >= 3 && f[0] != "." && f[0] != "" {
				nested[f[1]] = true
			}
		}
		for _, l := range strings.Split(errs, "\n") {
			if strings.HasPrefix(l, "WARN race plan: ") {
				warns = append(warns, l)
			}
		}
	}
	if len(nested) == 0 || len(warns) == 0 {
		t.Fatalf("the nested-module cell held nothing: %d nested package(s), %d WARN line(s)", len(nested), len(warns))
	}
	for _, l := range warns {
		if pkg := strings.Fields(strings.TrimPrefix(l, "WARN race plan: "))[0]; nested[pkg] {
			t.Errorf("a nested module package was counted against the cap: %s", l)
		}
	}
}
