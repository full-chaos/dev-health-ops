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
	raceBatchOverlap     = 3.5 // the default rk of ci/go_race_shard.awk
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
// the race plan caps a leg's row weight (race_leg_weight_cap of ci/check_go.sh, from the guard's constants). Off: a leg over the
// guard's limit (the red). Default: none. A package no leg can take under the cap still lands, and the plan says so.
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
	off, _ := guardPredictions(t, []string{"GO_RACE_LEG_WEIGHT_CAP=0"}, races, weights)
	if n, worst := over(off); n == 0 {
		t.Errorf("with the cap off no race leg is over %.0f s (max %.0f): the cell's red is gone, it would pass without the cap", limit, worst)
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
