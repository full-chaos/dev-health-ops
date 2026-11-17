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
