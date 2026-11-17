package testshard_test

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-12462: the race legs run a 30 minute job. The plan of every leg at the shipped leg count must predict a wall at or under 75% of that cap.
// The prediction is a pure function of ci/go_race_weights.d and the partition (the sum of the leg's package weights plus its providersync slice,
// scaled by the calibration of the hosted race legs); no test here reads or asserts a measured wall (D6418).
// CHAOS-13833: the race legs run on hosted runners (the pool is off). raceLegSecondsPerWeight is the p90 of (stage wall - 40 s) /
// (summed package seconds of the leg) over 65 hosted race leg-runs (median 0.366, max 0.524): the 6 race legs of main push runs
// 43659268009, 43662301700 (race-1 cancelled, left out), 43662511162, 43663384548, 43664577954, 43668418168, 43668892701,
// 43676204152, 43685091286, 43717634633 and pull request run 43700083233. It was 0.453, the median over 25 pool race legs.
const (
	raceLegCapSeconds        = 1800
	raceLegSetupSeconds      = 40.0
	raceLegSecondsPerWeight  = 0.50  // the p90 above (0.503), rounded
	providersyncSliceAtFiveS = 250.0 // one providersync slice with 5 race legs; it scales with 1/count
)

func raceWeights(t *testing.T) map[string]float64 {
	t.Helper()
	w := map[string]float64{}
	for _, l := range strings.Split(raceweightsMerge(raceweightsRealFiles(t)), "\n") {
		if f := strings.Split(l, "\t"); len(f) == 2 {
			v, err := strconv.ParseFloat(strings.TrimSpace(f[1]), 64)
			if err != nil {
				t.Fatalf("weight row %q: %v", l, err)
			}
			w[f[0]] = v
		}
	}
	return w
}

func TestEveryRaceLegIsPredictedAtOrUnderSeventyFivePercentOfTheJobCap(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	weights := raceWeights(t)
	limit := 0.75 * raceLegCapSeconds
	for k := 1; k <= races; k++ {
		load := providersyncSliceAtFiveS * 5 / float64(races)
		for _, row := range planKeys(t, "race-shard-plan", k, races) {
			f := strings.Split(row, "\t")
			key := f[2]
			if key == "internal/providersync" {
				continue
			}
			w, ok := weights[key]
			if !ok {
				w = 1
			}
			// CHAOS-13833: a part of a race split package weighs ceil(package weight / parts), as ci/go_race_shard.awk places it.
			if len(f) == 5 {
				parts, err := strconv.Atoi(f[4])
				if err != nil || parts < 2 {
					t.Fatalf("race-shard-plan %d %d: part row %q has no part count", k, races, row)
				}
				w = math.Ceil(w / float64(parts))
			}
			load += w
		}
		if predicted := raceLegSetupSeconds + raceLegSecondsPerWeight*load; predicted > limit {
			t.Errorf("race leg %d/%d: predicted %.0f s (weight load %.0f), over %.0f s = 75%% of the %d s job cap", k, races, predicted, load, limit, raceLegCapSeconds)
		} else {
			t.Log(fmt.Sprintf("race leg %d/%d: predicted %.0f s (weight load %.0f)", k, races, predicted, load))
		}
	}
}
