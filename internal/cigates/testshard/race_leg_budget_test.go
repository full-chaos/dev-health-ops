package testshard_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-12462: the race legs run a 30 minute job. The plan of every leg at the shipped leg count must predict a wall at or under 75% of that cap.
// The prediction is a pure function of ci/go_race_weights.d and the partition (the sum of the leg's package weights plus its providersync slice,
// scaled by the calibration of the 26.04 pool runs); no test here reads or asserts a measured wall (D6418).
const (
	raceLegCapSeconds        = 1800
	raceLegSetupSeconds      = 40.0
	raceLegSecondsPerWeight  = 0.453 // median of wall / sum of package times over 25 pool race legs, runs 41315075216..41322726564
	providersyncSliceAtFiveS = 250.0 // one providersync slice with 5 race legs; it scales with 1/count
)

func raceWeights(t *testing.T) map[string]float64 {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(repoRoot(t), "ci", "go_race_weights.d", "*.tsv"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no race weights files: %v", err)
	}
	w := map[string]float64{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range strings.Split(string(raw), "\n") {
			if f := strings.Split(l, "\t"); len(f) == 2 {
				v, err := strconv.ParseFloat(strings.TrimSpace(f[1]), 64)
				if err != nil {
					t.Fatalf("weight row %q: %v", l, err)
				}
				w[f[0]] = v
			}
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
			key := strings.Split(row, "\t")[2]
			if key == "internal/providersync" {
				continue
			}
			w, ok := weights[key]
			if !ok {
				w = 1
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
