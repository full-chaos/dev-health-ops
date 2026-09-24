package investment

import (
	"math"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// TestComputeQualityStatsDrivers_MatchProofOracle holds computeQualityStats
// and the proof tool's quality_drivers recomputation
// (goapiproof.RecomputeQualityDrivers) to the same list on a grid that
// sits on and one step past every threshold, so a threshold that drifts
// in either one fails here. Where the oracle determines thin_component,
// both ends of the quality_known_count range it allows must give that
// list.
func TestComputeQualityStatsDrivers_MatchProofOracle(t *testing.T) {
	const total = 100
	means := []float64{0.4, math.Nextafter(0.4, 0), 0.7}
	stddevs := []float64{0.25, math.Nextafter(0.25, 1), 0.1}
	determined, compared := 0, 0
	for _, unknown := range []int{0, 30, 31} {
		for _, lowPlus := range []int{50, 51} {
			for _, mean := range means {
				for _, stddev := range stddevs {
					bands := map[string]int64{
						"high": 0, "moderate": int64(total - unknown - lowPlus), "low": int64(lowPlus),
						"very_low": 0, "unknown": int64(unknown),
					}
					want, ok := goapiproof.RecomputeQualityDrivers(total, bands, &mean, &stddev)
					if !ok {
						continue
					}
					determined++
					for _, known := range []int{total - unknown, total} {
						row := QualityStatsRow{
							Total: total, QualityKnownCount: known, QualityMean: mean, QualityStddev: stddev,
							ModerateCount: total - unknown - lowPlus, LowCount: lowPlus, UnknownCount: unknown,
						}
						got := computeQualityStats(row, true).QualityDrivers
						compared++
						if !slices.Equal(got, want) {
							t.Fatalf("unknown=%d lowPlus=%d mean=%v stddev=%v known=%d: computeQualityStats = %v, oracle = %v", unknown, lowPlus, mean, stddev, known, got, want)
						}
					}
				}
			}
		}
	}
	if determined == 0 || compared == 0 {
		t.Fatalf("grid compared nothing: determined=%d compared=%d", determined, compared)
	}
}
