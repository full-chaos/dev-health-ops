package capacityforecast

import (
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// distributionToModel maps a forecast's Monte Carlo histograms onto the GraphQL
// `completionDistribution` field (CHAOS-7624).
//
// Both forecast paths use it, so the in-request simulation (`capacityForecast`)
// and the persisted rows (`capacityForecasts`) render one distribution
// identically.
//
// nil -- the GraphQL null -- means "no distribution": every row written before
// migration 101, and a forecast whose two modes both did not simulate. It is
// never an object of empty or zero bins, because absent is not "every run was
// zero" (North Star check 12). Inside the object a mode that did not simulate
// is a null list, the same rule one level down.
func distributionToModel(days, items *numerical.Histogram) *model.CapacityDistribution {
	daysBins := binsToModel(days)
	itemsBins := binsToModel(items)
	if daysBins == nil && itemsBins == nil {
		return nil
	}
	return &model.CapacityDistribution{Days: daysBins, Items: itemsBins, Runs: runsOf(daysBins, itemsBins)}
}

// runsOf is the number of simulation runs of a distribution (CHAOS-8477): the
// sum of the counts of the SERVED bins of a mode, so `runs` always agrees with
// the bins beside it and a caller never has to add them up itself.
//
// Both modes of one forecast come from one simulation and hold the same number
// of runs (numerical.ForecastCapacity draws request.Simulations samples for
// each). The days mode is read when it simulated, else the items mode; the
// caller guarantees one of them did.
func runsOf(days, items []model.CapacityDistributionBin) int {
	bins := days
	if bins == nil {
		bins = items
	}
	runs := 0
	for _, bin := range bins {
		runs += bin.Count
	}
	return runs
}

// binsToModel is nil for a nil or empty histogram. A malformed one (values and
// counts of different lengths) is truncated to the shorter, which cannot
// happen from either producer; failing a whole list query over it would turn a
// bad row into an outage.
func binsToModel(histogram *numerical.Histogram) []model.CapacityDistributionBin {
	if histogram == nil || len(histogram.Values) == 0 || len(histogram.Counts) == 0 {
		return nil
	}
	length := min(len(histogram.Values), len(histogram.Counts))
	// The run total of THIS mode's served bins. The cumulative share is taken
	// against it, so the last bin is exactly 1 whatever was stored.
	total := 0
	for index := 0; index < length; index++ {
		total += histogram.Counts[index]
	}
	bins := make([]model.CapacityDistributionBin, 0, length)
	running := 0
	for index := 0; index < length; index++ {
		running += histogram.Counts[index]
		bins = append(bins, model.CapacityDistributionBin{
			Value:           histogram.Values[index],
			Count:           histogram.Counts[index],
			CumulativeShare: cumulativeShare(running, total),
		})
	}
	return bins
}

// cumulativeShare is the share of a mode's simulation runs that completed on a
// bin's value or a lower one (CHAOS-8477): the running sum of the counts over
// the run total, from the same Monte Carlo distribution the percentile days
// come from (TestTheCurveAndTheServedPercentilesAreOneDistribution pins how
// the two agree).
// The bins are ascending by value (numerical.NewHistogram), so the share never
// falls, and the last bin's is total/total = 1 exactly. It is computed here so
// that a caller draws the cumulative curve from served values and adds no
// counts up itself.
//
// A total of 0 can only come from stored bins that all hold a count of 0; no
// producer writes that. The share is then 0, not NaN: a GraphQL Float cannot
// carry NaN, and one bad row must not fail a whole list query.
func cumulativeShare(running, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(running) / float64(total)
}

// histogramFromUint16 and histogramFromUint32 rebuild a Histogram from the
// parallel persisted arrays. An empty pair is nil: "no distribution".
func histogramFromUint16(values []uint16, counts []uint32) *numerical.Histogram {
	if len(values) == 0 || len(counts) == 0 {
		return nil
	}
	histogram := numerical.Histogram{
		Values: make([]int, 0, len(values)),
		Counts: make([]int, 0, len(counts)),
	}
	for _, value := range values {
		histogram.Values = append(histogram.Values, int(value))
	}
	for _, count := range counts {
		histogram.Counts = append(histogram.Counts, int(count))
	}
	return &histogram
}

func histogramFromUint32(values, counts []uint32) *numerical.Histogram {
	if len(values) == 0 || len(counts) == 0 {
		return nil
	}
	histogram := numerical.Histogram{
		Values: make([]int, 0, len(values)),
		Counts: make([]int, 0, len(counts)),
	}
	for _, value := range values {
		histogram.Values = append(histogram.Values, int(value))
	}
	for _, count := range counts {
		histogram.Counts = append(histogram.Counts, int(count))
	}
	return &histogram
}
