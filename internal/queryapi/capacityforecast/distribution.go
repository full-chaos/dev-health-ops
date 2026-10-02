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
	return &model.CapacityDistribution{Days: daysBins, Items: itemsBins}
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
	bins := make([]model.CapacityDistributionBin, 0, length)
	for index := 0; index < length; index++ {
		bins = append(bins, model.CapacityDistributionBin{
			Value: histogram.Values[index],
			Count: histogram.Counts[index],
		})
	}
	return bins
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
